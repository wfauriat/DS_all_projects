# DE_pipeline_stream — technical synthesis

> Internal name: `DE_modern`. Pretext: a synthetic city bike-share operator ("Riverton").
> Purpose: a *reference wiring* of a modern streaming DE stack, optimised for readability of the
> plumbing rather than for domain depth. Every config file and entry point is commented on both
> ends of each wiring point.

---

## 1. Identity

| Item | Value |
|---|---|
| Domain | bike-share telemetry (trips + station status snapshots) |
| Shape | 6 layers, each a separate runtime, glued by Docker Compose |
| Stack | FastAPI/SSE · Kafka (KRaft) · Spark 4.2 Structured Streaming · Airflow 3.3 · DuckDB · dbt-duckdb · Streamlit/Altair |
| Language mix | Python 3.12 (uv workspace), SQL (DuckDB dialect + Jinja), Bash, Make, TOML/YAML/dotenv |
| Size | ~123 files, ~12.8k lines incl. docs; code ≈ 5k lines |
| Companion docs | `PLAN.md` (plan of record + verbatim initial prompt), `logging_build.md` (build log, measurements, decisions), `TOUR.md` (guided tour, wiring map, 10 reproducible experiments), `README.md` (per-layer quickstart) |

**Explicit non-goal:** no Schema Registry, no Spark cluster, no cosmos, no Prometheus. All listed as
extensions in `PLAN.md` §14.

---

## 2. Topology

```
source-api (FastAPI, :8000)
  ├── GET /v1/stream  (SSE, Last-Event-ID resume) ──► bridge ──► Kafka
  ├── GET /v1/stations|/v1/bikes|/v1/weather      ──────────────► Airflow api_extract
  └── /admin/clock|/faults|/fault-log|/stats      ──────────────► Makefile, smoke, api_extract

bridge (python -m bikeshare_bridge)
  contract.route()  valid   → bikeshare.trip-events.v1   (key bike_id)
                             bikeshare.station-status.v1 (key station_id)
                    invalid → bikeshare.dlq.v1           (key event_id, payload = errors + raw)

Kafka (1 KRaft node, INTERNAL kafka:9092 / EXTERNAL localhost:9094)
  ├─► Spark Structured Streaming (4 queries, own checkpoints)
  │     ├─ alerts → bikeshare.alerts.v1
  │     └─ station_metrics → data/lake/station_metrics/date=…/ *.parquet
  └─► Airflow stream_landing (offsets stored in DuckDB)

DuckDB data/warehouse/bikeshare.duckdb
  raw.*  ──(Asset event)──► dbt_transform: freshness → build → dq_report → publish_serving
  staging → intermediate → marts (+ snapshots, reference seeds, audit)
        └──► data/warehouse/bikeshare_serving.duckdb  (marts only, atomic os.replace)
                   └──► Streamlit dashboard / harlequin / peek scripts
```

Compose profiles: core (source, kafka, kafka-init, bridge, console) + `stream` (spark) + `batch`
(airflow-db, -init, -apiserver, -scheduler, -dag-processor). 11 services, 2 of them one-shot.

---

## 3. Entry points

| Entry point | Command | Runtime |
|---|---|---|
| `source/bikeshare_sim/__main__.py` | `python -m bikeshare_sim` / `make source-run` | uvicorn + custom `Server.handle_exit` closing SSE streams |
| `ingest/bikeshare_bridge/__main__.py` | `python -m bikeshare_bridge` / `make bridge-run` | blocking loop, SIGTERM→SystemExit→`finally: bridge.close()` |
| `streaming/stream_analyzer.py` | `spark-submit` (image CMD) via `entrypoint.sh` under `tini` | `analyzer.main:main` |
| `orchestration/dags/{stream_landing,api_extract,dbt_transform}.py` | Airflow dag-processor | `@dag`/`@task` TaskFlow |
| `dashboard/app.py` | `streamlit run` / `make dashboard` | host process, localhost only |
| `scripts/{smoke,peek_*,forget_landed_topic,copy_warehouse_schema}.py` | `make smoke|alerts|lake|warehouse|scorecard|…` | host, `PYTHONPATH=orchestration/include` |
| `Makefile` | `make help` | the single human entry point; source controls are raw `curl` + `jq` |

---

## 4. Layer 1 — `source/` (package `bikeshare_sim`)

A deterministic, fault-injecting synthetic vendor. Pure state + explicit clock; no I/O outside `api.py`.

| Module | Responsibility | Key API |
|---|---|---|
| `clock.py` | `sim_now = anchor_sim + (wall − anchor_wall)·speed`; every control re-anchors first | `now/set_speed/pause/resume/advance`; `__getstate__` drops the wall anchor |
| `world.py` | city generation from seed: station placement (rejection sampling on a disc, ≥300 m apart), zones, names, capacities, bikes, distance matrix, demand shape table, `WeatherModel` (seasonal+daily cosine, AR(1) anomaly, 2-state Markov rain) | `build_world(cfg, start) -> World`; `DEPARTURE_SHAPE[zone, weekend, minute]` normalised to daily mean 1 |
| `engine.py` | advances in **fixed 10 s simulated steps**; Poisson departures, zone-pull × distance-decay destination choice, heap of pending arrivals, staggered status snapshots, silent rebalancing, occasional capacity expansion, hourly weather | `step() -> (t0, t1, events)`; `_event()` builds `event_id` from the seeded RNG bytes |
| `faults.py` | post-processing layer: truth in, corrupted emission out; **own RNG stream** | `process(t0,t1,events)`; per-event injector chain (`list[dict] -> list[dict]`, `[]` = drop); `Episode` list; `FaultLog` = ground truth |
| `eventlog.py` | outbox: `deque(maxlen)` ring buffer of `(seq, type, json)`; `asyncio.Event` fan-out; `read_after` returns a **gap notice** when the resume point fell out | `publish`, `read_after`, `wait_for_new`, `close` |
| `api.py` | FastAPI app factory; lifespan starts `sim.run()` task; hand-written SSE frames; admin routes | `create_app(settings)`, `sse_frames()`, `close_streams(app)` |
| `schemas.py` | published contract: strict Pydantic payloads + discriminated envelope union (`EVENT_ADAPTER`) | used for responses/OpenAPI and by tests |
| `simulation.py` | the only object the API talks to: clock→engine→faults→log; pickle persistence with a **config fingerprint** | `catch_up`, `run`, `save`, `load_or_create` |
| `config.py` | TOML → env overrides → Pydantic validation | `load_settings(path, env)` |

**Fault catalogue** (9): per-event `duplicate`, `late_event`, `schema_drift`, `impossible_status`,
`teleport`, `orphan_trip`; episode `frozen_station`, `silent_station`, `stream_stall`.
Each is rate-configurable in `config/source.toml`, togglable and force-triggerable over `/admin`,
and writes a row to the ground-truth log.

**Idioms worth stealing**
- Three RNG streams from one seed: `default_rng([seed, 0|1|2])` for world / engine / faults — toggling
  faults never changes the truthful world.
- Fixed-step generation makes output a pure function of `(seed, config)`, identical at 1× or 3600×,
  and across pauses and fast-forwards.
- Injector chain as a list of `(event, t) -> list[event]` functions composed by a nested comprehension.
- Concurrency model stated explicitly: one event loop, all routes `async def`, so a route and an
  engine step never interleave and **no locks are needed**.
- Atomic pickle save (`tmp` + `os.replace`), plus a `fingerprint` (sha256 of world/clock/emission
  config) that refuses to resume an incompatible state and moves it to `.bak`.
- Status snapshots stamped at the **end** of their step (a measured bug fix: stamping mid-step let a
  snapshot count trips from its own future, which destroyed dbt's frozen-station precision).

---

## 5. Layer 2 — `ingest/` (package `bikeshare_bridge`)

SSE → contract → Kafka. At-least-once, resumable, with a DLQ.

| Module | Responsibility |
|---|---|
| `sse.py` | ~40-line SSE parser (`parse(lines) -> Iterator[SseFrame]`), `open_stream()` context manager setting `Last-Event-ID`. Comment lines become a synthetic `KEEPALIVE` frame so an idle stream still drives the loop |
| `contract.py` | the consumer's **own copy** of the producer's contract (strict Pydantic, `extra="forbid"`, `strict=True`); `route()` → `Routed(topic, key, value, headers, …)`; `_dead_letter()` classifies into `unparseable` / `unknown_event_type` / `contract_violation` and keeps the raw text for replay |
| `sink.py` | `KafkaSink` over confluent-kafka: `enable.idempotence` (implies acks=all), zstd, `linger.ms=20`; delivery callbacks; `BufferError` → `poll(0.5)` retry = backpressure that propagates to the SSE read; `flush()` raises `DeliveryFailed` |
| `checkpoint.py` | `{last_seq, saved_at}` JSON, atomic write; saved **only after** `flush()` confirms |
| `bridge.py` | the loop: `run()` (reconnect with `BACKOFF_S` ladder) → `consume_once()` → `handle(frame)`; periodic `maybe_checkpoint` / `maybe_report`; `_resume_point()` detects a *reset source* (checkpoint ahead of `/health.last_seq`) and restarts from its oldest event |
| `logs.py` | `JsonFormatter` + `event(log, msg, **fields)` — one JSON object per line |
| `config.py` | frozen dataclass settings from env only; `Topics` dataclass mirrors `kafka/create-topics.sh` |

**Doctrine established here**
- *Syntax at ingestion, semantics downstream.* `bikes_available < 0` violates the contract → DLQ.
  `bikes_available > capacity` needs reference data → left to Spark and dbt.
- Producer and consumer **share a contract, not code** (two near-identical Pydantic modules, on purpose).
- Payload bytes are forwarded untouched; metadata goes in Kafka **headers**
  (`event_type`, `schema_version`, `source_seq`, `ingested_at`) → exact lineage.
- The failure table lives in the module docstring (source down / gap / reset source / no ack / SIGTERM).

---

## 6. Layer 3 — `streaming/` (package `analyzer`)

Four Structured Streaming queries, one per pattern, each with its own Kafka source and checkpoint.

| Query | Pattern | State | Sink |
|---|---|---|---|
| `reference_rules` | `foreachBatch` + `persist()` + stream-static broadcast join on API-refreshed stations; **no watermark on purpose** so late events are visible | none | Kafka alerts (batch write inside foreachBatch) |
| `station_metrics` | `withWatermark` + `dropDuplicatesWithinWatermark` + 30-min tumbling windows | RocksDB | Parquet file sink, `partitionBy("date")` → **exactly once** via `_spark_metadata` |
| `station_health` | same windows at 120 min; `window_alerts` heuristics | RocksDB | Kafka alerts |
| `trip_pairing` | stream-stream **full outer** join on `trip_id` with a time bound (`INTERVAL 3 hours`) + watermarks on both sides | RocksDB | Kafka alerts |

**`rules.py` is pure `DataFrame -> DataFrame`**, so the same functions run on streams (main.py) and on
small static frames (tests). Functions: `parse_events`, `over_capacity`, `teleports` (+`haversine_km`),
`late_events`, `station_windows`, `window_alerts`, `trip_pairs`, `orphan_alerts`, `make_alerts`,
`to_kafka_records`.

**Alert record design** (`schemas.ALERT_COLUMNS`): one flat shape for every rule;
`alert_id = substring(sha2(alert_type|entity_id|event_id-or-window_start), 1, 16)` — deterministic, so
the at-least-once Kafka sink is safe (dedup by id downstream); `to_json(..., ignoreNullFields=false)`
keeps one stable schema instead of keys that come and go.

**Supporting modules**
- `schemas.py` — `EVENT_SCHEMA` with a payload that is a *flat superset* of all three event types
  (missing fields parse to null); parsing never fails, because the contract was enforced upstream.
- `reference.py` — `StationReference` re-fetches `/v1/stations` every `ttl_s=60` inside `foreachBatch`
  (driver-side), falling back to the previous DataFrame if the source is down. Rationale: capacities
  change, and a frame loaded once at startup raises false `over_capacity` alerts after an expansion.
- `progress.py` — `StreamingQueryListener` emitting one JSON line per micro-batch
  (`input_rows, rows_per_s, batch_ms, watermark, state_rows, dropped_late, dropped_duplicates`).
- `config.py` — env-driven frozen dataclass; **Spark engine settings deliberately elsewhere**
  (`conf/spark-defaults.conf`: `local[4]`, 1 g driver, `shuffle.partitions=4`, RocksDB state store with
  changelog checkpointing, UTC session zone, UI port).
- `Dockerfile` — resolves the Kafka connector **at build time** (`spark-submit --packages` over a no-op
  script, then copies only the 4 jars next to Spark's own) so runtime needs no network or Ivy cache.
- `entrypoint.sh` — `nss_wrapper` gives the host UID a passwd entry, required by Hadoop's
  `UserGroupInformation`; `tini` as PID 1 forwards SIGTERM.

**Measured tuning recorded in the build log:** 30-min health windows gave ~8 % precision on
`frozen_station`; 120-min gave ~86 %. `silent` uses `< expected/2` because the world starts mid-window.
Both frozen and silent require `departures+arrivals ≥ 3` — a quiet station at 3 a.m. legitimately repeats itself.

---

## 7. Layer 4 — `orchestration/` (Airflow 3.3, LocalExecutor, Postgres metadata)

**Separation of concerns:** `include/landing/` and `include/serving/` are plain Python packages with
**no Airflow import**; the DAGs only schedule them. The same packages are imported by host scripts
(`scripts/peek_warehouse.py`, `smoke.py`) and unit-tested without Airflow installed.

### `include/landing/warehouse.py`
The single place that opens DuckDB and defines `raw`.
- `connect(path, read_only, attempts=30, wait_s=2)` — retries on the single-writer lock, then
  `SET TimeZone='UTC'` (DuckDB otherwise hands TIMESTAMPTZ back in the host's local zone).
- `TOPIC_TABLES`: topic → table. Four topic tables share one shape:
  `(topic, partition, offset) PRIMARY KEY, msg_key, msg_ts, headers VARCHAR, value VARCHAR, batch_id, loaded_at`.
  `value`/`headers` are **text, not the JSON type**, so a malformed message can never block landing (ELT).
- `raw._kafka_offsets(consumer, topic, partition, next_offset, updated_at)` — the source of truth for resume.
- `raw._ingest_batches(batch_id, run_id, source, rows, detail JSON, started_at, finished_at)` — the
  pipeline's own audit log, queryable in SQL. `raw._batch_ids` sequence.
- API datasets are typed tables (`stations`, `bikes`, `weather` PK `observed_at`, `fault_log` PK `fault_id`).

### `include/landing/kafka_loader.py` — the exactly-once argument
```
plan_slices()  stored next_offset (or low watermark) … min(high, start+max) per partition
read_slices()  assign() + consume() until each slice is exhausted or the 120 s budget expires
land_topic()   ONE transaction: INSERT rows (Arrow table registered as a view) +
               UPSERT raw._kafka_offsets + INSERT raw._ingest_batches
               → then, strictly after COMMIT, consumer.commit() purely so the Console shows lag
```
Crash before COMMIT → both untouched, next run re-reads the same slice. Crash after → both advanced.
PK on `(topic, partition, offset)` is the second line of defence. `forget_topic()` handles the one case
offsets cannot survive: a topic deleted and recreated (offsets restart at 0) — it deletes rows and
positions in one transaction.

### `include/landing/api_extract.py`
One extraction pattern per dataset, all idempotent:
`stations`/`bikes` = full snapshot with `DELETE`+insert; `weather` = incremental by `max(observed_at)`;
`fault_log` = incremental by `max(fault_id)`, paged. Shared `_write()` does transaction + audit row and
uses `ON CONFLICT DO NOTHING` for incremental tables.

### `include/serving/publish.py`
`ATTACH '<serving>.tmp'` → recreate each `marts` table → write a `main.published(published_at, tables JSON)`
row → `DETACH` → `os.replace`. Readers see the old inode or the new file, never half of each, and never wait.

### DAGs
| DAG | Schedule | Structure |
|---|---|---|
| `stream_landing` | `timedelta(minutes=5)` | `prepare_warehouse >> [land_trip_events, land_station_status, land_dlq, land_alerts] >> report`; `report` has `outlets=[RAW_STREAM]` and raises `AirflowSkipException` when nothing landed (a skipped task emits no asset event) |
| `api_extract` | `timedelta(minutes=15)` | same fan-out over the 4 extractors; `outlets=[RAW_API]` |
| `dbt_transform` | `schedule=(RAW_STREAM \| RAW_API)` — an asset expression, **no clock** | `source_freshness >> dbt_build(trigger_rule=all_done) >> quality_report(all_done)`; `[build, report] >> publish_serving(outlets=[SERVING_MARTS])` |

Airflow idioms on display: `@dag`/`@task` TaskFlow, a task factory closure (`def land(topic, table)`
returning a decorated task with a computed `task_id`), `pool="duckdb"` (1 slot) on every writing task,
`Variable.get` / `BaseHook.get_connection` fed **entirely from env** (`AIRFLOW_CONN_*`, `AIRFLOW_VAR_*`
in the compose `x-airflow-common` anchor — nothing clicked in the UI), `outlet_events[ASSET].extra` to
attach row counts to the asset event, `@task.bash` for dbt, `doc_md=__doc__`, `max_active_runs=1`,
`catchup=False`, `is_paused_upon_creation=False`.

`quality_report` reads dbt's own artifacts (`target/run_results.json`, `target/sources.json`) and the
scorecard table, and logs them — turning dbt results into orchestrator-visible signals.

Recovery tuning (measured): `orphaned_tasks_check_interval=60`, `task_instance_heartbeat_timeout=90`
(defaults 300/300), because a dead task held the only `duckdb` pool slot for 5 minutes.

---

## 8. Layer 5 — `transform/` (dbt-duckdb)

`dbt_project.yml`: layers = folders = **schemas** (`generate_schema_name` macro overridden to drop
dbt's `<target>_<custom>` prefix). staging/intermediate = views, marts = tables, snapshots, seeds in
`reference`, singular test failures in `audit`.

`vars` are the cross-component thresholds, each commented with its twin elsewhere:
`status_interval_min` (= source TOML = Spark env), `late_after_min`, `teleport_min_kmh`,
`max_trip_hours` (= Spark `MAX_TRIP_DURATION`), `judge_after_hours`.

### Sources
- `raw.*` with `loaded_at_field: loaded_at` and freshness `warn 15 min / error 60 min` (wall clock);
  `freshness: null` on the tables where silence is good news (dlq, alerts) or irrelevant.
- `lake.station_metrics` as an **external source**:
  `external_location: read_parquet('{{ env_var("LAKE_DIR") }}/{name}/*/*.parquet', hive_partitioning=true)`
  — DuckDB reading Spark's Parquet in place, no load step.

### Model graph
```
staging (9 views)                intermediate                marts
stg_trip_events ─┐               int_trips  (full join       dim_stations
stg_station_status│               on trip_id + pairing)      fct_trips (incremental)
stg_alerts        ├─────────────► int_status_sequence        mart_station_usage_hourly
stg_dead_letters  │               (LAG + range join,         mart_data_quality_daily
stg_stations ─► snap_stations     materialized=table)        mart_detection_scorecard
stg_fault_log     │               int_detections             mart_pipeline_health
stg_weather       │               (UNION of every finding)   mart_stream_vs_batch
stg_bikes         │
stg_station_metrics (lake) ──────────────────────────────────┘
seed detection_checks ───────────────────────────────────────┘
```

**Staging idioms**
- `value ->> '$.payload.field'` extraction + explicit casts; the three clocks materialised side by side
  (`event_time`, `emitted_at`, `ingested_at = msg_ts`, `loaded_at`).
- Dedup with `qualify row_number() over (partition by event_id order by loaded_at, partition, offset) = 1`,
  plus **two counters that distinguish the cause**: `copies` (total landings) vs
  `source_copies` (distinct `seq`) → `source_copies > 1` = source duplicate fault,
  `copies > source_copies` = bridge re-send after a crash.
- `stg_station_metrics` guards the empty lake with the `lake_file_count()` macro (`glob()` at compile
  time, `{% if not execute %}return 0{% endif %}`) and falls back to a typed `where false` select, so
  lineage is still recorded when Spark is off.
- `stg_dead_letters` `try_cast(value->>'$.raw' as json)` to recover keys from a rejected event.
- `stg_fault_log` normalises `details` JSON into `station_id`, `trip_id`, `ends_at`, `variant`.

**Intermediate idioms**
- `int_trips`: `full join` of starts and ends + a `latest_event_time` CTE playing the role of Spark's
  watermark, giving `complete | in_progress | missing_end | missing_start`. Calls the `haversine_km`
  macro (the same formula as Spark's) and derives `speed_kmh`.
- `int_status_sequence`: `lag(...) over (partition by station_id order by event_time)` + a **range join**
  on the trips between two snapshots (`list(m.event_id)` kept for traceability), giving the exact
  `stale_snapshot` (identical values although `net_flow <> 0`) and `reporting_gap` (`gap_min > 2×interval`)
  checks that Spark's windows can only approximate. Materialised as a table: the heaviest step.
- `int_detections`: every finding of every detector unioned into one shape
  `(detector, check_name, event_id, trip_id, station_id, event_time, window_start, window_end)`
  + `md5(concat_ws(...)) as detection_id`.

**Marts idioms**
- `fct_trips`: `materialized='incremental'`, `unique_key='trip_id'`, `incremental_strategy='delete+insert'`,
  filtered on **`loaded_at` (landing time), not `event_time`** — which is precisely what makes late
  events safe to reprocess.
- `mart_detection_scorecard`: joins detections to the ground truth through the **seed**
  `detection_checks.csv (detector, check_name, fault_type, match_on)`; `match_on ∈ {event, trip, episode, stall}`
  selects the join predicate. Computes precision, recall, `faults_judged/faults_found`, plus
  `explained_by_other_faults` (a finding caused by a *different* fault) and a `detector='any'` roll-up.
  Everything is bounded by `judged_until = least(max(event_time) − judge_after_hours, max(injected_at))`
  so that an undetected-yet or not-yet-extracted fault is not counted as a miss/false positive.
  Carries an **enforced contract** (`config: contract: {enforced: true}` + typed column list).
- `mart_stream_vs_batch`: the same 30-min windows computed by Spark and by SQL, compared only where both
  sides are final (`least(max(window_end), time_bucket(max(event_time)))`).
- `mart_pipeline_health`: wall-clock hour × `raw._ingest_batches` + `quantile_cont` latency percentiles
  of `loaded_at − ingested_at`.

**Test taxonomy on display**
| Kind | Example |
|---|---|
| generic built-in | `unique`, `not_null`, `accepted_values`, `relationships` (one at `severity: warn`) |
| custom generic | `tests/generic/within_range.sql` (`{% test %}` macro with `min_value`/`max_value`) |
| singular, expected to find rows | `status_above_capacity.sql`, `trip_speed_implausible.sql` with `config(severity='warn', store_failures=true, schema='audit')` — **tests as monitors** |
| unit test | `int_trips_pairs_halves_and_spots_orphans` in `_intermediate.yml`: hand-written `given` rows for two refs, `expect` rows compared as a table |
| contract | enforced on `mart_detection_scorecard` |
| freshness | `dbt source freshness` on `loaded_at` |

**Snapshot:** `snap_stations` (YAML-defined, `strategy: timestamp`, `updated_at: updated_at` = the
source's own simulated change time, `dbt_valid_to_current: '9999-12-31…'` so "valid at t" is a plain
`BETWEEN`). Used by `dbt_over_capacity` and the singular test for an **as-of join**: capacity is judged
at the time of the event, which Spark's "today's reference data" cannot do on a replay.

**Macros:** `generate_schema_name`, `haversine_km`, `lake_file_count`, `duckdb__snapshot_get_time`
(dispatch override returning TIMESTAMPTZ instead of dbt-duckdb's naive TIMESTAMP).

---

## 9. Layer 6 — `dashboard/app.py` (Streamlit + Altair)

The end of the pipeline modelled as *just another reader of the serving layer*.
- Reads `bikeshare_serving.duckdb` only, `read_only=True`, `SET TimeZone='UTC'`.
- `@st.cache_data` keyed on `(path, mtime)` → **cache by data version, not by clock**; a reload costs
  nothing until Airflow publishes.
- `@st.fragment(run_every=30s)` compares the mtime and calls `st.rerun(scope="app")` — the reader half
  of the `os.replace` handshake.
- Four tabs (Detection, Data quality, City, Pipeline) mapping 1:1 to marts; one slider scoping only the
  two tabs whose data is in simulated days.
- Chart conventions: fixed categorical slots (bridge/spark/dbt keep their colour in every chart),
  light/dark palettes chosen for colour-vision deficiency, crosshair hover layer built from
  `alt.selection_point(nearest=True)`, a `table_view` expander under every chart, explicit
  `alt.Scale(type="utc")` so wall-clock instants aren't rendered in the viewer's zone, and a near-zero
  series rendered as a short table instead of a chart.
- Tested headlessly with `streamlit.testing.v1.AppTest` against a serving copy built in the test,
  including a test that a republished file (new mtime + `os.replace`) invalidates the cache.

---

## 10. Cross-cutting: the three clocks

| Clock | Columns | Kind | Set by | Consumed by |
|---|---|---|---|---|
| event time | `event_time` | simulated | engine | Spark windows/watermarks, dbt business logic |
| emission time | `emitted_at` | simulated | source at emission | lateness = `emitted_at − event_time` |
| pipeline time | `msg_ts`(=`ingested_at`), `loaded_at`, `detected_at` | wall clock | bridge (Kafka record ts), Airflow, Spark | dbt freshness, `fct_trips` incremental, pipeline health, Airflow schedules |

At the default 60×: Airflow's 5-min interval = 5 simulated hours; Spark's 30-min watermark = 30 real seconds.

## 11. Cross-cutting: delivery semantics

| Hop | Guarantee | Mechanism | Dedup site |
|---|---|---|---|
| source → bridge | at-least-once from the resume point (50 k buffer, `gap` notice beyond) | SSE `Last-Event-ID` | — |
| bridge → Kafka | at-least-once | checkpoint only after `flush()`; idempotent producer | staging `copies > source_copies` |
| Kafka → Spark → alerts | at-least-once | checkpoints + deterministic `alert_id` | `stg_alerts` qualify |
| Kafka → Spark → lake | exactly-once | file sink + `_spark_metadata` | — |
| Kafka → DuckDB `raw` | **exactly-once** | offsets in the same transaction as rows + PK | — |
| raw → marts | idempotent | `dbt build`, `delete+insert` on `trip_id` | — |
| marts → serving | atomic | build under tmp name + `os.replace` | — |

Three consumers of the same topics, three ways of remembering a position (checkpoint file / Spark
checkpoint dir / offsets in the sink) — the pedagogical core of the project.

---

## 12. Environment & workbench

- **uv workspace**: virtual root (`package = false`) with members `source/` and `ingest/`;
  dependency groups `dev`, `warehouse`, `spark`, `dashboard`, all default. `tool.uv.sources` maps the
  member names to the workspace. Service images run `uv sync --frozen --package <member> --no-editable`
  from the **same `uv.lock`**, so container and host resolve identically.
- **Dockerfiles**: two-stage dependency caching (copy `pyproject.toml` + `uv.lock` + members' pyprojects
  → `--no-install-workspace`; then copy code → install non-editable).
- **Compose**: YAML anchors + extension field (`x-airflow-common: &airflow-common`, `<<: *airflow-common`,
  `<<: *airflow-common-env`), profiles, healthchecks with `service_healthy` / `service_completed_successfully`
  dependencies, `user: "${HOST_UID}:${HOST_GID}"` so bind-mounted files stay owned by the developer,
  `stop_grace_period` sized to each service's shutdown work, named volumes for Kafka/Airflow state.
  Dual Kafka listeners (INTERNAL/EXTERNAL) explained inline.
- **Makefile**: `-include .env` + `export`, `unexport VIRTUAL_ENV`, `BUILDX_NO_DEFAULT_ATTESTATIONS=1`
  (otherwise every build changes the image ID and restarts containers), self-documenting `help` target
  via an awk pass over `## ` comments and `##@` sections, jq programs hoisted into variables,
  Kafka CLI reached through `docker compose exec kafka /opt/kafka/bin/…`, Airflow CLI through
  `exec -T airflow-scheduler /entrypoint airflow`.
- **pytest**: `--import-mode=importlib`, `pythonpath = [source/tests, ingest/tests, streaming, orchestration/include]`,
  shared helpers as uniquely-named plain modules (`simkit.py`, `bridgekit.py`) rather than conftest imports.
- **ruff**: line-length 100, isort `known-first-party` covering all four internal packages.

## 13. Test strategy per layer

| Layer | Approach |
|---|---|
| source | `FakeWall` callable injected into the clock; `run_for(sim, hours=…)` advances and catches up synchronously — no asyncio, no sleeping. Tests: clock math, determinism per seed, each fault's signature, API via starlette TestClient |
| bridge | `httpx.MockTransport` `FakeSource` replaying scripted SSE bodies per connection; `FakeProducer` that "acknowledges" on poll/flush and can fail selected topics. End-to-end bridge tests with no network |
| streaming | session-scoped local `SparkSession` (`local[1]`, 1 shuffle partition, UI off); rules applied to static DataFrames |
| orchestration | real DuckDB in `tmp_path`, `ensure_raw_schema`; loader tested with a fake consumer; no Airflow needed |
| dashboard | `AppTest.from_file` in-process, serving copy built from `VALUES` lists |
| dbt | `dbt build` runs generic + singular + unit tests; contract enforcement |
| whole stack | `scripts/smoke.py`: 6 ordered hops, stops at the first failure, forces 2 faults, triggers 2 DAG runs via Airflow REST `POST /api/v2/dags/{id}/dagRuns`, never resets anything |

---

## 14. Notable design decisions (with their measured justification)

1. **Long-running streaming work runs as services, not Airflow tasks**; Airflow only orchestrates
   bounded batches. Compose `restart:` policies stand in for Kubernetes.
2. **Kafka is the fan-out point**, so Spark and the DuckDB loader are independent consumers with
   independent offsets → replay and backfill come for free.
3. **Each layer owns one path**: Spark owns the hot path, DuckDB+dbt own the analytical record; Spark
   reaches DuckDB only through Parquet files and the alerts topic, never by writing the DuckDB file.
4. **DuckDB's single writer drives three mechanisms**: the `duckdb` pool (1 slot), short connections
   with a retrying `connect()`, and the atomic serving copy for readers (plus a structure-only copy for
   `dbt docs generate`, which otherwise held the lock for seconds and failed ~1 minute in 5).
5. **ELT, not ETL**: raw keeps envelope columns + payload as text; typing happens in dbt staging, so
   schema drift can never break landing.
6. **Complementary detectors, scored honestly.** The scorecard shows bridge+Spark/dbt reaching 100 %
   together on `impossible_status`; dbt's exact sequential check beating Spark's windows on
   `frozen_station`; and "false" positives caused by another fault reported as `explained`, not hidden.
7. **"Make the source truthful" beats encoding simulator internals downstream** — the snapshot
   timestamp fix (1,002 → 238 false detections) is the canonical example.
8. **Resetting a job means resetting its progress *and* its outputs** (`spark-reset` deletes checkpoints,
   lake, the alerts topic, *and* the warehouse rows + offsets landed from that topic).

## 15. Known limitations (from the build log)

- `frozen_station` recall ~40 % in Spark (short episodes cannot fill a 2 h window); dbt's check finds them.
- `orphan_trip` precision 56–66 %: a second cause (a half arriving after the watermark) is identified
  but not yet attributed as "explained".
- `stale_snapshot` ~29 % unexplained false positives (dead-lettered trip events, silent rebalancing).
- The loader cannot tell a recreated topic from the original (no topic id stored with the offsets).
- Spark's `over_capacity` judges against *today's* capacities on a replay; dbt's as-of join does not.
- Full rebuild of all non-incremental models each run (~23 s at ~360 k events).
