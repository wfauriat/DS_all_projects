# Guided tour of DE_modern

This tour walks through the pipeline the way an event travels:

- it is born in the simulated city;
- it streams over SSE;
- it lands in Kafka;
- Spark watches it;
- Airflow lands it in DuckDB;
- dbt models it;
- the scorecard judges every detector against the ground truth.

At each stop you'll find what to open, which commands to run, which files to read, and the ideas the stop demonstrates.

Companion documents:

- [`PLAN.md`](PLAN.md): why things are designed this way.
- [`logging_build.md`](logging_build.md): what was built, measured and decided, layer by layer. It also explains how to replay one layer from its commit.
- [`README.md`](README.md): a short per-layer quickstart.

---

## 0. The whole picture

```
 source-api (FastAPI)            bridge (Python)               Kafka (KRaft)
 simulated city ──SSE /v1/stream──► contract check ──► bikeshare.trip-events.v1   (key bike_id)
 + fault layer                       │              ──► bikeshare.station-status.v1 (key station_id)
 + ground truth (/admin/fault-log)   └──(invalid)──► bikeshare.dlq.v1
   │                                                        │
   │ REST (stations, bikes, weather, fault log)             ├──► Spark Structured Streaming ──► bikeshare.alerts.v1
   │                                                        │     4 queries                   └─► data/lake (Parquet)
   │                                                        │
   └──────────────► Airflow: api_extract ──┐     Airflow: stream_landing (all 4 topics)
                                           ▼                 ▼
                          DuckDB warehouse  raw.*  (exactly-once, offsets stored with the rows)
                                           │  asset events
                                           ▼
                           Airflow: dbt_transform ──► dbt build: staging → intermediate → marts
                                                      (+ Spark's lake read in place)
                                           │
                                           ▼
                     data/warehouse/bikeshare_serving.duckdb  ←  you (make dashboard / sql / scorecard)
```

**Addresses** (when a `make` target exists, it wraps the command):

| What | URL |
|---|---|
| Source API docs | http://localhost:8000/docs |
| Redpanda Console | http://localhost:8081 |
| Spark UI (Structured Streaming tab) | http://localhost:4040 |
| Airflow (no login) | http://localhost:8080 |
| dbt docs, lineage graph (after `make dbt-docs`) | http://localhost:8082 |
| Dashboard on the serving copy (after `make dashboard`) | http://localhost:8501 |

---

## 1. Start it

```bash
make setup        # .venv (uv), .env with your UID/GID, data/ folders
make up           # builds images, starts 11 services (2 are one-shots that exit 0)
make ps           # health; kafka-init and airflow-init should read "Exited (0)"
make speed x=60   # normal pace: 1 simulated hour per real minute
make smoke        # ~3 min: forces a teleport and a schema drift, follows both to the marts
```

- `make smoke` (`scripts/smoke.py`) checks the running stack in pipeline order and stops at the first broken hop:
  1. the source is healthy and not paused;
  2. both event topics receive new events;
  3. a forced schema drift reaches the DLQ;
  4. all 4 Spark queries commit, and a forced teleport raises an alert;
  5. the DAGs parse and are unpaused; it triggers `stream_landing` and `api_extract`, and `dbt_transform` follows;
  6. the alert, the dead letter and both faults are in `raw`, the trip is flagged in `fct_trips`, and the serving copy is republished with no empty mart.

  It never starts or resets anything. Its two faults go into the ground truth like any other.

- `COMPOSE_PROFILES` in `.env` selects the optional parts: `stream` (Spark) and `batch` (Airflow). With the variable set to nothing (`COMPOSE_PROFILES= make up`), only the core runs: source, Kafka, bridge and Console.
- The whole stack needs about 4.5 GB of RAM.
- `make reset` wipes **all** state together: the world, the checkpoints, the topics, the lake, the warehouse and Airflow's database. These pieces point into each other, so resetting only one of them breaks the others.

---

## 2. The three clocks (read this once)

Every event carries several timestamps. Mixing them up is the classic streaming bug.

| Clock | Column(s) | Kind | Who sets it | Used by |
|---|---|---|---|---|
| **Event time** | `event_time` | simulated | the source's engine | Spark windows and watermark; dbt business logic (trips, hours, days) |
| **Emission time** | `emitted_at` | simulated | the source, when it sends the event | lateness = `emitted_at − event_time` (late_event detection) |
| **Pipeline time** | `msg_ts` (= `ingested_at`), `loaded_at`, `detected_at` | wall clock | the bridge (Kafka record timestamp), Airflow (landing), Spark (processing) | dbt source freshness, `fct_trips` incremental loads, pipeline health, Airflow schedules |

At the default 60× speed, one real minute is one simulated hour. So:

| Real time | Simulated time |
|---|---|
| Airflow's 5-minute landing interval | 5 hours of data per run |
| Spark's 30-minute watermark | 30 real seconds |
| Spark's 10 s trigger | 10 simulated minutes per micro-batch |

Status snapshots are stamped with the instant their values are true, the end of their 10-second engine step. `logging_build.md`, layer 5, tells why that mattered.

---

## 3. Wiring map

### Services

| Service | Role | Reached by containers at | Reached from your host at | Configured by |
|---|---|---|---|---|
| `source-api` | the synthetic vendor | `http://source-api:8000` | `localhost:8000` | `config/source.toml`; `.env` → `SIM_SEED`, `SIM_SPEED` |
| `kafka` | the event log (KRaft, 1 node) | `kafka:9092` (INTERNAL listener) | `localhost:9094` (EXTERNAL listener) | `KAFKA_*` env in `docker-compose.yml` |
| `kafka-init` | creates the topics, then exits | – | – | `kafka/create-topics.sh` |
| `bridge` | SSE → Kafka, contract check, DLQ | – | – | env `SOURCE_URL`, `KAFKA_BOOTSTRAP`, `BRIDGE_*` (`ingest/bikeshare_bridge/config.py`) |
| `console` | Kafka web UI | – | `localhost:8081` | `KAFKA_BROKERS` |
| `spark` | streaming analyzer | – | `localhost:4040` | `streaming/conf/spark-defaults.conf`; env in compose (`streaming/analyzer/config.py`) |
| `airflow-db` | Airflow's metadata (not the warehouse) | `airflow-db:5432` | – | `POSTGRES_*` |
| `airflow-init` | DB migration + pool `duckdb`, then exits | – | – | its `command` |
| `airflow-apiserver` | UI, REST API, Execution API for tasks | `airflow-apiserver:8080` | `localhost:8080` | `x-airflow-common` |
| `airflow-scheduler` | schedules **and runs** the tasks (LocalExecutor) | – | – | `x-airflow-common`: connections, variables, dbt env |
| `airflow-dag-processor` | parses `orchestration/dags/` | – | – | `x-airflow-common` |

The Kafka listeners come in pairs because Kafka tells every client which address to use next. `kafka` doesn't resolve on your host, and `localhost` inside a container is the container itself. The `kafka` service in `docker-compose.yml` explains this in detail.

### Topics

| Topic | Key (keeps order per…) | Partitions | Retention | Produced by | Consumed by |
|---|---|---|---|---|---|
| `bikeshare.trip-events.v1` | `bike_id` | 3 | 7 d | bridge | Spark; Airflow loader |
| `bikeshare.station-status.v1` | `station_id` | 3 | 7 d | bridge | Spark; Airflow loader |
| `bikeshare.dlq.v1` | `event_id` | 1 | 30 d | bridge | Airflow loader |
| `bikeshare.alerts.v1` | `entity_id` | 3 | 7 d | Spark | Airflow loader; `make alerts` (host) |

### Three consumers, three ways to remember a position

| Consumer | Position stored in | Committed to Kafka? |
|---|---|---|
| bridge (reads the SSE stream) | `data/bridge/checkpoint.json` (a source seq), saved after Kafka confirms delivery | – |
| Spark (4 queries) | `data/checkpoints/<query>/offsets` + `commits` | no, so the Console shows no lag for Spark |
| Airflow loader | `raw._kafka_offsets`, **in the same DuckDB transaction as the rows** | yes, group `duckdb-loader`, for display only |

### Delivery guarantees, hop by hop

| Hop | Guarantee | Mechanism | Duplicates handled where |
|---|---|---|---|
| source → bridge | at-least-once from the resume point (a 50k-event buffer; a `gap` notice beyond it) | SSE `Last-Event-ID` | – |
| bridge → Kafka | at-least-once | checkpoint only after `flush()`; idempotent producer | `stg_*`: `copies > source_copies` = bridge re-send |
| Kafka → Spark → alerts | at-least-once | checkpoints; Kafka sink / `foreachBatch` | deterministic `alert_id`, deduplicated in `stg_alerts` |
| Kafka → Spark → lake | exactly-once | file sink + `_spark_metadata` | – |
| Kafka → DuckDB `raw` | **exactly-once** | offsets stored with the rows + primary key | – |
| raw → marts | idempotent | `dbt build` (incremental `fct_trips` upserts) | – |
| marts → serving copy | atomic | build under a temp name, then `os.replace` | – |

### "I want to change X": where it lives

| To change | Edit | Also keep in sync |
|---|---|---|
| Speed, seed | `.env` (`SIM_SPEED`, `SIM_SEED`), or live: `make speed x=…` | – |
| Fault rates | `config/source.toml`, or live: `make fault-rate f=… r=…` | – |
| World size, status cadence | `config/source.toml` (then `make reset`) | status interval: Spark `STATUS_INTERVAL_MIN` (compose), dbt var `status_interval_min` |
| A topic | `kafka/create-topics.sh` | bridge `config.py`, Spark `analyzer/config.py`, `landing/warehouse.py` `TOPIC_TABLES`, `make` targets |
| Spark windows or watermark | `spark` env in `docker-compose.yml` | `make spark-reset` if a stateful query's shape changes |
| Spark engine settings | `streaming/conf/spark-defaults.conf` (restart `spark`) | shuffle partitions are frozen into the checkpoints |
| Landing frequency | `schedule=` in `orchestration/dags/stream_landing.py` / `api_extract.py` | – |
| Late / teleport / trip thresholds | dbt vars in `transform/dbt_project.yml` | Spark `LATE_AFTER_MIN`, `TELEPORT_MIN_SPEED_KMH`, `MAX_TRIP_DURATION` |
| Add a detector to the scorecard | a CTE in `models/intermediate/int_detections.sql` + a row in `seeds/detection_checks.csv` | – |
| Airflow connections / variables | `AIRFLOW_CONN_*` / `AIRFLOW_VAR_*` in `x-airflow-common` | – |

---

## 4. The tour, stop by stop

### Stop 1: the source (the "vendor")

**Open:** http://localhost:8000/docs

```bash
make clock                          # sim_time, engine_time, backlog, speed
make stream types=trip_started      # raw SSE frames: id / event / data (Ctrl-C)
make faults                         # the 9 faults: kind, rate, injected count, active episodes
make fault-log                      # ground truth: what was corrupted, when, where
make ff h=3 && make clock           # fast-forward: the backlog is generated as a burst
```

**Read, in order:**

1. `config/source.toml`
2. `source/bikeshare_sim/simulation.py`: the clock → engine → faults → event log wiring
3. `api.py`: the concurrency model, and SSE written by hand
4. `engine.py`, `faults.py`

**Ideas:**

- **Deterministic fixed steps.** The same seed gives the same stream, at any speed.
- **A separate fault layer with its own RNG.** Faults never change the truthful world.
- **A ground-truth log**, the basis of the scorecard.
- **Persistence.** The world and seq survive restarts, and a SIGTERM closes open streams cleanly.

### Stop 2: the bridge and Kafka

**Open:** http://localhost:8081. Browse the topics, a message's headers, and the consumer group `duckdb-loader` → lag.

```bash
make topics                              # messages per topic
make tail t=bikeshare.trip-events.v1     # partition, headers, key, value
make fault f=schema_drift && make dlq    # a contract violation, with the reason and the raw event
docker compose logs -f --no-log-prefix bridge | jq -c 'select(.msg=="throughput")'
```

**Read, in order:**

1. The `kafka` service in `docker-compose.yml` (listeners)
2. `kafka/create-topics.sh`
3. `ingest/bikeshare_bridge/bridge.py` (the failure table in its docstring)
4. `contract.py`
5. `sink.py`

**Ideas:**

- **Explicit topics, keys and partitions.** No auto-creation.
- **Contract check (shape) at ingestion; semantic check (needs reference data) downstream.** A negative count is dead-lettered by the bridge, while "more bikes than docks" is left for Spark and dbt.
- **Payloads untouched, metadata in headers.**
- **At-least-once:** the checkpoint moves only after delivery is confirmed.

### Stop 3: Spark, the real-time analyzer

**Open:** http://localhost:4040 → *Structured Streaming*. Look at input vs processing rate, batch durations, and the state rows.

```bash
make alerts          # counts by type + latest (a host Kafka consumer on localhost:9094)
make lake            # DuckDB reading Spark's Parquet in place
docker compose logs -f --no-log-prefix spark | grep '"progress"' | jq -c '{query, input_rows, watermark, dropped_late, dropped_duplicates}'
ls data/checkpoints/station_health/     # commits  metadata  offsets  sources  state
```

**Read, in order:**

1. `streaming/analyzer/main.py`: why four queries
2. `rules.py`
3. `streaming/conf/spark-defaults.conf`
4. `streaming/Dockerfile` and `entrypoint.sh`

**Ideas:**

| Query | Pattern |
|---|---|
| `reference_rules` | stateless `foreachBatch` + `persist()` + reference data refreshed from the API; it sees late events on purpose |
| `station_metrics` | watermark + `dropDuplicatesWithinWatermark` + windows → exactly-once Parquet |
| `station_health` | 2-hour windows (30-minute ones gave 8% precision: measured, see the log) |
| `trip_pairing` | stream-stream **full outer** join with a time bound; orphans are emitted once the watermark passes |

The rules are pure DataFrame functions, so the same code runs on streams and in unit tests.

### Stop 4: Airflow, the landing zone

**Open:** http://localhost:8080. Look at the DAGs `stream_landing`, `api_extract` and `dbt_transform`, then *Assets* (`raw_stream` / `raw_api` → `dbt_transform` → `serving_marts`) and *Pools* (`duckdb`, 1 slot).

```bash
make dags && make runs d=stream_landing
make warehouse                 # rows per raw table, stored positions vs Kafka end offsets, batches
make trigger d=api_extract     # run now; its asset event then triggers dbt_transform
```

**Read, in order:**

1. `orchestration/include/landing/kafka_loader.py`: the exactly-once argument
2. `orchestration/dags/stream_landing.py`
3. `x-airflow-common` in `docker-compose.yml`
4. `landing/warehouse.py`

**Ideas:**

- **Streams are services; Airflow runs bounded batches.**
- **Offsets live in the sink, in the same transaction as the rows.**
- **A pool to respect DuckDB's single writer.**
- **Data-aware scheduling with assets**, and skipping the asset event when nothing landed.
- **Connections and variables from env.** The landing code is plain Python, unit-tested without Airflow.
- **Recovery:** a task killed with its scheduler is reset by the orphaned-tasks check (60 s here), then retried.

### Stop 5: dbt, and the serving copy

```bash
make dbt-docs        # http://localhost:8082: the lineage graph from raw.* and lake.* to the marts
make scorecard       # precision / recall of every check, from the serving copy
make quality         # per simulated day
make sql             # harlequin on the serving copy: marts only, never locked
make serving         # when the copy was published, rows per table
```

`make dbt-docs` never opens the live warehouse for writing. Its catalog only needs tables, views and columns, so the target:

1. copies the warehouse's structure, with no rows, into `transform/target-docs/bikeshare.duckdb`. The copy opens the warehouse read-only for a split second, and waits if a task holds the lock;
2. generates the docs from that copy;
3. serves them.

Run on the live file, `dbt docs generate` would hold DuckDB's single lock for several seconds. It failed whenever a landing or a dbt build held it, about a minute out of every five.

**Read, in order:**

1. `transform/dbt_project.yml`, `profiles.yml` and `models/sources.yml`
2. `models/staging/stg_trip_events.sql`: dedup that tells a source duplicate from a bridge re-send
3. `snapshots/snap_stations.yml`
4. `models/intermediate/int_status_sequence.sql` and `int_detections.sql`
5. `models/marts/mart_detection_scorecard.sql`
6. `orchestration/dags/dbt_transform.py` and `orchestration/include/serving/publish.py`

**Ideas:**

- **Layers as schemas.**
- **SCD2 snapshot + as-of join.** Capacity is judged at the time of the event.
- **An incremental model on landing time**, so late events are included.
- **Tests as monitors:** the `warn` tests keep their rows in schema `audit`.
- **A unit test and an enforced contract.**
- **An external source over Spark's Parquet**, with an empty-lake guard.
- **Freshness on wall-clock landing time.**
- **Readers never take the writer's lock.** Data comes from an atomic serving copy, and the docs from a copy of the structure.
- **One dbt project, two runners, separate artifacts.** Airflow's dbt writes `transform/target/` (its `quality_report` reads `run_results.json` from there). dbt on the host writes `target-host/` or `target-docs/`.

### Stop 6: the dashboard, a reader of the serving copy

**Open:** http://localhost:8501 after `make dashboard` (Ctrl-C stops it). It runs on your host, listening on localhost only.

```bash
make dashboard       # Streamlit: dashboard/app.py
make smoke           # then watch the page reload by itself once dbt republishes
```

The dashboard is the end of the pipeline: a consumer, like an analyst's notebook would be. It reads `data/warehouse/bikeshare_serving.duckdb` and nothing else. It never opens the warehouse, so it can't queue behind a landing or make one wait. Four tabs, each ending with a *table view* of its data:

| Tab | Shows | From |
|---|---|---|
| **Detection** | recall per fault type for the bridge, Spark and dbt; for each check, the share of its detections that are true, explained by another fault, or unexplained | `mart_detection_scorecard` |
| **Data quality** | events per simulated day; source duplicates, late events and dead letters per day (the late-event spikes are stream stalls) | `mart_data_quality_daily` |
| **City** | trips per hour of day, members vs casual riders (the commute peaks); a station map sized by docks, colored by the share of time a station was empty or full | `fct_trips`, `mart_station_usage_hourly`, `dim_stations` |
| **Pipeline** | rows landed and bridge→DuckDB latency per wall-clock hour (UTC); the days where Spark's windows and the batch record differ | `mart_pipeline_health`, `mart_stream_vs_batch` |

One slider above the tabs picks the simulated days for *Data quality* and *City*. *Detection* and *Pipeline* have their own time rules (the judge horizon, wall-clock hours), so the slider doesn't apply to them.

**How it stays current without a lock:**

1. Airflow's `publish_serving` builds the new copy under a temporary name, then `os.replace`s it: readers see the old file or the new one, never half of each.
2. The dashboard loads every query once per *version* of that file: `st.cache_data`, keyed on the file's modification time.
3. A small `st.fragment(run_every="30s")` compares that time with the file on disk and reruns the page when a new copy has landed. A connection still open on the old file keeps reading the old version.

**Read, in order:**

1. `dashboard/app.py`: its docstring, then `load` and `follow_new_copies`, then one tab function
2. `orchestration/include/serving/publish.py`: the other half of the handshake
3. `dashboard/tests/test_app.py`: the page run headlessly (Streamlit's `AppTest`) on a serving copy built in the test

**Ideas:**

- **A dashboard is just another reader of the serving layer.** Nothing about it is special to the warehouse.
- **Cache by data version, not by clock.** The file's mtime is the version, so a reload costs nothing until Airflow publishes.
- **Honest charts.** Colors come from a palette validated for color-vision deficiencies, and bridge, spark and dbt keep the same color in every chart. Every chart has tooltips and a table view. Timestamps are shown in UTC, not in your browser's time zone. A series that is almost all zeros becomes a short table ("days that differ"), not a chart.

---

## 5. Experiments

At the default 60× pace, "a few minutes" below means real minutes. Spark reacts within about 10–30 s. The warehouse follows the next `stream_landing` run (every ≤ 5 min), followed by `dbt_transform` (under a minute).

The scorecard also needs the fault log, extracted every 15 min, and judges only faults older than 4 simulated hours. To see a fault scored sooner, run `make trigger d=api_extract` after a few minutes.

Every experiment below was run on 2026-09-16 at 60×. The numbers in parentheses were measured then.

| # | Do | Watch | Expect |
|---|---|---|---|
| 1 | `make fault f=teleport` | `make alerts`; later in `make sql`: `SELECT * FROM marts.fct_trips WHERE is_implausible_speed` | a Spark `teleport` alert within ~20 s (14 s); the trip flagged after the next landing + dbt run; both Spark's and dbt's `teleport` checks match the fault |
| 2 | `make fault f=schema_drift` (a few times) | `make dlq`; `make scorecard` → `orphan_trip`, column `explained` | the event lands in the DLQ with its error. If it was one half of a trip, the other half becomes an orphan for Spark and dbt alike, counted as *explained by another fault* (3 drifts: 3 dead letters, 3 orphans) |
| 3 | `make fault f=frozen_station station=ST-001` during a rush hour (07–09 or 17–19 sim) | `make scorecard` → `frozen_station` | dbt's `stale_snapshot` (snapshot-by-snapshot) usually finds it. Spark's 2-hour window only finds long episodes at busy stations (a 2 h 11 min freeze, split across two windows: 11 stale snapshots in dbt, nothing in Spark) |
| 4 | `make fault f=silent_station station=ST-001` | `make alerts`, then the scorecard → `silent_station` | a Spark `silent_station` alert once a 2-hour window lies inside the episode and closes (0 status reports, while 28 trips started or ended there); dbt's `reporting_gap` spans the whole episode |
| 5 | `make fault f=stream_stall` | bridge `throughput` lines: `docker compose logs -f --no-log-prefix bridge \| jq -c 'select(.msg=="throughput")'`; `make alerts` | the stream pauses 15–60 real seconds: `per_s` falls to 0 while `source_lag` climbs (493), then the backlog flushes in a burst. Events at least 25 sim-min late become `late_event` for Spark and dbt (233 each, for a 48-minute stall). Spark keeps the whole backlog: it arrives in order, ahead of a watermark that stood still during the stall, so `dropped_late` stays at its usual 0–2 per batch. Spark's alerts reach the warehouse one landing later than the events |
| 6 | `make ff h=24` | `make clock` (backlog); bridge `per_s`; Spark batch sizes; `make warehouse` | a burst through every hop: the day (86,400 simulated seconds, ~16.5k events) is generated in under a second, the bridge sends it at ~550 events/s, Spark reads it in one batch (16,452 rows, under its 20k cap: `h=48` shows the cap), and one landing run catches up |
| 7 | `docker compose kill -s KILL bridge && docker compose start bridge` | `make quality` → `resent_by_bridge` (after the next landing + dbt run) | a few bridge re-sends (2; at most 2 s of events), removed by staging dedup, never counted as source duplicates. The restart takes ~30 s, because compose re-runs `kafka-init` first |
| 8 | `docker compose kill -s KILL airflow-scheduler` during a `land_*` task, then `docker compose start airflow-scheduler` | the Airflow UI: the task retries after ~60–90 s (79 s, as attempt 2); the invariant query below | no row lost or duplicated: the killed attempt left no batch in `raw._ingest_batches`, the retry landed the slice once |
| 9 | `make spark-reset && make up` | Spark UI; `make lake`; `make alerts`; `make warehouse` | Spark replays every retained Kafka message in 20k-row batches (~366k messages in 3.5 min) and rebuilds the lake and the alerts topic. `spark-reset` also makes the warehouse forget the alerts it landed from the deleted topic, so the next landing re-lands the new topic from offset 0. The replayed alerts are close to the originals, not identical: fewer orphans (a late half now arrives in the same large batch as its partner: 59 fewer, all false positives), and fewer over-capacity alerts, because Spark judges old readings against today's capacities (4 fewer, all at ST-024, which grew from 36 to 40 docks since; dbt's as-of join still counts them) |
| 10 | `make pause`, wait ~25 min, `make resume` | `stream_landing` runs (task `report`); `dbt_transform` runs and what triggered them (Assets view); freshness in their `source_freshness` task log | `stream_landing` lands nothing, so `report` is skipped and emits no `raw_stream` event. dbt keeps running anyway: `api_extract` emits `raw_api` every 15 min, because its `report` never skips. Once the last landed event is 15 min old, those runs warn on `raw.trip_events` and `raw.station_status` (`stations` passes: every extract refreshes it); the runs still succeed. (Paused 21:09; last rows landed 21:13; the run triggered by the 21:33 extract warned) |

**The exactly-once invariant** (experiment 8). Open the *warehouse* read-only: the `raw` schema isn't in the serving copy. Retry if a DAG is writing at that moment.

```sql
-- harlequin --read-only data/warehouse/bikeshare.duckdb   (or duckdb in Python)
SELECT t.topic, t.rows, o.positions, t.rows = o.positions AS exactly_once
FROM (SELECT topic, count(*) AS rows FROM raw.trip_events GROUP BY topic
      UNION ALL SELECT topic, count(*) FROM raw.station_status GROUP BY topic) t
JOIN (SELECT topic, sum(next_offset) AS positions FROM raw._kafka_offsets GROUP BY topic) o USING (topic);
```

---

## 6. Reading the scorecard

`marts.mart_detection_scorecard` has one row per (check × fault type it targets). The targets are defined in `transform/seeds/detection_checks.csv`.

| Column | Meaning |
|---|---|
| `detector`, `check_name` | `bridge` (contract), `spark` (streaming rules), `dbt` (batch checks), `any` (all checks together, recall only) |
| `detections`, `true_detections`, `precision` | how many findings the check made, and how many point at a real fault of a targeted type |
| `explained_by_other_faults` | "false" findings caused by *another* fault: orphans whose other half was dead-lettered, stale snapshots that counted a teleported arrival |
| `faults_judged`, `faults_found`, `recall` | injected faults of that type old enough to judge, and how many the check found |

Only data up to `least(latest event − 4 h, latest extracted fault)` is judged. Without that bound, detections of faults not extracted yet would look false.

**Things to notice:**

- **Complementary checks.** On `impossible_status`, the bridge (negative counts) plus Spark or dbt (over capacity) reach 100% together.
- **Streaming vs batch.** On `frozen_station`, Spark's windows are a heuristic; dbt's sequential check is exact but noisier.
- **Honest numbers.** Every rate here was measured, and the investigations behind them are in `logging_build.md`.

---

## 7. Where the state lives

```
data/
  source/state.pkl                 the world, clock, event buffer, fault log     (source-api)
  bridge/checkpoint.json           last source seq known to be in Kafka          (bridge)
  checkpoints/<query>/             Spark offsets, commits, state stores          (spark)
  lake/station_metrics/date=…/     Spark's Parquet windows                       (spark → dbt)
  warehouse/bikeshare.duckdb       raw, staging, intermediate, snapshots, marts, audit   (Airflow + dbt)
  warehouse/bikeshare_serving.duckdb   marts only, for readers                   (publish_serving)
transform/
  target/, logs/                   dbt artifacts and logs                        (Airflow's dbt)
  target-host/, logs-host/         the same, for `make dbt`                      (you)
  target-docs/                     docs + bikeshare.duckdb, a structure-only copy (`make dbt-docs`)
docker volumes: kafka-data, airflow-db, airflow-logs
```

Useful resets:

| Target | What it wipes |
|---|---|
| `make source-reset` | the world only (breaks consistency; prefer `make reset`) |
| `make bridge-reset` | the bridge checkpoint: it replays the source buffer |
| `make spark-reset` | Spark's checkpoints, lake and alerts topic, and the alerts the warehouse landed from that topic: a replay from Kafka, re-landed from offset 0 |
| `make reset` | everything, consistently |

**Exploring without lock conflicts:** use the serving copy (`make sql`). For raw or staging, work on a private copy:

```sql
-- in duckdb (python): an in-memory session
ATTACH 'data/warehouse/bikeshare.duckdb' AS live (READ_ONLY);
ATTACH '/tmp/my_copy.duckdb' AS mine;
COPY FROM DATABASE live TO mine;
```

---

## 8. What is not here (yet)

- **Natural extensions:**
  - Schema Registry (Avro or Protobuf) instead of JSON;
  - a Spark cluster or Spark Connect instead of `local[4]`;
  - astronomer-cosmos, to render dbt models as Airflow tasks;
  - DuckLake for multi-writer tables;
  - Prometheus and Grafana for metrics;
  - an alert-routing consumer (Slack or email) on `bikeshare.alerts.v1`.
