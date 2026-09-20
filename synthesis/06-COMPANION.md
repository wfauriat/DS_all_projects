# Companion — skills, idioms, and navigation charts

Two parts.

- **Part I — The catalogue.** What is worth knowing and remembering from these four codebases,
  organised along the data pipeline. Every entry names the file that demonstrates it, so it can be
  read rather than taken on trust.
- **Part II — Navigation charts.** One per project: entry points, architecture, module map, stack,
  and a suggested reading path.

Project shorthand used throughout: **DE** = `DE_pipeline_stream`, **ME** = `mediaElection27`,
**EP** = `energy_predML`, **RL** = `refund-lab-autom`.

---

# Part I — The catalogue

## 1. Ingestion

### 1.1 Know your source before you design against it
Measure, don't assume. RL's `NOTES.md` §2 is the model: nine empirical facts (page cap, ambient error
rate, token TTL, latency, drop-writer delay, identifier width, "catalogue snapshots are frequently
byte-identical"), each paired with the design consequence. The last one alone prevented a guaranteed
false positive.

> **Remember:** the cheapest hour in any integration is the one spent with `curl` before writing code.

### 1.2 Pull patterns — one per dataset shape
There is no single "extract" step; there is a pattern per dataset.

| Shape | Pattern | Seen in |
|---|---|---|
| small, changes in place | **full snapshot, replaced** (`DELETE` + insert) | DE `extract_stations/bikes`; ME alias reseed |
| append-only, time-keyed | **incremental by `max(timestamp)`** | DE `extract_weather`; EP `fetch_demand` defaults |
| append-only, id-keyed | **incremental by `max(id)`, paged** | DE `extract_fault_log` |
| a stream | **bounded slice**: snapshot end offsets, read `[start, end)` | DE `kafka_loader.plan_slices` |
| a source with no "since I last looked" | **rolling + reconciliation + rotating audit** | RL `extract_api.py` |
| files with no manifest | **scan for unknown names, decide everything from the bytes** | RL `extract_drop.py` |

### 1.3 The "what became knowable since I last looked" problem
RL's central lesson. When the API can filter *valid time* and *knowledge time* but not *arrival*, no
single mechanism suffices. Four, with costs:
1. **rolling** re-pull of the last N valid days (cheap; catches ordinary lateness);
2. **count reconciliation** over coarse buckets, **bisected** down to the offending day (finds both
   appearing *and disappearing* rows without re-reading history);
3. **rotating deep audit** of individual historical days (catches restatements, which change no count);
4. **bootstrap** once.

Cost control: buckets rotate on a `(from, to, every_n_cycles)` schedule; a cycle costs ~20 requests
instead of ~50. And the discipline that keeps it honest: **`total_count` is a change hint, never a
completeness check** — every conclusion from it is confirmed by a real pull.
→ `RL/pipeline/extract_api.py`, `RL/pipeline/orchestrator.py::_bisect`

### 1.4 Absorb transport hostility in exactly one module
Everything ugly about HTTP in one place, so the rest of the code reads as if the source behaved:
token caching + re-auth on 401 *and* on a 200 body saying `token_expired`; jittered backoff honouring
`Retry-After`; **content-encoding sniffed from the bytes, never the header**; cursor validation
(repeated cursor, non-moving cursor, empty page claiming a successor, already-seen keys); a
per-cycle stats object.
→ `RL/pipeline/vendor.py`

Two refinements worth stealing:
- **Retry semantics differ by question.** `get_json` retries a 503 for 17 s; `probe()` does not,
  because "is the source up right now" is a different question from "get me this data".
- **Ambient hostility is counted, not reported.** 2.7 % 429s are the environment; only a *sustained*
  outage is an incident.

### 1.5 Async fetching with bounded concurrency
One `httpx.AsyncClient` + `asyncio.Semaphore(n)` + `asyncio.gather`; a per-source result object that
**never raises** — a network error becomes `status=0, error="..."` in the result, so one dead feed
cannot abort the batch. Conditional-GET headers (`If-None-Match`, `If-Modified-Since`) prepared per source.
→ `ME/app/ingest/fetcher.py`

### 1.6 Tolerant parsing with a sanity window
Feed dates are a mess. The pattern: try the structured field, then the free-text field, then the
channel-level fallback; assume a *named* timezone for naive values; convert to UTC; then **reject
anything absurd** (< 1990, > now + 7 days). Skip rows missing a URL or title, and say so.
→ `ME/app/ingest/parser.py::normalise_pubdate`

### 1.7 Decide file facts from the bytes
Compression from the magic number (`raw[:2] == b"\x1f\x8b"`), not the extension. Encoding by
attempting a strict UTF-8 decode and *recording* the fallback. Truncation from a short gzip stream
(salvage what decompressed with `zlib.decompressobj`), a missing trailing newline, or a final row with
the wrong field count. Header diffed against the previous file of the same kind. sha256 of every file kept.
→ `RL/pipeline/extract_drop.py::DropFile`

### 1.8 SSE, hand-rolled
Server-Sent Events is ~40 lines: lines until a blank line make a frame; `id:` / `event:` / `data:`;
`:` starts a comment. Resume with the `Last-Event-ID` header. A ring buffer on the server plus a
**gap notice** when the resume point has fallen out of it is the whole delivery contract.
→ `DE/ingest/bikeshare_bridge/sse.py`, `DE/source/bikeshare_sim/eventlog.py`

---

## 2. Contracts, validation and normalisation

### 2.1 Syntax at ingestion, semantics downstream
DE's doctrine, and the cleanest available statement of where validation belongs:
- `bikes_available < 0` violates the **contract** (shape/types/range) → reject at ingestion.
- `bikes_available > capacity` needs **reference data** → a semantic check, downstream.

### 2.2 Producer and consumer share a contract, not code
DE keeps two near-identical Pydantic modules on purpose (`source/.../schemas.py` and
`ingest/.../contract.py`). Sharing the class would make a producer change silently a consumer change.
Strictness matters: `ConfigDict(extra="forbid", strict=True)` is what makes `"754s"` fail as an int and
a renamed field fail as an unknown key.

### 2.3 Dead-letter with the evidence
A rejected record goes to a DLQ carrying `error_type` (classified: `unparseable` /
`unknown_event_type` / `contract_violation`), the list of validation errors, the source position, and
**the original text** so it can be replayed once fixed.
→ `DE/ingest/bikeshare_bridge/contract.py::_dead_letter`

### 2.4 When you cannot reject: repair, but record
RL's three principles when the vendor is not yours:
1. **Never drop a field** — unknowns go to `extra_json` and raise a schema-drift signal. *"A field we
   throw away is a fault we cannot see."*
2. **Repair, but record** — an id that lost its leading zeros is re-padded so the join survives, and
   the repair is counted, *because the repair is the evidence*.
3. **Never raise** — an unparseable row is written with nulls and counted.
→ `RL/pipeline/normalize.py`

Two details worth memorising:
- **Follow renames by similarity**: one expected field missing + one unknown present →
  `difflib.SequenceMatcher` ratio + a bonus for a shared underscore token, threshold ~0.55. Downstream
  keeps working *and* the mapping is reported.
- **Sentinels are not nulls.** `-999`, `1900-01-01`, `N/A` are values pretending to be data → map to
  NULL and report. Bare `""` / `null` / `none` are how a source says "nothing" → do **not** report, or
  you have built a false-positive generator.

### 2.5 Mojibake detection that doesn't fire on real text
Require the *pair*: an accented capital (`Ã`, `Â`, …) followed by a high byte. `Åland` is a real place;
`Ã³` is not. Verified against a list of real and corrupted city names.
→ `RL/pipeline/normalize.py::looks_mojibake`

### 2.6 Validation at both ends of a wire
ME validates with Pydantic on the way out and **Zod on the way in**, with `z.infer` giving the
TypeScript type. Runtime validation at the API boundary catches the schema drift that a compile-time
type cannot.
→ `ME/frontend/src/api/timeseries.ts`

---

## 3. Storage and modelling

### 3.1 ELT: land raw, type later
Raw tables keep envelope columns plus the payload **as text, not as a JSON type**, "so a malformed
message can never block landing". All parsing and typing happens in the transform layer.
→ `DE/orchestration/include/landing/warehouse.py`

### 3.2 Bitemporality, and why it should be built first
Two layers:
- `src_<entity>`: **append-only**, PK `(business key…, version)`, carrying the source's
  `knowledge_time` and your `first_seen_sim`. Never updated, never deleted.
- `cur_<entity>`: current belief, maintained by a **version-guarded upsert**
  (`ON CONFLICT … DO UPDATE … WHERE excluded.version > cur.version`).

What it buys: *"what did the source say on date X"* becomes `WHERE knowledge_time <= X` with the max
version — a query, not an archive restore. Half of RL's fault catalogue (restatements, late arrivals,
silent deletion, censoring) is **only visible** this way. What it costs: nothing per cycle, because
`cur_*` is incremental; rebuilding "latest version per key" from history each cycle is what would have
made a 2200-cycle run unaffordable.

RL's own retrospective: *"Bitemporal storage should have been the first thing built, not the second."*
And the generalised blind spot it caused: **every channel restates, not just the versioned one** — the
API announces a restatement by bumping `version`; a file drop tells you nothing at all, and the only
way to see it is to keep every delivered row and compare.
→ `RL/pipeline/storage.py`, `RL/asof.py`

### 3.3 Tombstones, never deletes
A row the source stops serving gets `deleted_sim` set. Historical facts must still join, and the
disappearance becomes queryable evidence rather than silence.

### 3.4 SCD2 as a *join condition*, not a history archive
DE's `snap_stations` (a dbt snapshot, `strategy: timestamp`, keyed on the source's own `updated_at`)
exists for one reason: *"more bikes than docks" must be judged against the capacity the station had at
the time of the event.* The **as-of join** is the point:
```sql
join snap_stations v
  on v.station_id = s.station_id
 and s.event_time >= v.dbt_valid_from
 and s.event_time <  v.dbt_valid_to
```
and `dbt_valid_to_current: '9999-12-31…'` instead of NULL is what makes "valid at t" a plain `BETWEEN`.

This is also the sharpest *comparison* in the corpus: Spark refreshes station reference data from the
API every 60 s, which is correct live but wrong on a replay (it judges old readings against today's
capacities). dbt's as-of join has no such gap.

### 3.5 Layers as schemas
`staging` (one view per raw table: JSON → typed, deduplicated, no business logic) → `intermediate`
(reusable building blocks) → `marts` (what people query), plus `snapshots`, `reference` (seeds) and
`audit` (stored test failures). Override `generate_schema_name` so the layer *is* the schema.
→ `DE/transform/dbt_project.yml`, `DE/transform/macros/generate_schema_name.sql`

### 3.6 Dedup that keeps the reason
```sql
qualify row_number() over (partition by event_id order by loaded_at, "partition", "offset") = 1
```
…plus **two counters** that distinguish the cause:
- `copies` = how many times this `event_id` landed;
- `source_copies` = how many distinct source sequence numbers carried it.

`source_copies > 1` → the **source** duplicated it (a data fault). `copies > source_copies` → the
**bridge** re-sent it (at-least-once delivery working as designed). Same symptom, different diagnosis.
→ `DE/transform/models/staging/stg_trip_events.sql`

### 3.7 Dedup scope is an analytical decision
ME shipped `UNIQUE(content_hash)` across all articles, then **removed it in a migration** because it
collapsed the same AFP wire republished by several outlets into one row — and "each outlet's pick-up is
a distinct data point". The migration docstring says exactly that.
→ `ME/app/db/migrations/versions/98d3055b30f7_*.py`

### 3.8 Incremental on *landing* time, not event time
```sql
{% if is_incremental() %} and loaded_at > (select max(loaded_at) from {{ this }}) {% endif %}
```
Filtering on `loaded_at` with `incremental_strategy='delete+insert'` and `unique_key='trip_id'` is
precisely what makes late events safe: a `trip_ended` delayed three hours still has a fresh
`loaded_at`, so its trip is reprocessed and upserted.
→ `DE/transform/models/marts/fct_trips.sql`

### 3.9 Sequential checks that windows cannot do
```sql
lag(bikes_available) over (partition by station_id order by event_time)
```
plus a range join to the trips that happened between two snapshots gives an **exact** frozen-station
check (identical values although the net flow was non-zero) where a streaming window can only offer a
heuristic. The trip ids are kept in a list column so a finding can be traced back to its evidence.
→ `DE/transform/models/intermediate/int_status_sequence.sql`

### 3.10 The audit table lives in the data warehouse
All four put "what the pipeline did" next to "what the data says", so both are answerable in SQL:
`raw._ingest_batches` / `raw._kafka_offsets` (DE), `ingest_runs` (ME), `predictions` + `model_runs`
(EP), `pull_log` / `day_counts` / `test_results` / `metrics` (RL).

### 3.11 An embedded database has exactly one writer — design around it
DuckDB's single-writer lock drives five separate mechanisms in DE:
1. an Airflow **pool with one slot** on every writing task;
2. short connections (open → one transaction → close) with a **retrying `connect()`**;
3. `SET TimeZone='UTC'` on connect (otherwise TIMESTAMPTZ comes back in the host's local zone);
4. an **atomic serving copy** for readers;
5. a **structure-only copy** for `dbt docs generate`, which otherwise held the lock for seconds and
   failed about one minute in five.
→ `DE/orchestration/include/landing/warehouse.py`, `DE/orchestration/include/serving/publish.py`

---

## 4. Orchestration

### 4.1 Streams are services; orchestrators run bounded batches
DE's rule. Kafka and Spark run under compose `restart:` policies (Kubernetes' role in production);
Airflow only ever runs work that ends. Everything else follows from it.

### 4.2 Exactly-once into a warehouse — the whole argument
```
1. start  per partition, the next offset stored IN THE WAREHOUSE (or the earliest retained)
2. end    per partition, the end offset snapshotted when the task starts
3. read   the slice [start, end)
4. write  ONE transaction: rows + new offsets + an audit row
5. show   only then, commit the same offsets to Kafka — for the lag display, never to resume
```
A crash before step 4 leaves both untouched and the next run re-reads the slice; a crash after leaves
both advanced. A primary key on `(topic, partition, offset)` is the second line of defence. This was
verified by killing the scheduler mid-landing.
→ `DE/orchestration/include/landing/kafka_loader.py`

**The corollary nobody expects:** offsets number the messages of *one* topic. A topic deleted and
recreated starts at 0, so the stored positions silently skip everything below them. Resetting a job
therefore means resetting **its progress and its outputs together** — DE's `spark-reset` deletes
checkpoints, the lake, the alerts topic *and* the warehouse rows landed from it, in one transaction,
only once Kafka confirms the topic is gone.

### 4.3 Data-aware scheduling
A DAG with **no clock**: `schedule=(RAW_STREAM | RAW_API)` — an asset expression. Upstream DAGs declare
`outlets=[ASSET]` and attach row counts via `outlet_events[ASSET].extra`. And the key detail:
**raise `AirflowSkipException` when nothing landed**, because a skipped task emits no asset event, so
nothing downstream runs for nothing.
→ `DE/orchestration/dags/*.py`

### 4.4 Keep the business logic out of the orchestrator
DE's `include/landing/` and `include/serving/` are plain Python packages with **no Airflow import**.
The DAGs only schedule them. Consequences: unit-testable without Airflow installed, and the same
package is imported by host scripts (`peek_warehouse.py`, `smoke.py`). EP does the same with Prefect
(`@task`s are thin wrappers over plain functions).

### 4.5 Connections and variables from the environment
`AIRFLOW_CONN_<ID>` / `AIRFLOW_VAR_<KEY>` set in compose. Nothing stored in the metadata database,
nothing clicked in the UI, the whole wiring readable in one file.

### 4.6 Task factories
```python
def land(topic, table):
    @task(task_id=f"land_{table}", pool="duckdb")
    def land_topic() -> dict: ...
    return land_topic()

landed = [land(t, tbl) for t, tbl in TOPIC_TABLES.items()]
prepare() >> landed
report(landed)
```
A closure returning a decorated task with a computed `task_id` — fan-out without repetition.

### 4.7 Recovery is a tuning problem with a measurement
A task whose process died stays "running" and **holds its pool slot**. With one slot, everything
queues. DE measured five minutes of blocked landing, then set
`orphaned_tasks_check_interval=60` and `task_instance_heartbeat_timeout=90` (defaults 300/300) and
re-measured: 87 s. The note also records that the *first* fix tried (heartbeat timeout alone) did not
work, because the orphan check is what recovers a dead scheduler's tasks.

### 4.8 When the clock is simulated, the runner owns it
RL advances the world **outside** the cycle, so a cycle that fails is retried against the *same*
simulated day rather than burning another one. It also advances in thirds, probing availability at
each step, because a compressed one-touch-per-day loop would step over a multi-hour outage that an
hourly pipeline would have sampled 24 times.
→ `RL/pipeline/orchestrator.py::run`, `::advance_sampled`

### 4.9 Overlap the dead time
RL's dominant per-cycle cost was a 1.6 s wait for the vendor's file writer — pure dead time. It now
runs in a background thread while the API channel is extracted. The discipline that makes it safe:
**only the filesystem is touched from the thread**; nothing is parsed or written until the main thread
joins it.

### 4.10 Event-driven instead of scheduled
ME's second hop has no scheduler at all: S3 `ObjectCreated` triggers the loader Lambda. The cron only
starts the chain. And the bucket lives in the *same* CDK stack as the loader to keep the notification
intra-stack and avoid a cross-stack cycle.

---

## 5. Streaming

### 5.1 One query per pattern, one sink per query
DE runs four Structured Streaming queries rather than one, and says why: a query has one sink, and
stateless and stateful logic do not mix. The clearest case is **late events** — the stateless rule
query must *see* them to flag them, while the windowed queries must ignore them past the watermark.

| Query | Pattern | State | Sink |
|---|---|---|---|
| `reference_rules` | `foreachBatch` + `persist()` + broadcast join to API-refreshed reference data, **no watermark** | none | Kafka |
| `station_metrics` | watermark + `dropDuplicatesWithinWatermark` + tumbling windows | RocksDB | Parquet (**exactly once** via `_spark_metadata`) |
| `station_health` | the same windows, longer | RocksDB | Kafka |
| `trip_pairing` | stream-stream **full outer** join + time bound | RocksDB | Kafka |

### 5.2 Rules as pure `DataFrame -> DataFrame`
Structured Streaming uses the same API for streams and tables, so every rule runs unchanged on a
streaming DataFrame in production and on a small static one in a unit test with a local `SparkSession`.
This is the single highest-leverage structural decision in the Spark layer.
→ `DE/streaming/analyzer/rules.py`, `DE/streaming/tests/conftest.py`

### 5.3 Deterministic ids make an at-least-once sink safe
```python
alert_id = substring(sha2(concat_ws("|", alert_type, entity_id, coalesce(event_id, window_start)), 256), 1, 16)
```
A replayed micro-batch produces *identical* rows, so consumers dedup by id. Pair it with
`to_json(..., {"ignoreNullFields": "false"})` so every record carries every field with explicit
nulls — one stable schema instead of keys that come and go.

### 5.4 Stream-static joins go stale
Reference data loaded once at startup is wrong the moment the reference changes. Refresh it on a TTL
inside `foreachBatch` (which runs on the driver, once per micro-batch), and **keep the previous copy
if the refresh fails**.
→ `DE/streaming/analyzer/reference.py`

### 5.5 A windowed heuristic is not an exact check — measure the difference
30-minute health windows gave ~8 % precision on frozen stations; 2-hour windows ~86 %. Both
require `departures + arrivals ≥ 3`, because a quiet station at 3 a.m. legitimately repeats itself.
The exact check is sequential and belongs in batch SQL. DE then builds `mart_stream_vs_batch` to
compare the two computations — **only over windows that are final on both sides**.

### 5.6 Three consumers, three ways to remember a position
| Consumer | Position stored in | Committed to the broker? |
|---|---|---|
| bridge | a checkpoint file (a source sequence number), saved **after** `flush()` confirms | – |
| Spark | its own checkpoint directory (offsets + commits + state) | no |
| warehouse loader | `raw._kafka_offsets`, **in the same transaction as the rows** | yes, for the lag display only |

### 5.7 Producer hygiene
`enable.idempotence=True` (implies `acks=all`), `compression.type=zstd`, `linger.ms=20`,
`delivery.timeout.ms`. Delivery is asynchronous: `produce()` only enqueues; the acknowledgement comes
through a callback served by `poll()`/`flush()`. **Move the checkpoint only after `flush()` succeeds.**
On `BufferError`, `poll()` and retry — that backpressure propagates all the way up to the source read.
→ `DE/ingest/bikeshare_bridge/sink.py`

### 5.8 Kafka topology facts worth internalising
- **Two listeners** (INTERNAL/EXTERNAL) exist because a client connects to a bootstrap address and is
  told which address to use next; `kafka` does not resolve on your host and `localhost` inside a
  container is the container.
- **The key picks the partition**, and the partition is the unit of ordering. Key trips by `bike_id`,
  status by `station_id`.
- **Declare topics explicitly**; disable auto-creation so a typo fails loudly instead of silently
  creating a topic with default settings.
- **Naming**: `<domain>.<dataset>.v<major>`; a breaking payload change means a new topic, not an edit.

---

## 6. Data quality and detection

### 6.1 Tests as monitors
A dbt singular test that is *expected* to find rows, at `severity: warn` with `store_failures=true`
and its own `audit` schema, is a monitor with a query interface. The build stays green; the rows
accumulate where you can inspect them.
→ `DE/transform/tests/status_above_capacity.sql`

### 6.2 The full dbt test taxonomy, in one project
generic built-ins (`unique`, `not_null`, `accepted_values`, `relationships`) · a **custom generic**
(`{% test within_range(model, column_name, min_value, max_value) %}`) · **singular** tests · a **unit
test** (hand-written `given` rows, `expect` compared as a table — logic checked independently of
today's data) · an **enforced contract** (`config: contract: {enforced: true}` + typed columns) ·
**source freshness** on wall-clock landing time.

### 6.3 Score your detectors against ground truth
If you can generate the faults, you can measure the detection. DE's `mart_detection_scorecard`:
- a **seed CSV** `(detector, check_name, fault_type, match_on)` drives the matching, so adding a
  detector means adding rows, not rewriting SQL;
- `match_on ∈ {event, trip, episode, stall}` selects the join predicate;
- precision, recall, and a `detector='any'` roll-up;
- **`explained_by_other_faults`** — a "false" finding caused by a *different* real fault (an orphan
  trip whose partner was dead-lettered) is reported next to precision, not hidden;
- a **judging horizon** `least(max(event_time) − N hours, max(injected_at))`, so a fault too fresh to
  detect, or not yet extracted, is not counted as a miss. Without it, `late_event` looked 82 % precise;
  with it, 100 %.

### 6.4 Specificity over sensitivity, when a claim costs something
RL's regime. The rules, all transferable to any alerting system where someone has to read the alerts:
- **Build on facts that cannot happen by chance** (a value outside a closed domain, a truncated gzip
  stream, a timestamp offset in a naive feed) rather than thresholds over noisy series.
- **Enumerate the baseline you must not fire on** (weekday seasonality, 18 % growth, Q4 peak, Black
  Friday at 3×, catalogue turnover) and treat it as a test set.
- **One line per named fault.** Repeats increment a counter; they do not append. *"A fault reported
  five times is not five detections, and repeated claims can only add false positives."*
- **Learn undocumented domains** before claiming on them: 90 days of observation, and a novel value
  needs ≥2 % share and ≥10 occurrences.
- **Detect in the loop**, so the detection timestamp means something.

### 6.5 Telling confusable faults apart is most of the work
| Pair | The discriminator |
|---|---|
| category rename vs new category | did an **established value stop** at the same moment? |
| late partition vs missing delivery | arrived after being **observed** absent, vs still absent 4+ days later |
| silent row deletion vs survivorship | **which entity** lost the rows |
| clock skew vs late-arriving events | minutes & **all** timestamps & `knowledge_time < occurred_at`, vs days & **specific** partitions |
| id reuse vs category rename | **immutable** attributes of a few keys, vs one categorical across the whole feed |
| field rename vs leakage column | one field disappears as another appears, vs a field that separates the outcome |

### 6.6 Retraction as an engineering practice
RL withdrew four claims into `retractions.jsonl`, each with its reason, under one rule:
**an entry is removed only when the detector that produced it is found mis-specified and is fixed in
the same change** — never because it turned out not to match, and nothing is ever added after the fact.

The three retractions are each a lesson:
1. **Do the Poisson arithmetic before trusting a rare-event detector.** A night hour holding 1.5 % of a
   ~500-event day is empty by chance ~1 in 2000; the horizon has ~50,000 hour-checks; ~25 such hours
   are *expected*. The detector was a false-positive generator by construction.
2. **A distribution metric must be computed on the common support.** A new level arriving made
   PSI = 1.06 through a near-zero denominator; over the shared levels the same comparison gave 0.0003.
   **A new level is a schema event, not a distribution shift**, and belongs to a different detector.
3. **Two detectors naming one event is not coverage — it is a guaranteed false positive** whenever the
   second name is not the right one. Aggregate-statistic detectors must be told about the events their
   deterministic siblings already own.

Plus a self-inflicted one worth keeping: a retraction script corrupted the scored deliverable because
it was edited by an ad-hoc script with no read-back check. *"The retraction path deserved the same care
as the write path."*

### 6.7 State your gaps explicitly
RL's coverage table lists all 30 possible faults × the detector × the signal × **the risk of missing
it**, and closes with an honest count: 22 deterministic/reconciliation-based, 5 on blind thresholds,
3 with a structural gap left open *on purpose* because closing it would cost more in false positives
than it buys. A table like this is worth more than a claim of completeness.

---

## 7. Machine learning

### 7.1 Leakage prevention is a coding convention
Every rolling and difference feature is `.shift(1)`-ed **before** the window, so the current row's own
target can never enter its features — and a unit test encodes the invariant with literal index
arithmetic (`result.iloc[30]["demand_lag_24h"] == df.iloc[6]["demand_mwh"]`).
→ `EP/src/features/lag.py`, `EP/tests/test_features/test_lag.py`

### 7.2 Temporal splits, always
`df.iloc[:split]` for train/validation; `TimeSeriesSplit` for cross-validation. Never a shuffle, never
`KFold`. Any reported metric from shuffled time-series data is fiction.

### 7.3 Cyclical encoding
`sin/cos(2π · x / period)` for hour-of-day and day-of-week, so hour 23 and hour 0 are adjacent in
feature space. A raw integer cannot express that; one-hot throws away the ordering.

### 7.4 A baseline that is one column
`y_pred = df["demand_lag_24h"]`, logged to the tracking server with the same metrics as the real model.
RMSE 1927 → 564 → 501 is a story; "RMSE 501" alone is not. The cheapest honesty mechanism there is.

### 7.5 Eliminate train/serve skew by construction
The serving path imports the *same* feature functions used in training, and one constant
(`NON_FEATURE_COLS`) defines the feature set for training, serving, monitoring and the dashboard.
Inference then builds features at request time from stored history (14 days, ≥169 rows required),
appending a placeholder row for the target hour.
→ `EP/src/serving/predict.py`

### 7.6 The registry is the deployment mechanism
The API resolves `max(version)` from the model registry at startup, so retraining promotes itself with
no config change — and it fetches that run's logged `rmse` at the same time, to size the confidence
interval (`± 1.96 × RMSE`, with a documented fallback). **The model and its uncertainty travel
together.**

### 7.7 Version the model inside the data
`mentions(… extractor, extractor_version …)` with the version in the unique key (ME), and
`predictions(order_id, model_version, …)` (RL). Several extractors or model versions coexist without
clobbering each other, comparison is a `GROUP BY`, and rollback is a `WHERE`.

### 7.8 Censoring: absence of a label is not a negative label
A non-refunded order has **no label row at all** until its 30-day window closes. Training on
"no label = not refunded" teaches the model that recent orders are never refunded. Restrict training to
orders whose window has closed *and* which carry a label row; everything younger is undetermined.
→ `RL/pipeline/ml.py`

### 7.9 Point-in-time features
Customer-history features count only prior orders, and only the labels whose `knowledge_time` preceded
the order being scored. The loader is as-of too: `… WHERE knowledge_time <= ?`. A refund that settled
afterwards cannot inform the prediction for the order that preceded it.

### 7.10 A leakage screen that raises an alarm
At every retrain, compute a univariate AUC for each numeric feature; anything with
`max(auc, 1-auc) > 0.90` is treated as leakage — **reported as an incident** and dropped from the
model. And the honest footnote: it has dropped nothing, which is correct if no leakage was injected and
a miss if it was.

### 7.11 Evaluate prospectively, not on the holdout
*"Not the training holdout — that is a self-report."* RL's `evaluate.py` scores every order with the
number **actually written at ingest time by whichever model was live then**, and compares it against
the outcome that arrived weeks later. Hand-rolled tie-aware AUC and equal-count-bin ECE.

### 7.12 Know what horizon you actually serve
EP's 1-step-ahead RMSE of ~500 MWh does not transfer to a 24 h forecast.
`scripts/compare_models.py` implements the recursive forecast honestly — each prediction is written
back into `demand_mwh` and into the downstream lag cells before the next step, so the horizon uses only
predicted or genuinely past values.

### 7.13 Drift monitoring that closes the loop
1. **Log every served prediction** to a table keyed on `(timestamp, entity)`.
2. **Inner-join predictions with actuals** when the actuals arrive → real measured error, not an
   input-distribution proxy. Require a minimum number of matched rows (48) before reporting.
3. **Anchor the reference set to the training cutoff** (`max(timestamp)` at training time, written to a
   file). Reference = what the model was trained on; current = what arrived since. Only under this
   definition is "monitor right after retraining ⇒ no drift" true.
4. **Threshold in config, not in code** (`DRIFT_THRESHOLD`), and a **flag file** plus a lazy import as
   the control channel between a fat local environment and a slim monitoring container.
→ `EP/src/workflows/monitor.py`

---

## 8. Serving and interfaces

### 8.1 Readers never take the writer's lock
Publish a **serving copy**: build under a temporary name, then `os.replace`. Readers see the old file
or the new one, never half of each, and never wait. The reader half: cache keyed on the file's **mtime**
(the data version), plus a small timer that compares mtime and reruns when a new copy lands.
→ `DE/orchestration/include/serving/publish.py`, `DE/dashboard/app.py`

### 8.2 A dashboard is just another reader of the serving layer
DE's Streamlit app reads the serving copy and nothing else. Nothing about it is special to the
warehouse; an analyst's notebook would sit in the same position.

### 8.3 FastAPI patterns worth keeping
- **App factory** + `lifespan` for startup work (start a background task; load a model).
- One injected dependency alias: `SessionDep = Annotated[AsyncSession, Depends(get_session)]`.
- **Error mapping is part of the contract**: 503 no model, 400 insufficient history, 500 otherwise.
- **Auxiliary work must not fail the request** — prediction logging is wrapped in its own try/except.
- Response models with `from_attributes=True`; `Field(alias="from")` + `response_model_by_alias=True`
  to expose a Python keyword as a JSON key.
- `Mangum(app, lifespan="off")` to run the same app on Lambda behind a catch-all `/{proxy+}` route.

### 8.4 Concurrency model, stated explicitly
DE's source API: one process, one event loop, every route `async def`, and the simulation as a task on
the same loop — therefore a route and a simulation step never interleave and **no locks are needed**.
A plain `def` route would run in a thread pool and race. One sentence in a docstring that saves hours.

### 8.5 Push the time zone into the query
```sql
date_trunc('day', published_at AT TIME ZONE :tz)::date AS day
```
A "day" is the editorial day in the requested IANA zone, parameterised per request — not a UTC day.

### 8.6 Ship the registries with the data
ME's `/timeseries` returns `points` **plus** the full candidate and source registries, so the frontend
renders labels and colours from one request even when the window is empty or filtered.

### 8.7 Frontend patterns
- **URL as the canonical state store** (`useSearchParams`), with one module owning parse/serialise —
  including a documented tri-state: param missing = "all", present-and-empty = "none", list = subset.
- **Zod at the boundary**, `z.infer` for the type.
- Compute in plain functions (`rollupByCandidate`, `enumerateDays`, `buildRows`), then hand the result
  to the chart, so the chart component stays declarative.
- **Enumerate the full day axis** so gaps become zeros rather than missing points.
- **Stable colour per entity id**, and ordering that encodes domain meaning (pipeline order; political lean).

### 8.8 Chart discipline (DE's dashboard, the most deliberate in the corpus)
A palette validated for colour-vision deficiency in light *and* dark mode; a fixed slot per entity
across every chart; thin marks (2 px lines, ≤24 px bars rounded at the data end) with 2 px
surface-coloured gaps and rings; tooltips on every mark; a **table view under every chart**; explicit
`utc` scales so wall-clock instants are not rendered in the viewer's zone; and a series that is almost
all zeros rendered as **a short table, not a chart**.

---

## 9. Environment, packaging, deployment

### 9.1 One lockfile, many artefacts
A **uv workspace**: a virtual root (`package = false`) with members, dependency groups
(`dev`, `warehouse`, `spark`, `dashboard`), and `tool.uv.sources` mapping member names to the
workspace. Each service image then runs
`uv sync --frozen --package <member> --no-editable` **from the same lockfile**, so container and host
resolve identically. Dockerfile layering: copy `pyproject.toml` + `uv.lock` + members' pyprojects →
`--no-install-workspace` (cached), then copy code → install.
→ `DE/pyproject.toml`, `DE/source/Dockerfile`

### 9.2 Per-service images, and what that buys
EP builds four images from four requirements files; the serving image excludes optuna/sklearn/
prefect/evidently, the monitoring image excludes all ML packages. This is not only size — it makes the
`except ImportError` branch in the monitoring workflow a **designed control-flow path**: the same
function auto-retrains locally and writes a flag file in the slim container.

### 9.3 Compose idioms
YAML anchors + extension fields (`x-airflow-common: &airflow-common`, `<<: *airflow-common`) ·
**profiles** for optional subsystems (`stream`, `batch`, `tools`) · healthchecks with
`condition: service_healthy` and `condition: service_completed_successfully` for one-shots ·
`user: "${HOST_UID}:${HOST_GID}"` so bind-mounted files stay yours · `stop_grace_period` sized to each
service's shutdown work · named volumes for state that should survive `down`.

### 9.4 Makefile idioms
`-include .env` + `export` so make and compose agree · `unexport VIRTUAL_ENV` when a global venv would
confuse the tooling · `BUILDX_NO_DEFAULT_ATTESTATIONS=1` (otherwise every build changes the image ID
and needlessly restarts containers) · a self-documenting `help` target · jq programs hoisted into
variables · resets that wipe **all** interdependent state together, because the pieces point into each
other.

### 9.5 Two run modes, documented including the sharp edges
EP's `local-*` and `docker-*` target families share `./data/` but **not** the model store, and the
README says so plainly: *"the two modes do not share trained models — run `local-*` or `docker-*`
consistently within a workflow."* Naming the failure mode is worth more than pretending it doesn't exist.

### 9.6 Artifact stores must not bake in absolute paths
EP's `STRATEGY.md` is a short, reusable RFC: a filesystem tracking backend stores the *absolute* path
where training ran, so any other machine must mount it at the same path. A database backend plus
`--serve-artifacts` makes URIs proxy-relative and all I/O go through the server. Problem → target
architecture → why it works → stepwise plan, in ~130 lines.

### 9.7 Infrastructure as code — the three artefacts
`CDK Python` (a template generator, purely local) → `CloudFormation template` (`cdk synth`, free, no
AWS contact) → `real resources` (`cdk deploy`, a diff against cloud-held state).
**Renaming a construct changes its logical ID**, so CloudFormation destroys and recreates the resource —
"the #1 way to accidentally drop a production database".
Cross-stack references compile to `Outputs` + `Fn::ImportValue`; `add_dependency()` enforces order
where no value reference exists.

### 9.8 Cost as an architectural constraint, traced end to end
ME's chain, worth memorising as an example of a constraint propagating:
```
< $5/month  →  no NAT gateway (~$32/mo)
            →  VPC has only PRIVATE_ISOLATED subnets
            →  the fetcher must live OUTSIDE the VPC (free Lambda egress)
            →  everything touching RDS lives INSIDE it, reaching S3 by a free gateway endpoint
            →  Secrets Manager is unreachable at runtime (an interface endpoint costs ~$14.60/mo)
            →  credentials are injected as plaintext env vars, resolved by CloudFormation at deploy
            →  an explicitly documented and accepted security trade-off
```
Plus the guardrails: budget alarms at 80 %/100 %, S3 lifecycle (Glacier-IR @30 d, expire @365 d),
`log_retention=ONE_WEEK` on every function, everything tagged for Cost Explorer.

### 9.9 CI/CD
- Branch CI: lint → type-check (**`mypy --strict`**) → **unit tests only** (they must not need a database).
- Deploy on `main` with `paths-ignore` for docs, **OIDC federation** (`permissions: id-token: write`)
  so there are no long-lived cloud keys, `cdk synth` as a sanity gate, then a two-phase deploy: backend
  first, read the API URL from CloudFormation outputs into `$GITHUB_OUTPUT`, build the frontend with it
  baked in, deploy the frontend.
- The one-time manual setup steps documented **inside the workflow file**.

### 9.10 Cron, done properly
A wrapper script rather than a command in the crontab, because cron's environment is minimal. Inside:
`set -uo pipefail` **without `-e`** (so failures get logged rather than truncating the run), an explicit
`cd`, a readiness wait on a `/dev/tcp` probe for `@reboot` races, and scannable
`===== <iso ts> start/end (exit N) =====` markers.
→ `ME/scripts/cron_ingest.sh`

---

## 10. Testing

### 10.1 The isolation primitive comes first
- EP: `tmp_duckdb` monkeypatches `src.data.store.DB_PATH` to a `tmp_path` file — **no test can ever
  touch the production database** — and `seed_demand` inserts 400 hours of seeded sinusoidal data,
  enough for a 168 h lag.
- DE: a real DuckDB in `tmp_path` with the schema applied.
- ME: a socket probe + `pytest.mark.skipif` so the suite stays green without Docker.

### 10.2 Choose the mocking boundary deliberately, and say so
EP's API tests mock **only** the MLflow load and the model itself, so real feature generation runs
against the temp database. RL mocks the drift report to control the drift share, so the test is about
flag-writing logic. DE mocks the *network* (a scripted SSE transport, a fake Kafka producer that
acknowledges on poll/flush and can fail selected topics) and nothing else. Every one of these has a
docstring explaining the choice.

### 10.3 Drive time instead of waiting for it
Inject the clock (`FakeWall` callable), then advance the simulated clock and let the engine catch up
synchronously — no asyncio, no sleeping.
→ `DE/source/tests/simkit.py`

### 10.4 Test the wiring, not only the units
RL extracted four inline lines into a static method for one stated reason: *"those four lines are the
wiring, the wiring is what failed once already, and wiring you cannot call from a test is wiring you
are not testing."*

### 10.5 An end-to-end check that follows the data
DE's `smoke.py` checks the **running** stack in pipeline order and stops at the first broken hop:
source healthy → both topics receiving → a forced schema drift reaches the DLQ → all four Spark queries
commit and a forced teleport raises an alert → the DAGs run → the alert, the dead letter and both
faults are in `raw`, the trip is flagged, and the serving copy is republished with no empty mart.
It never starts or resets anything — *"a target that resets on its own would wipe your world by
accident"* — and its two forced faults go into the ground truth like any other, so the scorecard stays honest.

### 10.6 pytest mechanics for multi-package repos
`--import-mode=importlib` so several packages can have identically-named test files;
`pythonpath = [...]` to make each component importable; shared helpers as **uniquely-named plain
modules** (`simkit.py`, `bridgekit.py`) rather than conftest imports; `asyncio_mode = "auto"` with
session-scoped loops so an async engine's pool is reused.

---

## 11. Documentation

Six genres, each doing a job the others cannot. Steal the set.

| Genre | What it must contain | Example |
|---|---|---|
| **Plan of record** | the original request *verbatim*, the decisions taken with the user, the architecture, and the build order | `DE/PLAN.md` |
| **Build log** | written **during** the work: what was built, what was measured, every deviation with its reason, every bug at full length | `DE/logging_build.md`, `RL/NOTES.md` |
| **Guided tour** | a wiring map (service ↔ address ↔ config file), "I want to change X → edit Y, also keep Z in sync", a stop-by-stop reading order, and **experiments with measured outcomes** | `DE/TOUR.md` |
| **Mental models** | the transferable concepts, deliberately project-independent | `ME/dev/MENTAL-MODELS.md` |
| **Focused RFC** | one problem, the target architecture, why it works, a stepwise plan | `EP/STRATEGY.md` |
| **Coverage table** | every case × the mechanism that handles it × **the risk of missing it** | `RL/NOTES.md` §13 |

Conventions worth adopting wholesale:
- **Every config file starts with a header saying who reads it and what it wires.**
- **Every wiring point is commented on both ends.**
- **Every entry-point module's docstring** states its role in the flow, what is upstream and
  downstream, and how to run it alone.
- **Put the failure table in the docstring** of the module that handles the failures
  (`DE/ingest/.../bridge.py` lists: source unreachable · gap · reset source · no acknowledgement · SIGTERM).
- **Record honest numbers**, including the bad ones, with the date they were measured.

---

## 12. Fifteen things to remember

1. Keep the raw bytes. Type later. Schema drift should break a model, never an ingestion.
2. Every write should be safe to repeat: natural key + conflict clause, or a version-guarded upsert.
3. Offsets/positions belong **in the sink, in the same transaction as the rows**.
4. Name your clocks — event, emission, knowledge, landing, processing — and write down which one each
   column is on.
5. Incremental models filter on **landing** time, not event time, or late data is lost.
6. `tmp` + `os.replace` for anything a reader might open mid-write; cache by mtime on the reader side.
7. An embedded database has one writer: queue the writers, keep connections short, publish a copy for
   readers.
8. Syntax at ingestion, semantics downstream. If you cannot reject, repair **and record** — the repair
   is the evidence.
9. Bitemporal storage makes "what did we believe then" a query. Build it first if restatements exist.
10. Tombstone, never delete.
11. Pure `DataFrame -> DataFrame` (or pure function) rules run unchanged in production and in tests.
12. Shift your lag features. Split time-series temporally. Keep a baseline that is one column.
13. Absence of a label is not a negative label; features must be as-of the decision point.
14. An alert nobody can trust is worse than no alert: build on facts that cannot happen by chance,
    dedupe by fault, and retract with reasons when you were wrong.
15. Write the notes while you work, with the numbers and the mistakes in them.

---
---

# Part II — Navigation charts

---

## Chart 1 — `DE_pipeline_stream`

### Stack
`FastAPI` · `uvicorn` · `Pydantic v2` · `numpy` · `httpx` · `confluent-kafka` · `Apache Kafka 4.3 (KRaft)` ·
`Redpanda Console` · `PySpark 4.2 Structured Streaming` · `RocksDB state store` · `Apache Airflow 3.3
(LocalExecutor + Postgres)` · `DuckDB 1.5` · `pyarrow` · `dbt-core 1.12 / dbt-duckdb` · `harlequin` ·
`Streamlit + Altair` · `uv workspace` · `pytest` · `ruff` · `Docker Compose` · `GNU Make`

### Architecture
```
 ┌ layer 1 ────────────────────────────────────────────────────────────────┐
 │ source-api (:8000)   clock → engine → fault layer → event log           │
 │   SSE /v1/stream · REST /v1/{stations,bikes,weather} · /admin/*         │
 └──────┬───────────────────────────────────────┬──────────────────────────┘
        │ SSE (continuous)                      │ HTTP pulls (batch)
 ┌ layer 2 ──────────────────┐                  │
 │ bridge: contract → route  │                  │
 │   valid → 2 topics        │                  │
 │   invalid → dlq           │                  │
 └──────┬────────────────────┘                  │
        ▼                                       │
   Kafka (kafka:9092 internal / localhost:9094 external)
   trip-events.v1 · station-status.v1 · dlq.v1 · alerts.v1
        │                                       │
 ┌ layer 3 ──────────────┐            ┌ layer 4 ▼───────────────────────────┐
 │ Spark: 4 queries      │            │ Airflow 3                           │
 │  reference_rules      │──alerts──► │  stream_landing  (5 min) → raw_stream
 │  station_metrics ──► lake/*.parquet│  api_extract     (15 min) → raw_api  │
 │  station_health       │            │  dbt_transform   (on asset)          │
 │  trip_pairing         │            └──────────┬──────────────────────────┘
 └───────────────────────┘                       ▼
 ┌ layer 5 ─────────────────────────────────────────────────────────────────┐
 │ DuckDB  raw → staging(9 views) → intermediate(3) → marts(7)              │
 │   + snapshots(SCD2) + seeds + audit                                       │
 │   publish_serving → bikeshare_serving.duckdb  (atomic os.replace)        │
 └──────┬───────────────────────────────────────────────────────────────────┘
        ▼ layer 6
   Streamlit dashboard · harlequin · peek scripts
```

### Module map
| Path | Role |
|---|---|
| `config/source.toml` | the simulated world: seed, size, clock, emission cadence, **fault rates** |
| `source/bikeshare_sim/` | `clock` · `world` · `engine` · `faults` · `eventlog` · `api` · `schemas` · `config` · `simulation` |
| `ingest/bikeshare_bridge/` | `sse` · `contract` · `sink` · `checkpoint` · `bridge` · `logs` · `config` |
| `streaming/analyzer/` | `main` (4 queries) · `rules` (pure DF→DF) · `schemas` · `reference` · `progress` · `config` |
| `streaming/conf/` | `spark-defaults.conf` (master, memory, shuffle partitions, RocksDB) |
| `orchestration/dags/` | `stream_landing` · `api_extract` · `dbt_transform` |
| `orchestration/include/landing/` | `warehouse` (the only DuckDB opener) · `kafka_loader` · `api_extract` |
| `orchestration/include/serving/` | `publish` (the atomic serving copy) |
| `transform/` | `dbt_project.yml` · `profiles.yml` · `models/{sources,staging,intermediate,marts}` · `snapshots` · `seeds` · `tests` · `macros` |
| `dashboard/app.py` | Streamlit, four tabs, reads the serving copy only |
| `scripts/` | `smoke` · `peek_{alerts,lake,warehouse,serving}` · `forget_landed_topic` · `copy_warehouse_schema` |

### Entry points
`make help` · `make setup / up / down / reset` · `make smoke` ·
`make clock / speed / pause / ff / stream / stats / faults / fault / fault-log` ·
`make topics / tail / dlq / bridge-run / bridge-reset` ·
`make alerts / lake / spark-reset` · `make dags / runs / trigger / warehouse / sql` ·
`make dbt / dbt-docs / scorecard / quality / serving / dashboard`
UIs: source `:8000/docs` · Console `:8081` · Spark `:4040` · Airflow `:8080` · dbt docs `:8082` ·
dashboard `:8501`

### Reading path
1. `TOUR.md` §0–3 (the whole picture, the three clocks, the wiring map)
2. `config/source.toml` → `source/bikeshare_sim/simulation.py` → `api.py` → `engine.py`, `faults.py`
3. the `kafka` service in `docker-compose.yml` → `kafka/create-topics.sh` →
   `ingest/bikeshare_bridge/bridge.py` (its docstring failure table) → `contract.py` → `sink.py`
4. `streaming/analyzer/main.py` → `rules.py` → `conf/spark-defaults.conf`
5. `orchestration/include/landing/kafka_loader.py` (the exactly-once argument) → `stream_landing.py`
   → `x-airflow-common` in compose
6. `transform/dbt_project.yml` → `stg_trip_events.sql` → `snap_stations.yml` →
   `int_status_sequence.sql` → `mart_detection_scorecard.sql` → `dbt_transform.py` → `publish.py`
7. `dashboard/app.py`, then run `TOUR.md` §5's ten experiments

---

## Chart 2 — `mediaElection27`

### Stack
`Python 3.12` · `FastAPI` · `Mangum` · `SQLAlchemy 2.0 (async + sync)` · `asyncpg` / `psycopg` ·
`alembic` · `pydantic` / `pydantic-settings` · `httpx` · `feedparser` · `structlog` · `boto3` ·
`PostgreSQL 16 (RDS)` · `AWS CDK (Python)` · `Lambda` / `S3` / `EventBridge` / `API Gateway HTTP API` /
`CloudFront` / `Secrets Manager` / `CloudWatch` / `Budgets` ·
`Vite` · `React 19` · `TypeScript` · `TanStack Query` · `ECharts` · `Tailwind v4` · `Zod` ·
`react-router 7` · `GitHub Actions (OIDC)` · `ruff` · `mypy --strict` · `pytest` · `testcontainers`

### Architecture
```
EventBridge cron (0/6/12/18 UTC)
      ▼
ingest Lambda    ── NO VPC, free egress ──► fetch ~15 RSS feeds (async, semaphore-bounded)
      │  reads seeds/sources.yaml from its own zip (no DB)
      ▼ put_object  feeds/YYYY-MM-DD/{slug}.xml
S3 raw bucket  (SSE-S3, BPA, enforce_ssl, Glacier-IR@30d, expire@365d)
      ▼ S3 ObjectCreated
loader Lambda   ── IN VPC (PRIVATE_ISOLATED) ──
      │  parse → canonical_url + content_hash → INSERT..ON CONFLICT DO NOTHING
      │  ingest_runs audit row → keyword extractor inline
      │  also the admin entrypoint: {"action": "migrate" | "seed" | "extract"}
      ▼
RDS Postgres 16   sources · articles · candidates · candidate_aliases · mentions · ingest_runs
      ▲
API Lambda (Mangum → FastAPI) ── IN VPC ──  /healthz /version /stats /candidates /sources
      ▲                                      /timeseries /articles
API Gateway HTTP API  ($default, /{proxy+})
      ▲
CloudFront (OAC) ◄── private S3 bucket ◄── React SPA (VITE_API_BASE_URL baked at build)
```

### Module map
| Path | Role |
|---|---|
| `seeds/{sources,candidates}.yaml` | **the editorial config**: feeds, candidates, aliases, disambiguation rules |
| `app/config.py` | one `pydantic-settings` object (both DSNs, dirs, ingest tuning) |
| `app/db/` | `models.py` (ORM + constraints + indexes) · `engine.py` (dual async/sync) · `migrations/` |
| `app/ingest/` | `fetcher` · `parser` · `dedup` · `run` (local CLI) · `lambda_handler` (fetch) · `loader_handler` (persist + admin) |
| `app/extract/` | `base` (Protocol + dataclasses) · `keyword` (matcher v1) · `run` (idempotent driver) |
| `app/sources/seed.py` | YAML → DB UPSERT; aliases delete-all + reinsert |
| `app/api/` | `main` · `deps` · `mangum_handler` · `routers/{meta,candidates,sources,timeseries,articles}` |
| `app/models/` | Pydantic response models |
| `infra/cdk/` | `app.py` + `stacks/{network,runtime,data,ingest,api,frontend,observability}` |
| `frontend/src/` | `api/` (Zod + hooks) · `lib/{url-state,colors,lean}` · `components/` · `routes/` · `i18n.ts` |
| `scripts/` | `cron_ingest.sh` + `media27.cron` (the pre-AWS host deployment) |
| `dev/` | `PLAN-v1` · `MENTAL-MODELS` · two `CHECKPOINT-*` · `SPECIFICATION-REBUILD` · `cheatsheet_sql` · `schema_db` |

### Entry points
`make install / db-up / migrate / seed / ingest-once / extract / extract-all / api / test / lint /
typecheck` · `make frontend-install / frontend-dev / frontend-build` ·
`aws lambda invoke --payload '{"action":"migrate"|"seed"|"extract"}'` ·
`cdk synth` / `cdk deploy <Stack>` · push to `main` → `deploy.yml`

### Reading path
1. `README.md` (the no-NAT split) → `dev/MENTAL-MODELS.md` §1–2
2. `app/db/models.py` (the schema *is* the design) → the `content_hash` migration
3. `app/ingest/fetcher.py` → `parser.py` → `dedup.py` → `run.py`
4. `app/ingest/loader_handler.py` (cold-start hook, admin dispatch, inline extract)
5. `app/extract/base.py` → `keyword.py` → `tests/unit/test_keyword.py` (reads as a spec)
6. `app/api/routers/timeseries.py` (array binds, `AT TIME ZONE`, embedded registries)
7. `infra/cdk/stacks/network_stack.py` → `ingest_stack.py` → `api_stack.py` → `observability_stack.py`
8. `.github/workflows/deploy.yml` → `frontend/src/lib/url-state.ts` → `routes/Dashboard.tsx`
9. `dev/CHECKPOINT-2026-05-17.md` (six production crises, each with its lesson)

---

## Chart 3 — `energy_predML`

### Stack
`Python 3.11+` · `pandas` · `numpy` · `DuckDB` · `Parquet` · `requests` · `XGBoost` · `Optuna` ·
`scikit-learn` · `MLflow (SQLite backend + --serve-artifacts)` · `Evidently AI` · `Prefect` ·
`FastAPI` + `uvicorn` · `pydantic-settings` · `Streamlit` + `matplotlib` · `Docker Compose` ·
`pytest` · `GitHub Actions`

### Architecture
```
EIA Open Data API  /v2/electricity/rto/region-data/data/
   │ fetch_demand()  offset pagination, 5000 rows/request
   ▼
data/raw/{REGION}_{ts}.json      (raw, timestamped)
   │ validate_records()          (schema → typed, UTC, nulls dropped + logged)
   ▼
DuckDB data/energy.duckdb
   ├─ demand(timestamp, region, demand_mwh)                PK (timestamp, region)
   └─ predictions(timestamp, region, demand_mwh, predicted_at)
   │ build_features()  11 temporal + 10 lag/rolling/diff  (all shift(1))
   ▼
data/processed/features_v{ts}.parquet
   │ run_training()   temporal 80/20 split
   ▼
MLflow  experiment "energy_grid_forecast" + registry "energy_demand_xgboost"
   │   baseline(lag_24h) · xgboost_default · Optuna(TimeSeriesSplit) · xgboost_tuned → REGISTERED
   │   + data/train_cutoff.txt = max(demand.timestamp) at training time
   ▼
FastAPI  GET /health · POST /predict     ← loads max(version) at startup, + that run's rmse
   │   features built at request time from 14 d of history; every prediction logged to DuckDB
   ▼
Monitoring (Evidently)  reference = ≤ cutoff, current = > cutoff
   │   DataDriftPreset + RegressionPreset (predictions ⋈ actuals, ≥48 rows)
   │   drift_share > DRIFT_THRESHOLD → touch data/retrain_needed → run_training(use_tuning=False)
   ▼
Streamlit  Forecast · Historical Accuracy · Feature Importance · Drift Reports
```

### Module map
| Path | Role |
|---|---|
| `src/config.py` | path constants + `pydantic-settings` (`EIA_API_KEY`, `MLFLOW_TRACKING_URI`, `DEFAULT_REGION`, `DRIFT_THRESHOLD`) |
| `src/data/` | `fetch` (paginated client) · `validate` (schema → typed) · `store` (DuckDB, idempotent upsert, prediction log) |
| `src/features/` | `temporal` (11, incl. cyclical) · `lag` (10, all shifted) · `pipeline` (`NON_FEATURE_COLS`, versioned Parquet) |
| `src/models/` | `baseline` (lag-24h) · `xgboost_model` (train + Optuna/TimeSeriesSplit) · `evaluate` (RMSE/MAE/R²/MAPE) |
| `src/serving/` | `predict` (request-time features, `±1.96×RMSE`) · `api` (registry resolution, error mapping) |
| `src/monitoring/drift.py` | Evidently reports + drift-share extraction + HTML/JSON saving |
| `src/workflows/` | `ingest` (Prefect flow) · `train` (the training pipeline) · `monitor` (drift + auto-retrain) |
| `streamlit_app/app.py` | four tabs |
| `scripts/compare_models.py` | 1-step vs 24 h **recursive** forecast |
| `docker/` | four Dockerfiles + four requirements files |
| `tests/` | `conftest` (`tmp_duckdb`, `seed_demand`) + 25 unit + 24 integration |

### Entry points
`make setup` · `make local-mlflow / local-fetch / local-train / local-serve / local-dashboard /
local-monitor` · `make docker-fetch / docker-train / docker-serve / docker-monitor` · `make test` ·
`make clean`
UIs: MLflow `:5000` · API `:8000` (`/docs`) · Streamlit `:8501`

### Reading path
1. `README.md` "Key Design Decisions" → `CLAUDE.md` "Implementation Status"
2. `src/features/lag.py` + `tests/test_features/test_lag.py` (the leakage discipline)
3. `src/features/pipeline.py` (`NON_FEATURE_COLS`, versioned Parquet)
4. `src/workflows/train.py` (temporal split, baseline → default → Optuna → register, cutoff file)
5. `src/serving/predict.py` then `api.py` (request-time features; registry as deployment)
6. `src/workflows/monitor.py` (cutoff-based reference, matched predictions, flag file + lazy import)
7. `docker-compose.yml` + `Makefile` (the two-mode design) → `STRATEGY.md`
8. `scripts/compare_models.py` (what the served horizon actually is)

---

## Chart 4 — `refund-lab-autom`

### Stack
`Python 3.11+` · `requests` · `pandas` · `numpy` · `scikit-learn (HistGradientBoostingClassifier)` ·
`SQLite (WAL)` · `csv` / `gzip` / `zlib` / `difflib` / `hashlib` from the stdlib · `pytest` ·
plain `bash`.
**Deliberately absent:** DuckDB, dbt, Airflow/Prefect, Docker, any web framework.

### Architecture
```
refund-lab-source  (separate repo + process; a sealed ledger of ~11 faults from a public 30)
  ├── HTTP API   /v1/{orders,customers,events,labels} · /v1/health · /_lab/{clock,advance}
  │     paginated + cursored; filters valid time (since/until) AND knowledge time (as_of)
  │     hostile: 429 · 401 expiry · 503 windows · lying Content-Encoding · cursor anomalies
  │              · 200 bodies containing {"error"} · a total_count that may lie
  └── file drop  drop/payments/*.csv[.gz] · drop/catalog/*.csv   (no manifest, no checksum)

run.py / loop.sh  ── Pipeline.cycle()  ==  one simulated day
  1 advance the world in 3 steps, probing availability      (runner owns the clock)
  2 start the background drop-watch thread                  (overlap the 1.6 s dead time)
  3 API channel:  rolling re-pull → count reconciliation (bisected) → rotating deep audit
  4 drop channel: bytes-first decode → landing → bitemporal load
  5 conformed models over dirty partitions only
  6 data tests (partition-scoped; full every 50 cycles)
  7 MONITORS ──► incidents.jsonl (appended the moment a detector fires)
  8 retrain every 60 sim days / score new orders
  9 checkpoint state

landing/        raw bytes + raw JSONL.gz, never rewritten
warehouse/wh.db  src_* (append-only, bitemporal) │ cur_* (current belief) │ dim_/fct_/agg_ │ ops
incidents.jsonl  THE DELIVERABLE      retractions.jsonl  withdrawn claims, with reasons
```

### Module map
| Path | Role |
|---|---|
| `pipeline/config.py` | everything environmental; a deliberately narrow regex read of the vendor's `lab.toml`; `DOMAINS`, `RECON_BUCKETS`, `LOOKBACK`, cadences |
| `pipeline/util.py` | time parsing (tolerant, returns `had_offset`), hashing, JSON, logging, coercion, median/quantile |
| `pipeline/vendor.py` | **all transport hostility**: auth, backoff, encoding sniffing, cursor validation, `TransportStats`, unretried `probe()` |
| `pipeline/normalize.py` | schema-on-read: rename following, id re-padding, sentinels, mojibake, `NormStats` |
| `pipeline/extract_api.py` | landing, bitemporal load, version classification, day counts, `pull_window` |
| `pipeline/extract_drop.py` | `DropFile` (bytes-first decoding) + `DropIngestor` (scan, expect, ingest) |
| `pipeline/storage.py` | the warehouse schema + `Store` (`belief_as_of` is the bitemporal read) |
| `pipeline/transform.py` | `dim_customer` (SCD2) · `dim_product` · `fct_orders` · `fct_payments` · `agg_daily` · `conformance_rules` · `test()` |
| `pipeline/monitors.py` | **17 detectors** + `psi` / `auc` / `robust_baseline` / `near_dst` |
| `pipeline/incidents.py` | the scored log, with the dedupe policy |
| `pipeline/ml.py` | point-in-time featurisation, censoring, leakage screen, train/score |
| `pipeline/orchestrator.py` | the cycle, reconciliation bisection, tombstoning, audit, bootstrap, the run loop |
| `run.py` · `asof.py` · `score.py` · `evaluate.py` · `loop.sh` | CLI surfaces |
| `NOTES.md` | the real artefact: measurements, false positives, retractions, coverage, post-mortem |

### Entry points
`./loop.sh` · `python run.py bootstrap | cycle N | run | status | tests` ·
`python asof.py order|customer|day|diff [--as-of …]` ·
`python score.py <order> | --day D | --summary` · `python evaluate.py` · `python -m pytest tests/`

### Reading path
1. `NOTES.md` §1 (what the exercise is) and §2 (what was measured first)
2. `pipeline/storage.py` (the two layers) → `asof.py` (what they buy)
3. `pipeline/vendor.py` (hostility in one place)
4. `pipeline/normalize.py` (repair-but-record) → `pipeline/extract_drop.py` (bytes-first)
5. `pipeline/orchestrator.py::cycle` (the whole increment in one function) and `::_bisect`
6. `pipeline/monitors.py` — `run()`, then `m_domains`, `m_delivery`, `m_reconciliation`
7. `NOTES.md` §6 (confusable pairs), §7 (two false positives), §11 (the four retractions), §13 (coverage)
8. `pipeline/ml.py` (censoring + point-in-time) → `evaluate.py` (prospective evaluation)

---

## Appendix — one-line "where do I look for X"

| X | Project · file |
|---|---|
| SSE client / server | DE · `ingest/bikeshare_bridge/sse.py`, `source/bikeshare_sim/eventlog.py` |
| Kafka producer hygiene | DE · `ingest/bikeshare_bridge/sink.py` |
| Kafka consumer with offsets in the sink | DE · `orchestration/include/landing/kafka_loader.py` |
| Structured Streaming patterns | DE · `streaming/analyzer/main.py`, `rules.py` |
| Airflow assets / pools / task factories | DE · `orchestration/dags/*` |
| dbt layering, snapshots, incremental, tests | DE · `transform/**` |
| detection scorecard vs ground truth | DE · `transform/models/marts/mart_detection_scorecard.sql` |
| atomic serving copy + mtime caching | DE · `include/serving/publish.py`, `dashboard/app.py` |
| deterministic simulation with fault injection | DE · `source/bikeshare_sim/{engine,faults}.py` |
| async bounded fetching + tolerant parsing | ME · `app/ingest/{fetcher,parser}.py` |
| SQLAlchemy 2.0 async + FastAPI deps | ME · `app/db/`, `app/api/` |
| alias matching with disambiguation | ME · `app/extract/keyword.py` |
| CDK stacks, OIDC deploy, cost guardrails | ME · `infra/cdk/`, `.github/workflows/deploy.yml` |
| typed SPA over an analytics API | ME · `frontend/src/` |
| leakage-free time-series features | EP · `src/features/lag.py` |
| Optuna + TimeSeriesSplit + MLflow registry | EP · `src/models/xgboost_model.py`, `src/workflows/train.py` |
| drift → auto-retrain loop | EP · `src/workflows/monitor.py` |
| per-service images / two run modes | EP · `docker/`, `Makefile` |
| bitemporal store + as-of queries | RL · `pipeline/storage.py`, `asof.py` |
| hostile HTTP client | RL · `pipeline/vendor.py` |
| schema-on-read with repair-and-record | RL · `pipeline/normalize.py` |
| file-drop forensics | RL · `pipeline/extract_drop.py` |
| a detector suite and its discipline | RL · `pipeline/monitors.py` + `NOTES.md` |
| point-in-time ML with censoring | RL · `pipeline/ml.py`, `evaluate.py` |