# DE_modern: a streaming data pipeline on a modern stack

A synthetic bike-share operator streams events over SSE. A pipeline built
with Kafka, Spark Structured Streaming, Airflow, DuckDB and dbt consumes that
stream, lands it, transforms it and checks it for faults.

- [`PLAN.md`](PLAN.md): the request, the architecture and the design decisions.
- [`logging_build.md`](logging_build.md): what is built so far and what comes next.

> **Start with [`TOUR.md`](TOUR.md)**, the guided tour. It covers the architecture, the three clocks, a wiring map
> (services, topics, delivery guarantees, where to change what), a stop-by-stop walkthrough and hands-on experiments.
> The sections below are the per-layer quickstarts.
>
> **Is everything working?** `make up`, then `make smoke` (~3 min): it forces two faults and follows them
> through every hop, from the source to the marts.

## Quickstart (layer 1: the source)

```bash
make setup      # .venv (uv), .env with your UID/GID, data/ dirs
make test       # unit tests
make up         # build + start the stack (for now: source-api) → http://localhost:8000/docs
make help       # every target
```

Play with the simulated world:

```bash
make clock                          # simulated time, speed, engine backlog
make stream                         # raw SSE frames (Ctrl-C to stop)
make stream types=trip_started      # only one event type
make speed x=600                    # 10 simulated minutes per real second
make ff h=6                         # fast-forward 6 h; the skipped events arrive as a burst
make faults                         # the fault catalogue, with live counters
make fault f=teleport               # force one fault now
make fault f=silent_station station=ST-007
make fault-log                      # ground truth: what was injected, when, where
make stats                          # emitted events, faults, engine internals
make source-reset                   # wipe the world (fresh start next time)
make down                           # stop the stack (data/ is kept)
```

The source can also run on the host with `make source-run`, using the same
config and the same state file. Don't run it at the same time as the container:
both use port 8000.

## Layer 2: Kafka and the bridge

`make up` now also starts Kafka (a single KRaft node), `kafka-init` (a one-shot
that creates the topics), the bridge (source SSE → Kafka) and Redpanda Console.

```
source-api ──SSE──► bridge ──► bikeshare.trip-events.v1     (key: bike_id)
                      │    ──► bikeshare.station-status.v1  (key: station_id)
                      └──────► bikeshare.dlq.v1             (contract violations, with the reason)
```

```bash
make ps                                   # 5 services; kafka-init shows "Exited (0)"
make topics                               # messages per topic
make tail t=bikeshare.trip-events.v1      # next 5 messages: partition, headers, key, value
make fault f=schema_drift && make dlq     # watch a contract violation land in the DLQ
make logs s=bridge                        # JSON logs; a "throughput" line every 30 s
open http://localhost:8081                # Redpanda Console: browse topics and messages
make reset                                # wipe ALL state (world, checkpoint, topics) together
```

Kafka answers on two addresses. Containers use `kafka:9092`; tools on your
machine use `localhost:9094` (e.g. `make bridge-run`). The kafka service in
`docker-compose.yml` explains why.

If your `.env` predates layer 2, the new variables (`KAFKA_HOST_PORT`,
`CONSOLE_PORT`) fall back to their defaults. Compare it with `.env.example`.

## Layer 3: the Spark analyzer

A Spark 4.2 Structured Streaming job (`streaming/`) runs four queries over
the Kafka topics:

| query | pattern | output |
|---|---|---|
| `reference_rules` | stateless `foreachBatch` + station data refreshed from the source API | `over_capacity`, `teleport`, `late_event` → `bikeshare.alerts.v1` |
| `station_metrics` | watermark + dedup + 30-min windows | Parquet in `data/lake/station_metrics/` (exactly-once file sink) |
| `station_health` | watermark + dedup + 2-hour windows | `frozen_station`, `silent_station` → alerts topic |
| `trip_pairing` | stream-stream full outer join with a time bound | `orphan_trip` → alerts topic |

```bash
make alerts                 # alert counts by type + the latest ones (a host Kafka consumer)
make lake                   # DuckDB reading Spark's Parquet in place, from the host
make logs s=spark           # one JSON "progress" line per micro-batch and query
open http://localhost:4040  # Spark UI → Structured Streaming tab
make spark-reset && make up # replay everything from Kafka (checkpoints, lake, alerts wiped; warehouse re-lands alerts)
COMPOSE_PROFILES= make up   # run the stack without Spark (~1.5 GB less RAM)
```

Measured against the source's ground truth (after injecting extra frozen and silent episodes):

| alert type | precision | recall |
|---|---|---|
| over_capacity | 100% | 100% |
| teleport | 100% | 98% |
| late_event | 100% | 100% |
| orphan_trip | 66% | 100% |
| frozen_station | 86% | 40% |
| silent_station | 100% | 100% |

The orphan "false positives" are not rule bugs. The missing half of those trips
was schema-drifted and dead-lettered by the bridge, so the pipeline really
never saw it: one fault showing up as another.

Frozen recall is limited by episodes too short, or at stations too quiet, to fill
a 2-hour window. The exact check is sequential and comes in layer 5, with dbt.

## Layer 4: Airflow and the DuckDB landing zone

Airflow 3 (`orchestration/`) schedules two DAGs that fill the warehouse's `raw`
schema, `data/warehouse/bikeshare.duckdb`:

| DAG | every | what | outlet asset |
|---|---|---|---|
| `stream_landing` | 5 min | the four Kafka topics → `raw.trip_events`, `raw.station_status`, `raw.dlq`, `raw.alerts` | `raw_stream` |
| `api_extract` | 15 min | the source API → `raw.stations`, `raw.bikes` (snapshots), `raw.weather`, `raw.fault_log` (incremental) | `raw_api` |

Landing is **exactly once**. Each run reads a bounded slice of each partition,
then writes the rows *and* the new Kafka position in one DuckDB transaction.
`orchestration/include/landing/kafka_loader.py` explains why that works. It was
checked by killing the scheduler mid-landing: no row lost, none duplicated.

```bash
make dags                    # the DAGs, paused state, import errors
make runs d=stream_landing   # latest runs
make trigger d=api_extract   # run a DAG now
make warehouse               # rows per raw table, loader positions vs Kafka, latest batches
make sql                     # harlequin on the warehouse, read-only
open http://localhost:8080   # Airflow UI (no login locally): graphs, logs, Assets, Pools
```

Wiring worth reading:

- **Connections and variable:** set as environment variables in the
  `x-airflow-common` block of `docker-compose.yml` (`AIRFLOW_CONN_*`,
  `AIRFLOW_VAR_*`). Nothing is clicked in the UI.
- **Pool `duckdb`:** one slot, because DuckDB allows a single writer. Every
  writing task queues there.
- **Assets:** `raw_stream` and `raw_api` carry row counts. Layer 5's dbt DAG
  will run on them instead of a clock.

The landing code is plain Python (`orchestration/include/landing/`), unit-tested
without Airflow; the DAGs only schedule it.

## Layer 5: dbt, the scorecard and the serving copy

dbt (`transform/`) turns `raw` into analytics tables inside the same DuckDB warehouse:

```
raw.* (Airflow) ─┐                         ┌─ dim_stations, fct_trips (incremental)
                 ├─► staging ─► intermediate ─┼─ mart_station_usage_hourly
lake (Spark) ────┘   9 views    trips, status ├─ mart_data_quality_daily
                                sequences,    ├─ mart_detection_scorecard   (vs the ground truth)
snap_stations (SCD2) ──────────  detections    ├─ mart_pipeline_health
seed detection_checks ─────────────────────────└─ mart_stream_vs_batch      (Spark vs batch)
```

The DAG `dbt_transform` has **no schedule**. It runs when `stream_landing` or
`api_extract` emits its asset, i.e. when new rows have landed:

1. `dbt source freshness`
2. `dbt build`: seeds, snapshot, models and tests, in order
3. a quality report in the task log
4. **publish**: the marts are copied into `data/warehouse/bikeshare_serving.duckdb`,
   swapped atomically. Readers query that copy, so the warehouse's single-writer
   lock never blocks them.

```bash
make scorecard      # precision/recall of the bridge, Spark and dbt checks vs the fault log
make quality        # data quality per simulated day
make serving        # what the serving copy holds, and when it was published
make sql            # harlequin on the serving copy
make dbt c="build -s staging"   # dbt on the host (same version; mind the warehouse lock)
make dbt-docs       # lineage graph raw → marts: http://localhost:8082 (built from a structure-only copy)
make dashboard      # Streamlit on the serving copy: http://localhost:8501 (TOUR.md, stop 6)
```

What dbt shows here, beyond models:

- **Snapshot (SCD2):** `snap_stations` keeps the history of station capacities, so
  "more bikes than docks" is judged against the capacity valid at the time.
- **Incremental model:** `fct_trips` only reprocesses trips whose latest half
  *landed* since the last run, so late events are still counted.
- **Seed:** `detection_checks.csv` maps each check to the fault it targets.
- **Tests:**
  - generic tests, plus a custom `within_range`;
  - two singular tests that are *expected* to find rows (severity `warn`, rows
    kept in schema `audit`);
  - a unit test of trip pairing;
  - an enforced contract on the scorecard.
- **Macros:** `haversine_km`, an empty-lake guard, and the schema naming rule.

The scorecard answers "who catches what". The bridge's contract check catches
negative bike counts; Spark and dbt catch over-capacity reports; together they
catch every impossible status. dbt's snapshot-by-snapshot check finds every frozen
station that Spark's 2-hour windows miss. Detections caused by a *different*
fault (an orphan trip whose other half was dead-lettered) are counted as
"explained", not as plain false positives.
