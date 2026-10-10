"""The warehouse: one DuckDB file, and the one place that opens it and defines `raw`.

DuckDB is embedded: a library plus a file, with no server. Only ONE process at
a time may open the file read-write (an OS file lock). Everything here follows
from that:
  - every Airflow task that writes runs in the pool `duckdb` (1 slot), so tasks
    queue instead of colliding;
  - connections are short: open, write one transaction, close;
  - anyone else opening the file (you with harlequin, dbt from the host) can hit
    the lock while a task writes, so connect() retries for a while.

The `raw` schema is the landing zone (ELT). Data is stored as it arrived,
parsed as little as possible, so landing never breaks when a payload changes.
Parsing and typing are dbt's job (staging models, layer 5).
"""

import time

import duckdb

# Kafka topic → raw table. All four tables share one shape (see _topic_table).
TOPIC_TABLES = {
    "bikeshare.trip-events.v1": "trip_events",
    "bikeshare.station-status.v1": "station_status",
    "bikeshare.dlq.v1": "dlq",
    "bikeshare.alerts.v1": "alerts",
}


def connect(path: str, read_only: bool = False, attempts: int = 30, wait_s: float = 2.0):
    """Open the warehouse, waiting while another process holds the write lock.

    The session time zone is set to UTC. Otherwise DuckDB hands TIMESTAMPTZ
    values to Python in the machine's local zone (+01:00 on a Paris host, +00:00
    in a container): the same instants, but confusing in logs and API cursors.
    """
    for attempt in range(1, attempts + 1):
        try:
            conn = duckdb.connect(path, read_only=read_only)
            conn.execute("SET TimeZone = 'UTC'")
            return conn
        except duckdb.IOException as exc:
            if "lock" not in str(exc).lower() or attempt == attempts:
                raise
            time.sleep(wait_s)
    raise AssertionError("unreachable")


def _topic_table(name: str) -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS raw.{name} (
        -- where the message came from: (topic, partition, offset) identifies it uniquely
        topic       VARCHAR     NOT NULL,
        "partition" INTEGER     NOT NULL,
        "offset"    BIGINT      NOT NULL,
        msg_key     VARCHAR,
        msg_ts      TIMESTAMPTZ,            -- Kafka record timestamp: when the producer sent it
        headers     VARCHAR,                -- JSON object text
        -- the message itself, as JSON text. Kept as text, not the JSON type, so that
        -- a malformed message can never block landing. dbt parses it.
        value       VARCHAR     NOT NULL,
        batch_id    BIGINT      NOT NULL,   -- → raw._ingest_batches
        loaded_at   TIMESTAMPTZ NOT NULL,   -- wall clock, when this row was landed
        -- A message can land only once. The offsets stored with the data already
        -- guarantee that; the key is the second line of defence.
        PRIMARY KEY (topic, "partition", "offset")
    );"""


RAW_SCHEMA = [
    "CREATE SCHEMA IF NOT EXISTS raw;",
    *(_topic_table(name) for name in TOPIC_TABLES.values()),
    """
    -- The loader's position in each partition: the SOURCE OF TRUTH for where to resume.
    -- Written in the same transaction as the rows it covers.
    CREATE TABLE IF NOT EXISTS raw._kafka_offsets (
        consumer    VARCHAR     NOT NULL,
        topic       VARCHAR     NOT NULL,
        "partition" INTEGER     NOT NULL,
        next_offset BIGINT      NOT NULL,   -- the first offset NOT landed yet
        updated_at  TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (consumer, topic, "partition")
    );""",
    "CREATE SEQUENCE IF NOT EXISTS raw._batch_ids;",
    """
    -- One row per landing batch (a topic or an API endpoint, per task run): what was
    -- consumed, from where to where. The pipeline's own log, queryable in SQL.
    CREATE TABLE IF NOT EXISTS raw._ingest_batches (
        batch_id    BIGINT      PRIMARY KEY,
        run_id      VARCHAR,                -- the Airflow DAG run
        source      VARCHAR     NOT NULL,   -- kafka:<topic> or api:<path>
        rows        BIGINT      NOT NULL,
        detail      VARCHAR,                -- JSON: partition ranges, cursors, notes
        started_at  TIMESTAMPTZ NOT NULL,
        finished_at TIMESTAMPTZ NOT NULL
    );""",
    # The batch side of the source (api_extract.py). Typed columns: these payloads are small and stable.
    """
    CREATE TABLE IF NOT EXISTS raw.stations (   -- current snapshot, replaced at every extract
        station_id VARCHAR, name VARCHAR, lat DOUBLE, lon DOUBLE, capacity INTEGER, zone VARCHAR,
        installed_at TIMESTAMPTZ, updated_at TIMESTAMPTZ,
        batch_id BIGINT NOT NULL, extracted_at TIMESTAMPTZ NOT NULL
    );""",
    """
    CREATE TABLE IF NOT EXISTS raw.bikes (      -- current snapshot, replaced at every extract
        bike_id VARCHAR, bike_type VARCHAR, commissioned_at TIMESTAMPTZ,
        batch_id BIGINT NOT NULL, extracted_at TIMESTAMPTZ NOT NULL
    );""",
    """
    CREATE TABLE IF NOT EXISTS raw.weather (    -- append-only, incremental by observed_at
        observed_at TIMESTAMPTZ PRIMARY KEY, temperature_c DOUBLE, precipitation_mm DOUBLE,
        wind_kmh DOUBLE,
        batch_id BIGINT NOT NULL, extracted_at TIMESTAMPTZ NOT NULL
    );""",
    """
    CREATE TABLE IF NOT EXISTS raw.fault_log (  -- append-only, incremental by fault_id: the GROUND TRUTH
        fault_id BIGINT PRIMARY KEY, fault_type VARCHAR, injected_at TIMESTAMPTZ,
        event_id VARCHAR, event_type VARCHAR, details VARCHAR,  -- details: JSON text
        batch_id BIGINT NOT NULL, extracted_at TIMESTAMPTZ NOT NULL
    );""",
]


def ensure_raw_schema(conn) -> None:
    """Idempotent: safe to run at the start of every DAG run."""
    for statement in RAW_SCHEMA:
        conn.execute(statement)


def next_batch_id(conn) -> int:
    return conn.execute("SELECT nextval('raw._batch_ids')").fetchone()[0]
