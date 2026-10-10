"""The source's REST API → DuckDB: the batch side of the vendor.

Kafka carries what HAPPENS (events). The API serves what IS (reference data)
and a few batch datasets. Each dataset needs a different extraction pattern:

    dataset     endpoint             pattern                     why
    stations    GET /v1/stations     full snapshot, replaced     small, and changes in place
                                                                 (dbt snapshots keep the history)
    bikes       GET /v1/bikes        full snapshot, replaced     same
    weather     GET /v1/weather      incremental, by time        append-only; ask for what is newer
                                                                 than the latest row we hold
    fault_log   GET /admin/fault-log incremental, by id, paged   append-only ground truth: ids 1, 2, 3…

Every extract is idempotent. Re-running a snapshot replaces it with the same
data, and incremental loads skip rows already held (primary key + ON CONFLICT
DO NOTHING). A failed and retried Airflow task therefore never duplicates anything.
"""

import json
import logging
from datetime import UTC, datetime, timedelta

import httpx
import pyarrow as pa

from . import warehouse

log = logging.getLogger(__name__)

FAULT_LOG_PAGE = 10_000  # the API's maximum page size


def _write(conn, table: str, rows: list[dict], columns: str, run_id: str, source: str,
           detail: dict, replace: bool) -> int:  # fmt: skip
    """One transaction: clear the table (snapshots) or skip known keys (incremental),
    insert the rows, write the audit row."""
    started_at = datetime.now(UTC)
    if rows:
        conn.register("incoming", pa.Table.from_pylist(rows))
    conn.execute("BEGIN TRANSACTION")
    try:
        batch_id = warehouse.next_batch_id(conn)
        if replace:
            conn.execute(f"DELETE FROM raw.{table}")
        before = conn.execute(f"SELECT count(*) FROM raw.{table}").fetchone()[0]
        if rows:
            # Incremental tables have a primary key: rows we already hold are skipped.
            skip_known = "" if replace else " ON CONFLICT DO NOTHING"
            conn.execute(
                f"INSERT INTO raw.{table} SELECT {columns}, ?, ? FROM incoming{skip_known}",
                [batch_id, started_at],
            )
        inserted = conn.execute(f"SELECT count(*) FROM raw.{table}").fetchone()[0] - before
        conn.execute(
            "INSERT INTO raw._ingest_batches VALUES (?, ?, ?, ?, ?, ?, ?)",
            [batch_id, run_id, source, inserted, json.dumps(detail, default=str), started_at,
             datetime.now(UTC)],
        )  # fmt: skip
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        if rows:
            conn.unregister("incoming")
    log.info("%s → raw.%s: %d rows (%s)", source, table, inserted, detail)
    return inserted


def extract_stations(conn, api: httpx.Client, run_id: str) -> int:
    rows = api.get("/v1/stations").raise_for_status().json()
    columns = (
        "station_id, name, lat, lon, capacity, zone, "
        "installed_at::TIMESTAMPTZ, updated_at::TIMESTAMPTZ"
    )
    return _write(conn, "stations", rows, columns, run_id, "api:/v1/stations",
                  {"snapshot_rows": len(rows)}, replace=True)  # fmt: skip


def extract_bikes(conn, api: httpx.Client, run_id: str) -> int:
    rows = api.get("/v1/bikes").raise_for_status().json()
    columns = "bike_id, bike_type, commissioned_at::TIMESTAMPTZ"
    return _write(conn, "bikes", rows, columns, run_id, "api:/v1/bikes",
                  {"snapshot_rows": len(rows)}, replace=True)  # fmt: skip


def extract_weather(conn, api: httpx.Client, run_id: str) -> int:
    """Incremental by time: everything observed after the latest hour we already hold."""
    latest = conn.execute("SELECT max(observed_at) FROM raw.weather").fetchone()[0]
    start = (latest + timedelta(hours=1)) if latest else datetime(2000, 1, 1, tzinfo=UTC)
    rows = api.get("/v1/weather", params={"start": start.isoformat()}).raise_for_status().json()
    columns = "observed_at::TIMESTAMPTZ, temperature_c, precipitation_mm, wind_kmh"
    return _write(conn, "weather", rows, columns, run_id, "api:/v1/weather",
                  {"since": start, "received": len(rows)}, replace=False)  # fmt: skip


def extract_fault_log(conn, api: httpx.Client, run_id: str) -> int:
    """Incremental by id, page by page, until the API has nothing newer."""
    after = conn.execute("SELECT coalesce(max(fault_id), 0) FROM raw.fault_log").fetchone()[0]
    rows, cursor = [], after
    while True:
        page = api.get("/admin/fault-log", params={"after_id": cursor, "limit": FAULT_LOG_PAGE})
        page = page.raise_for_status().json()
        rows += [{**f, "details": json.dumps(f["details"])} for f in page]
        if len(page) < FAULT_LOG_PAGE:
            break
        cursor = page[-1]["fault_id"]
    columns = "fault_id, fault_type, injected_at::TIMESTAMPTZ, event_id, event_type, details"
    return _write(conn, "fault_log", rows, columns, run_id, "api:/admin/fault-log",
                  {"after_id": after, "received": len(rows)}, replace=False)  # fmt: skip
