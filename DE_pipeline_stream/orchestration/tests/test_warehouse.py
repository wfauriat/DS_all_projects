"""The single-writer rule of DuckDB, seen from connect()."""

import subprocess
import sys
import time

import duckdb
import pytest

from landing import warehouse


def test_the_raw_schema_is_idempotent(db):
    warehouse.ensure_raw_schema(db)  # a second time, as every DAG run does
    tables = {r[0] for r in db.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'raw'"
    ).fetchall()}  # fmt: skip
    assert {"trip_events", "_kafka_offsets", "_ingest_batches", "fault_log"} <= tables


def hold_write_lock(path: str, seconds: float) -> subprocess.Popen:
    """Another PROCESS opens the file read-write, like an Airflow task would."""
    code = f"import duckdb, time; c = duckdb.connect({path!r}); print('locked', flush=True); time.sleep({seconds})"
    holder = subprocess.Popen([sys.executable, "-c", code], stdout=subprocess.PIPE, text=True)
    assert holder.stdout.readline().strip() == "locked"
    return holder


def test_connect_waits_while_another_process_writes(db_path):
    duckdb.connect(db_path).close()
    holder = hold_write_lock(db_path, seconds=2)
    started = time.monotonic()
    conn = warehouse.connect(db_path, read_only=True, attempts=20, wait_s=0.25)
    assert time.monotonic() - started >= 1  # it had to wait...
    conn.close()  # ...and got in once the writer was done
    holder.wait()


def test_connect_gives_up_eventually(db_path):
    duckdb.connect(db_path).close()
    holder = hold_write_lock(db_path, seconds=5)
    with pytest.raises(duckdb.IOException, match="(?i)lock"):
        warehouse.connect(db_path, attempts=2, wait_s=0.1)
    holder.kill()
