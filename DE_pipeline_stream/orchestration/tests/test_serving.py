"""The serving copy: only the marts, replaced atomically, readable while being replaced."""

import subprocess
import sys

import duckdb

from serving import publish


def make_warehouse(path: str, rows: int) -> None:
    conn = duckdb.connect(path)
    conn.execute("CREATE SCHEMA IF NOT EXISTS marts")
    conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
    conn.execute(
        "CREATE OR REPLACE TABLE marts.scorecard AS SELECT range AS n FROM range(?)", [rows]
    )
    conn.execute("CREATE OR REPLACE TABLE raw.big AS SELECT range AS n FROM range(1000)")
    conn.close()


def test_only_the_marts_are_published(tmp_path):
    warehouse, serving = str(tmp_path / "w.duckdb"), str(tmp_path / "s.duckdb")
    make_warehouse(warehouse, rows=3)
    assert publish.publish(warehouse, serving) == {"marts.scorecard": 3}

    reader = duckdb.connect(serving, read_only=True)
    schemas = {
        r[0] for r in reader.execute("SELECT DISTINCT schema_name FROM duckdb_tables()").fetchall()
    }
    assert schemas == {"marts", "main"}  # no raw, no staging
    assert reader.execute("SELECT count(*) FROM main.published").fetchone()[0] == 1


def rows_seen_by_a_new_process(path: str) -> int:
    """Readers are separate processes. (Within ONE process, DuckDB would reuse the
    database instance it already has open for that path: the old file.)"""
    code = f"import duckdb; print(duckdb.connect({path!r}, read_only=True).execute('SELECT count(*) FROM marts.scorecard').fetchone()[0])"
    return int(
        subprocess.run(
            [sys.executable, "-c", code], capture_output=True, text=True, check=True
        ).stdout
    )


def test_a_reader_is_never_disturbed_by_a_republish(tmp_path):
    warehouse, serving = str(tmp_path / "w.duckdb"), str(tmp_path / "s.duckdb")
    make_warehouse(warehouse, rows=3)
    publish.publish(warehouse, serving)
    early_reader = duckdb.connect(serving, read_only=True)  # holds the file open

    make_warehouse(warehouse, rows=5)
    publish.publish(warehouse, serving)  # replaces the file under the reader's feet

    assert (
        early_reader.execute("SELECT count(*) FROM marts.scorecard").fetchone()[0] == 3
    )  # its snapshot
    assert rows_seen_by_a_new_process(serving) == 5  # the next reader gets the new file
