"""Publish the marts to the serving copy: a second DuckDB file, made for readers.

The warehouse has one writer at a time, and every few minutes that writer is an
Airflow task. Anyone else opening the file can hit its lock. So readers get their
own file instead, holding only the marts, rebuilt after each successful dbt build:

    warehouse  bikeshare.duckdb          raw · staging · intermediate · snapshots · marts
                     │  publish()
                     ▼
    serving    bikeshare_serving.duckdb  marts + a `published` row saying when

Atomic: the copy is built under a temporary name, then renamed over the previous
one. A reader that has the old file open keeps reading it (on Linux the old
inode stays alive until closed); the next reader opens the new one. Nobody ever
sees a half-built file, and nobody ever waits.
"""

import json
import os
from datetime import UTC, datetime

from landing import warehouse


def publish(warehouse_path: str, serving_path: str, schemas: tuple[str, ...] = ("marts",)) -> dict:
    """Copy every table of `schemas` into a fresh serving file. Returns {table: rows}."""
    tmp = f"{serving_path}.tmp"
    for leftover in (tmp, f"{tmp}.wal"):  # from a publish that crashed half-way
        if os.path.exists(leftover):
            os.remove(leftover)

    conn = warehouse.connect(warehouse_path)
    copied: dict[str, int] = {}
    try:
        conn.execute(f"ATTACH '{tmp}' AS serving")
        for schema in schemas:
            conn.execute(f"CREATE SCHEMA serving.{schema}")
            tables = conn.execute(
                "SELECT table_name FROM duckdb_tables() "
                "WHERE database_name = current_database() AND schema_name = ? ORDER BY table_name",
                [schema],
            ).fetchall()
            for (table,) in tables:
                conn.execute(
                    f"CREATE TABLE serving.{schema}.{table} AS SELECT * FROM {schema}.{table}"
                )
                copied[f"{schema}.{table}"] = conn.execute(
                    f"SELECT count(*) FROM serving.{schema}.{table}"
                ).fetchone()[0]
        conn.execute(
            "CREATE TABLE serving.main.published AS SELECT ?::TIMESTAMPTZ AS published_at, ?::JSON AS tables",
            [datetime.now(UTC), json.dumps(copied)],
        )
        conn.execute("DETACH serving")  # flushes and closes the new file
    finally:
        conn.close()

    os.replace(tmp, serving_path)  # the atomic swap
    return copied
