"""Copy the warehouse's structure (schemas, tables, views, no rows) to a private file.

    make dbt-docs     (runs it first: PYTHONPATH=orchestration/include uv run python
                       scripts/copy_warehouse_schema.py transform/target-docs/bikeshare.duckdb)

`dbt docs generate` builds its catalog from a database: every table and view,
with its columns. It needs no rows. Run on the live warehouse, dbt opens the file
read-write for several seconds, so it fails whenever an Airflow task holds
DuckDB's single lock (a landing or a dbt build, about a minute out of every five),
and a dbt task in Airflow fails if it starts while dbt on the host holds the lock.

So dbt reads this copy instead. Taking it is one read-only open that waits for
the lock (landing.warehouse.connect, as Airflow's tasks do), and a copy of the
DDL, well under a second.

The file must be named bikeshare.duckdb: dbt-duckdb names the database after
the file, and the docs show that name in every relation ("bikeshare"."marts"."fct_trips").
"""

import sys
from pathlib import Path

from landing import warehouse

WAREHOUSE = Path("data/warehouse/bikeshare.duckdb")


def main() -> None:
    dest = Path(sys.argv[1])
    if dest.name != WAREHOUSE.name:
        sys.exit(f"the copy must be named {WAREHOUSE.name} (the docs show it), got {dest}")
    if not WAREHOUSE.exists():
        sys.exit(f"{WAREHOUSE} does not exist yet: the first stream_landing run creates it")

    # COPY FROM DATABASE refuses to overwrite existing objects: start from an empty file.
    dest.parent.mkdir(parents=True, exist_ok=True)
    for stale in (dest, dest.with_name(dest.name + ".wal")):
        stale.unlink(missing_ok=True)

    conn = warehouse.connect(str(WAREHOUSE), read_only=True)
    try:
        # READ_WRITE explicitly: an ATTACH inherits the read-only mode of the session otherwise.
        conn.execute(f"ATTACH '{dest}' AS structure_copy (READ_WRITE)")
        # (SCHEMA): the DDL only. The session's own database is named after its file: bikeshare.
        conn.execute("COPY FROM DATABASE bikeshare TO structure_copy (SCHEMA)")
        tables, views = conn.execute("""
            SELECT (SELECT count(*) FROM duckdb_tables() WHERE database_name = 'structure_copy'),
                   (SELECT count(*) FROM duckdb_views() WHERE database_name = 'structure_copy' AND NOT internal)
        """).fetchone()  # fmt: skip
    finally:
        conn.close()
    print(f"copied the warehouse's structure to {dest}: {tables} tables, {views} views, no rows")


if __name__ == "__main__":
    main()
