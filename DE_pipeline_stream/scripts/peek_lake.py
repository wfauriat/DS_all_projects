"""Peek at the Parquet lake written by Spark, with DuckDB, from the host.

    make lake          (= uv run python scripts/peek_lake.py)

A preview of layer 5's wiring. DuckDB reads Spark's Parquet files in place:
no copy, no load step. hive_partitioning turns the date=… folders back into a
`date` column.

Spark's file sink records its committed files in _spark_metadata/. DuckDB's
glob doesn't read that log, which is fine while Spark runs healthily: a
crashed batch could leave an uncommitted file behind, and DuckDB would count it.
"""

import duckdb

LAKE = "data/lake/station_metrics"


def main() -> None:
    try:
        duckdb.sql(f"""
            CREATE VIEW windows AS
            SELECT * FROM read_parquet('{LAKE}/**/*.parquet', hive_partitioning = true)
        """)
    except duckdb.IOException:
        print("no Parquet files yet: Spark writes a window once the watermark has closed it")
        return

    print("per simulated day:")
    duckdb.sql("""
        SELECT date,
               count(*)                   AS windows,
               count(DISTINCT station_id) AS stations,
               sum(status_reports)        AS reports,
               sum(departures)            AS departures,
               sum(arrivals)              AS arrivals,
               max(window_end)            AS closed_up_to
        FROM windows GROUP BY date ORDER BY date
    """).show()

    print("latest windows:")
    duckdb.sql("""
        SELECT window_start, station_id, status_reports, min_bikes, max_bikes, departures, arrivals
        FROM windows ORDER BY window_start DESC, station_id LIMIT 8
    """).show()


if __name__ == "__main__":
    main()
