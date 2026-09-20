"""Peek at the DuckDB warehouse from the host: what landed, and where the loader stands.

    make warehouse     (= PYTHONPATH=orchestration/include uv run python scripts/peek_warehouse.py)

It uses the same `landing` package as the Airflow tasks. The connection is
read-only and short-lived: DuckDB allows one writer, and every few minutes that
writer is an Airflow task. If one is writing right now, connect() waits.

The positions table shows the exactly-once bookkeeping at work. The loader's
next offset is stored IN DuckDB; Kafka's end offset is where the topic is now.
The difference is what the next stream_landing run will pick up.
"""

import os

from confluent_kafka import Consumer, TopicPartition

from landing import kafka_loader, warehouse

WAREHOUSE = "data/warehouse/bikeshare.duckdb"


def main() -> None:
    if not os.path.exists(WAREHOUSE):
        print(f"{WAREHOUSE} does not exist yet: the first stream_landing run creates it")
        return
    conn = warehouse.connect(WAREHOUSE, read_only=True)

    print("rows per raw table:")
    tables = [r[0] for r in conn.execute(
        "SELECT table_name FROM duckdb_tables() WHERE schema_name = 'raw' ORDER BY table_name"
    ).fetchall()]  # fmt: skip
    counts = " UNION ALL ".join(f"SELECT '{t}' AS tbl, count(*) AS n FROM raw.{t}" for t in tables)
    for table, n in conn.execute(counts).fetchall():
        print(f"  raw.{table:<16} {n:>9,}")

    print("\nloader positions (stored in DuckDB) vs Kafka end offsets:")
    kafka = Consumer({"bootstrap.servers": f"localhost:{os.environ.get('KAFKA_HOST_PORT', '9094')}",
                      "group.id": "peek-warehouse", "enable.auto.commit": False})  # fmt: skip
    stored = conn.execute(
        'SELECT topic, "partition", next_offset FROM raw._kafka_offsets WHERE consumer = ? '
        'ORDER BY topic, "partition"',
        [kafka_loader.CONSUMER_GROUP],
    ).fetchall()
    for topic, partition, next_offset in stored:
        _, end = kafka.get_watermark_offsets(TopicPartition(topic, partition), timeout=10)
        print(f"  {topic:<30} p{partition}  landed up to {next_offset:>7}  kafka end {end:>7}"
              f"  → {end - next_offset} waiting")  # fmt: skip
    kafka.close()

    print("\nlatest ingest batches:")
    for row in conn.execute("""
        SELECT batch_id, source, rows, finished_at::TIMESTAMP(0) AS finished, run_id
        FROM raw._ingest_batches ORDER BY batch_id DESC LIMIT 8
    """).fetchall():
        print("  " + "  ".join(str(v) for v in row))

    print("\na first look inside the JSON (dbt's job from layer 5): events by type")
    for event_type, n in conn.execute("""
        SELECT value->>'$.event_type' AS event_type, count(*)
        FROM raw.trip_events GROUP BY ALL ORDER BY 1
    """).fetchall():
        print(f"  {event_type:<16} {n:>9,}")
    conn.close()


if __name__ == "__main__":
    main()
