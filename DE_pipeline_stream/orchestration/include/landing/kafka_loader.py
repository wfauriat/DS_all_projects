"""Kafka → DuckDB, in bounded batches, exactly once.

A stream never ends, but an Airflow task must. So each run lands a BOUNDED slice:

    1. start  per partition, the next offset stored IN DUCKDB (raw._kafka_offsets),
              or the earliest retained offset on the very first run
    2. end    per partition, the end offset as it is when the task starts: a
              snapshot, so messages that arrive during the run wait for the next one
    3. read   the slice [start, end)
    4. write  ONE DuckDB transaction: the rows + the new positions + an audit row
    5. show   only then, commit the same positions to Kafka as consumer group
              "duckdb-loader", so Redpanda Console can display the lag

Why that is exactly-once: the rows and the position move in one transaction. A
crash before step 4 commits leaves both untouched, and the next run re-reads the
same slice. A crash after leaves both advanced. The offsets committed to Kafka in
step 5 are never used to resume, only displayed. The primary key on (topic,
partition, offset) is a second line of defence.

Compare with the bridge (at-least-once, dedup downstream) and Spark (checkpoints):
three consumers of the same topics, three ways of tracking a position.
"""

import json
import logging
from dataclasses import asdict, dataclass
from datetime import UTC, datetime

import pyarrow as pa
from confluent_kafka import Consumer, KafkaError, TopicPartition

from . import warehouse

log = logging.getLogger(__name__)

CONSUMER_GROUP = "duckdb-loader"


def make_consumer(bootstrap_servers: str) -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": CONSUMER_GROUP,  # only used for the commit that feeds the lag view
            "enable.auto.commit": False,  # positions are committed explicitly, after DuckDB
            "enable.partition.eof": False,
        }
    )


@dataclass
class Slice:
    partition: int
    start: int  # first offset to read
    end: int  # first offset NOT to read (end offset snapshot, or capped)
    next_offset: int = -1  # where the next run starts; set after reading
    rows: int = 0
    note: str | None = None


# ── 1–2: plan ───────────────────────────────────────────────────────────────
def plan_slices(consumer, conn, topic: str, max_per_partition: int) -> list[Slice]:
    partitions = sorted(consumer.list_topics(topic, timeout=15).topics[topic].partitions)
    stored = dict(
        conn.execute(
            'SELECT "partition", next_offset FROM raw._kafka_offsets WHERE consumer = ? AND topic = ?',
            [CONSUMER_GROUP, topic],
        ).fetchall()
    )
    slices = []
    for p in partitions:
        low, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=15)
        start, note = stored.get(p, low), None
        if start < low:  # retention deleted messages before we landed them
            note = f"offsets {start}..{low - 1} expired before landing (data loss)"
            log.warning("%s[%d]: %s", topic, p, note)
            start = low
        slices.append(Slice(p, start, min(high, start + max_per_partition), start, note=note))
    return slices


# ── 3: read ─────────────────────────────────────────────────────────────────
def read_slices(consumer, topic: str, slices: list[Slice], timeout_s: float) -> list[dict]:
    todo = {s.partition: s for s in slices if s.start < s.end}
    if not todo:
        return []
    consumer.assign([TopicPartition(topic, s.partition, s.start) for s in todo.values()])
    rows: list[dict] = []
    deadline = datetime.now(UTC).timestamp() + timeout_s
    while todo and datetime.now(UTC).timestamp() < deadline:
        for msg in consumer.consume(num_messages=5000, timeout=1.0):
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(f"kafka error on {topic}: {msg.error()}")
            s = todo.get(msg.partition())
            if s is None or msg.offset() >= s.end:
                continue  # arrived after the snapshot: next run
            rows.append(_row(msg))
            s.next_offset, s.rows = msg.offset() + 1, s.rows + 1
            if s.next_offset >= s.end:
                del todo[msg.partition()]
    for s in todo.values():  # time budget exhausted: land what was read, the rest next run
        s.note = f"stopped at {s.next_offset} of {s.end} (read timeout)"
        log.warning("%s[%d]: %s", topic, s.partition, s.note)
    return rows


def _row(msg) -> dict:
    _, ts_ms = msg.timestamp()
    return {
        "topic": msg.topic(),
        "partition": msg.partition(),
        "offset": msg.offset(),
        "msg_key": msg.key().decode() if msg.key() is not None else None,
        "msg_ts": datetime.fromtimestamp(ts_ms / 1000, UTC) if ts_ms and ts_ms > 0 else None,
        "headers": json.dumps({k: v.decode() for k, v in msg.headers() or []}),
        "value": msg.value().decode(),
    }


# ── 4–5: write, then show ───────────────────────────────────────────────────
ROW_SCHEMA = pa.schema(
    [
        ("topic", pa.string()),
        ("partition", pa.int32()),
        ("offset", pa.int64()),
        ("msg_key", pa.string()),
        ("msg_ts", pa.timestamp("ms", tz="UTC")),
        ("headers", pa.string()),
        ("value", pa.string()),
    ]
)


def land_topic(
    conn,
    consumer,
    topic: str,
    run_id: str,
    max_per_partition: int = 200_000,
    read_timeout_s: float = 120.0,
) -> dict:
    """Land one bounded slice of `topic` into its raw table. Returns a summary (Airflow XCom)."""
    table = warehouse.TOPIC_TABLES[topic]
    started_at = datetime.now(UTC)
    slices = plan_slices(consumer, conn, topic, max_per_partition)
    rows = read_slices(consumer, topic, slices, read_timeout_s)
    moved = [s for s in slices if s.next_offset != s.start or s.note]

    if moved:
        # The rows go in as one Arrow table registered as a view: a single bulk insert.
        conn.register("incoming", pa.Table.from_pylist(rows, schema=ROW_SCHEMA))
        conn.execute("BEGIN TRANSACTION")
        try:
            batch_id = warehouse.next_batch_id(conn)
            now = datetime.now(UTC)
            conn.execute(
                f'INSERT INTO raw.{table} SELECT topic, "partition", "offset", msg_key, msg_ts, '
                "headers, value, ?, ? FROM incoming",
                [batch_id, now],
            )
            conn.executemany(
                "INSERT INTO raw._kafka_offsets VALUES (?, ?, ?, ?, ?) "
                '  ON CONFLICT (consumer, topic, "partition") '
                "  DO UPDATE SET next_offset = excluded.next_offset, updated_at = excluded.updated_at",
                [[CONSUMER_GROUP, topic, s.partition, s.next_offset, now] for s in moved],
            )
            conn.execute(
                "INSERT INTO raw._ingest_batches VALUES (?, ?, ?, ?, ?, ?, ?)",
                [batch_id, run_id, f"kafka:{topic}", len(rows),
                 json.dumps([asdict(s) for s in moved]), started_at, datetime.now(UTC)],
            )  # fmt: skip
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        finally:
            conn.unregister("incoming")
        # Step 5, strictly after COMMIT: purely for visibility (lag in the Console).
        consumer.commit(
            offsets=[TopicPartition(topic, s.partition, s.next_offset) for s in moved],
            asynchronous=False,
        )
    consumer.close()

    summary = {
        "topic": topic,
        "table": f"raw.{table}",
        "rows": len(rows),
        "partitions": {s.partition: [s.start, s.next_offset] for s in slices},
        "notes": [s.note for s in slices if s.note],
    }
    log.info("landed %s", summary)
    return summary


# ── reset: the topic itself was deleted and created again ───────────────────
def forget_topic(conn, topic: str) -> dict:
    """Forget a topic's landed rows and stored positions, together, in one transaction.

    Offsets number the messages of ONE topic. A topic deleted and created again
    (`make spark-reset` does this to the alerts topic) starts over at offset 0,
    while the positions stored here still point into the old one. The loader
    would then skip every new message below them, silently, and a new message at
    an offset already landed would collide with the old row's primary key.

    Forgetting the rows and the positions together makes the next run land the
    new topic from its earliest offset, like a first run. Only a reset may call
    this, and only once Kafka confirms the old topic is gone.
    """
    table = warehouse.TOPIC_TABLES[topic]
    conn.execute("BEGIN TRANSACTION")
    try:
        [rows] = conn.execute(f"DELETE FROM raw.{table} WHERE topic = ?", [topic]).fetchone()
        [positions] = conn.execute(
            "DELETE FROM raw._kafka_offsets WHERE consumer = ? AND topic = ?",
            [CONSUMER_GROUP, topic],
        ).fetchone()
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    log.info("forgot %s: %d rows, %d positions", topic, rows, positions)
    return {"topic": topic, "rows": rows, "positions": positions}
