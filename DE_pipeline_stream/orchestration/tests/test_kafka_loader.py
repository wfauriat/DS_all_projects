"""Bounded, exactly-once landing, against a fake Kafka consumer and a real DuckDB file."""

import json
from types import SimpleNamespace

import pytest

from landing import kafka_loader

TOPIC = "bikeshare.trip-events.v1"


class FakeMessage:
    def __init__(self, partition: int, offset: int, value: bytes) -> None:
        self._p, self._o, self._v = partition, offset, value

    def error(self):
        return None

    def topic(self):
        return TOPIC

    def partition(self):
        return self._p

    def offset(self):
        return self._o

    def key(self):
        return b"BK-0001"

    def value(self):
        return self._v

    def headers(self):
        return [("event_type", b"trip_started")]

    def timestamp(self):
        return (1, 1_789_500_000_000)


class FakeConsumer:
    """Just enough of confluent_kafka.Consumer. Partitions hold messages from offset `low`."""

    def __init__(self, messages: dict[int, int], low: dict[int, int] | None = None) -> None:
        self.low = low or dict.fromkeys(messages, 0)
        self.log = {p: [f'{{"n": {i}}}'.encode() for i in range(n)] for p, n in messages.items()}
        self.positions: dict[int, int] = {}
        self.committed: dict[int, int] | None = None
        self.on_consume = None  # hook: simulate messages arriving mid-run

    def high(self, p: int) -> int:
        return self.low[p] + len(self.log[p])

    def list_topics(self, topic, timeout=None):
        return SimpleNamespace(topics={TOPIC: SimpleNamespace(partitions=dict.fromkeys(self.log))})

    def get_watermark_offsets(self, tp, timeout=None):
        return self.low[tp.partition], self.high(tp.partition)

    def assign(self, tps):
        self.positions = {tp.partition: tp.offset for tp in tps}

    def consume(self, num_messages, timeout):
        if self.on_consume:
            self.on_consume(self)
        out = []
        for p, pos in self.positions.items():
            while pos < self.high(p) and len(out) < num_messages:
                out.append(FakeMessage(p, pos, self.log[p][pos - self.low[p]]))
                pos += 1
            self.positions[p] = pos
        return out

    def commit(self, offsets, asynchronous):
        self.committed = {tp.partition: tp.offset for tp in offsets}

    def close(self):
        pass


def stored_offsets(db) -> dict[int, int]:
    return dict(db.execute('SELECT "partition", next_offset FROM raw._kafka_offsets').fetchall())


def row_count(db) -> int:
    return db.execute("SELECT count(*) FROM raw.trip_events").fetchone()[0]


def test_first_run_lands_everything_and_records_the_position(db):
    consumer = FakeConsumer({0: 3, 1: 2, 2: 0})
    summary = kafka_loader.land_topic(db, consumer, TOPIC, run_id="run-1")

    assert summary["rows"] == 5 and row_count(db) == 5
    assert stored_offsets(db) == {0: 3, 1: 2}  # partition 2: nothing to land, nothing stored
    assert consumer.committed == {0: 3, 1: 2}  # the same positions, for the lag view
    msg_key, headers, value = db.execute(
        "SELECT msg_key, headers, value FROM raw.trip_events ORDER BY \"partition\", \"offset\" LIMIT 1"
    ).fetchone()  # fmt: skip
    assert (msg_key, json.loads(headers), json.loads(value)) == (
        "BK-0001",
        {"event_type": "trip_started"},
        {"n": 0},
    )
    [(source, rows)] = db.execute("SELECT source, rows FROM raw._ingest_batches").fetchall()
    assert (source, rows) == (f"kafka:{TOPIC}", 5)


def test_next_run_lands_only_new_messages(db):
    kafka_loader.land_topic(db, FakeConsumer({0: 3}), TOPIC, run_id="run-1")
    summary = kafka_loader.land_topic(db, FakeConsumer({0: 5}), TOPIC, run_id="run-2")
    assert summary["partitions"] == {0: [3, 5]}
    assert row_count(db) == 5  # 3 + 2, never 3 + 5


def test_messages_arriving_during_a_run_wait_for_the_next(db):
    consumer = FakeConsumer({0: 3})

    def two_more_arrive(c):
        c.log[0] += [b'{"late": 1}', b'{"late": 2}']
        c.on_consume = None

    consumer.on_consume = two_more_arrive
    kafka_loader.land_topic(db, consumer, TOPIC, run_id="run-1")
    assert row_count(db) == 3 and stored_offsets(db) == {0: 3}  # bounded by the start snapshot


def test_a_failed_write_moves_neither_rows_nor_position(db):
    kafka_loader.land_topic(db, FakeConsumer({0: 2}), TOPIC, run_id="run-1")
    # Sabotage the LAST statement of the transaction: the audit row will collide
    # with the batch id the sequence hands out next.
    db.execute("INSERT INTO raw._ingest_batches VALUES (2, 'x', 'x', 0, NULL, now(), now())")
    consumer = FakeConsumer({0: 5})
    with pytest.raises(Exception, match="(?i)constraint"):
        kafka_loader.land_topic(db, consumer, TOPIC, run_id="run-2")

    assert row_count(db) == 2  # the 3 new rows were rolled back...
    assert stored_offsets(db) == {0: 2}  # ...together with their position
    assert consumer.committed is None  # and Kafka was never told otherwise


def test_expired_offsets_restart_at_the_earliest_retained(db):
    kafka_loader.land_topic(db, FakeConsumer({0: 2}), TOPIC, run_id="run-1")
    consumer = FakeConsumer({0: 3}, low={0: 5})  # retention deleted offsets 0..4
    summary = kafka_loader.land_topic(db, consumer, TOPIC, run_id="run-2")
    assert summary["partitions"] == {0: [5, 8]}
    assert "expired before landing" in summary["notes"][0]


def test_a_run_is_capped_per_partition(db):
    kafka_loader.land_topic(db, FakeConsumer({0: 10}), TOPIC, run_id="r1", max_per_partition=4)
    assert row_count(db) == 4 and stored_offsets(db) == {0: 4}


def test_a_recreated_topic_is_skipped_below_the_old_positions(db):
    """The bug `make spark-reset` used to cause: the loader trusts positions from the deleted topic."""
    kafka_loader.land_topic(db, FakeConsumer({0: 5}), TOPIC, run_id="run-1")
    recreated = FakeConsumer({0: 7})  # a new topic, offsets 0..6
    summary = kafka_loader.land_topic(db, recreated, TOPIC, run_id="run-2")
    assert summary["partitions"] == {0: [5, 7]}  # offsets 0..4 of the new topic: never landed


def test_forgetting_a_recreated_topic_relands_it_from_the_start(db):
    kafka_loader.land_topic(db, FakeConsumer({0: 5, 1: 2}), TOPIC, run_id="run-1")
    other = ["duckdb-loader", "bikeshare.dlq.v1", 0, 9, "2026-01-01 00:00:00+00"]
    db.execute("INSERT INTO raw._kafka_offsets VALUES (?, ?, ?, ?, ?)", other)

    forgotten = kafka_loader.forget_topic(db, TOPIC)

    assert forgotten == {"topic": TOPIC, "rows": 7, "positions": 2}
    assert row_count(db) == 0
    assert db.execute("SELECT topic, next_offset FROM raw._kafka_offsets").fetchall() == [
        ("bikeshare.dlq.v1", 9)  # other topics keep their positions
    ]
    summary = kafka_loader.land_topic(db, FakeConsumer({0: 3}), TOPIC, run_id="run-2")
    assert summary["partitions"] == {0: [0, 3]}  # from offset 0, and no primary-key collision
    assert row_count(db) == 3
    assert db.execute(
        'SELECT "partition", next_offset FROM raw._kafka_offsets WHERE topic = ?', [TOPIC]
    ).fetchall() == [(0, 3)]
