"""The bridge loop against a scripted source and a fake producer: resume, DLQ, gaps, failures."""

import json
import logging

import pytest

from bikeshare_bridge.bridge import Bridge
from bikeshare_bridge.checkpoint import Checkpoint
from bikeshare_bridge.config import Settings
from bikeshare_bridge.sink import DeliveryFailed, KafkaSink

from bridgekit import TOPICS, FakeProducer, FakeSource, sample, sse_body


def make_bridge(tmp_path, source: FakeSource, producer: FakeProducer | None = None, **overrides):
    defaults = {
        "source_url": "http://source",
        "state_dir": tmp_path,
        "checkpoint_every_s": 0,  # checkpoint after every frame: easy to observe
        "report_every_s": 1e9,
    }
    settings = Settings(**{**defaults, **overrides})
    producer = producer or FakeProducer()
    sink = KafkaSink("unused", producer_factory=lambda _config: producer)
    checkpoint = Checkpoint(tmp_path / "checkpoint.json")
    return Bridge(settings, sink, checkpoint, source.client()), producer, checkpoint


def test_first_start_replays_the_source_buffer_and_routes_by_contract(tmp_path):
    drifted = sample("trip_ended", seq=4, duration_s="754s")
    source = FakeSource([sse_body(sample("trip_started", 1), sample("station_status", 2),
                                  sample("trip_ended", 3), drifted)])  # fmt: skip
    bridge, producer, checkpoint = make_bridge(tmp_path, source)

    assert bridge.consume_once() == 4
    assert source.resume_headers == ["0"]  # "from the oldest buffered event"
    assert len(producer.on(TOPICS.trip_events)) == 2
    assert len(producer.on(TOPICS.station_status)) == 1
    [dead] = producer.on(TOPICS.dlq)
    assert json.loads(dead.value)["error_type"] == "contract_violation"
    assert checkpoint.load() == 4
    assert bridge.totals == {"trip_started": 1, "station_status": 1, "trip_ended": 1,
                             "dlq:contract_violation": 1}  # fmt: skip


def test_restart_resumes_right_after_the_checkpoint(tmp_path):
    Checkpoint(tmp_path / "checkpoint.json").save(7)
    source = FakeSource([sse_body(sample("trip_started", 8))], last_seq=8)
    bridge, _, checkpoint = make_bridge(tmp_path, source)
    bridge.consume_once()
    assert source.resume_headers == ["7"]
    assert checkpoint.load() == 8


def test_live_first_start_sends_no_resume_header(tmp_path):
    source = FakeSource([sse_body(sample("trip_started", 50))])
    bridge, _, _ = make_bridge(tmp_path, source, start_from="live")
    bridge.consume_once()
    assert source.resume_headers == [None]


def test_kafka_failure_never_moves_the_checkpoint(tmp_path):
    Checkpoint(tmp_path / "checkpoint.json").save(3)
    source = FakeSource([sse_body(sample("trip_started", 4))], last_seq=4)
    broken = FakeProducer(fail_topics={TOPICS.trip_events})
    bridge, _, checkpoint = make_bridge(tmp_path, source, broken)
    with pytest.raises(DeliveryFailed):
        bridge.consume_once()
    assert checkpoint.load() == 3  # run() then reconnects from 3 and re-sends


def test_gap_is_counted_and_the_stream_continues(tmp_path, caplog):
    gap = ("gap", {"requested_after_seq": 0, "oldest_available_seq": 100})
    source = FakeSource([sse_body(gap, sample("trip_started", 100), sample("trip_started", 101))])
    bridge, producer, _ = make_bridge(tmp_path, source)
    with caplog.at_level(logging.WARNING, logger="bridge"):
        bridge.consume_once()
    assert bridge.totals["lost_upstream"] == 99
    assert bridge.last_seq == 101 and len(producer.messages) == 2
    assert not any("seq jump" in r.message for r in caplog.records)


def test_a_reset_source_is_read_again_from_its_oldest_event(tmp_path):
    Checkpoint(tmp_path / "checkpoint.json").save(500)
    source = FakeSource([sse_body(sample("trip_started", 1))], last_seq=20)  # seq went backwards
    bridge, _, checkpoint = make_bridge(tmp_path, source)
    bridge.consume_once()
    assert source.resume_headers == ["0"]
    assert checkpoint.load() == 1


def test_close_flushes_and_saves_the_position(tmp_path):
    source = FakeSource([sse_body(sample("trip_started", 1), sample("trip_started", 2))])
    bridge, _, checkpoint = make_bridge(tmp_path, source, checkpoint_every_s=1e9)
    bridge.consume_once()
    assert checkpoint.load() is None  # not due yet...
    bridge.close()
    assert checkpoint.load() == 2  # ...but a clean stop always checkpoints


def test_missing_topics_are_reported_before_streaming():
    sink = KafkaSink("unused", producer_factory=lambda _config: FakeProducer())
    assert sink.check_topics(TOPICS.all()) == {t: 3 for t in TOPICS.all()}
    with pytest.raises(RuntimeError, match="missing Kafka topics"):
        sink.check_topics(["bikeshare.typo.v1"])
