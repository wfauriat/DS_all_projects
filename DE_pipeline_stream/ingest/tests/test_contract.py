"""Routing by contract: valid events to their topic, everything else to the DLQ."""

import json

import pytest

from bikeshare_bridge.contract import route

from bridgekit import TOPICS, sample

NOW = "2026-09-15T20:00:00.000Z"


def routed(event: dict | str, seq: int = 1):
    data = event if isinstance(event, str) else json.dumps(event)
    return route(data, seq, TOPICS, ingested_at=NOW)


def test_trip_events_are_keyed_by_bike_and_kept_byte_for_byte():
    event = sample("trip_started", seq=42)
    data = json.dumps(event)
    msg = route(data, 42, TOPICS, ingested_at=NOW)
    assert msg.topic == TOPICS.trip_events
    assert msg.key == "BK-0001"
    assert msg.value == data.encode()
    assert dict(msg.headers) == {
        "event_type": "trip_started",
        "schema_version": "1",
        "source_seq": "42",
        "ingested_at": NOW,
    }


def test_station_status_is_keyed_by_station():
    msg = routed(sample("station_status"))
    assert (msg.topic, msg.key) == (TOPICS.station_status, "ST-001")


@pytest.mark.parametrize(
    ("event", "field"),
    [
        # the three schema_drift variants the source can emit
        ({**sample("station_status"), "payload": {"station_id": "ST-001", "num_bikes_available": 3,
          "ebikes_available": 1, "docks_available": 5, "is_renting": True}}, "bikes_available"),
        (sample("trip_ended", duration_s="754s"), "duration_s"),
        (sample("trip_started", bike_id=None), "bike_id"),
        # half of impossible_status: a negative count breaks the contract
        (sample("station_status", bikes_available=-2), "bikes_available"),
    ],
)  # fmt: skip
def test_contract_violations_go_to_the_dlq_with_the_reason(event, field):
    msg = routed(event, seq=9)
    record = json.loads(msg.value)
    assert msg.topic == TOPICS.dlq
    assert msg.key == event["event_id"]
    assert record["error_type"] == "contract_violation"
    assert any(field in e for e in record["errors"])
    assert json.loads(record["raw"]) == event  # replayable once fixed


def test_over_capacity_passes_the_contract():
    """Needs the station's capacity to be judged: a semantic check, done downstream."""
    assert routed(sample("station_status", bikes_available=99)).topic == TOPICS.station_status


def test_garbage_and_unknown_types_are_dead_letters_too():
    garbage = routed("{not json")
    assert (garbage.topic, garbage.key, garbage.error_type) == (TOPICS.dlq, None, "unparseable")
    unknown = routed({**sample("trip_started"), "event_type": "bike_stolen"})
    assert unknown.error_type == "unknown_event_type"


def test_contract_agrees_with_the_real_source(tmp_path):
    """Consumer-driven contract test: everything the source's truthful stream emits
    passes the bridge's contract, and every drifted event is rejected."""
    from simkit import make_settings, make_sim, run_for  # the source's own test helpers

    settings = make_settings(tmp_path)
    clean = make_sim(settings)
    run_for(clean, hours=6)
    records, _ = clean.log.read_after(0, 10**9)
    assert {route(data, seq, TOPICS, NOW).topic for seq, _, data in records} == {
        TOPICS.trip_events,
        TOPICS.station_status,
    }

    settings.faults["schema_drift"] = settings.faults["schema_drift"].model_copy(
        update={"enabled": True, "rate": 1.0}
    )
    drifting = make_sim(settings)
    run_for(drifting, hours=1)
    records, _ = drifting.log.read_after(0, 10**9)
    assert {route(data, seq, TOPICS, NOW).topic for seq, _, data in records} == {TOPICS.dlq}
