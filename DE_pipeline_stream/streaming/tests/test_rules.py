"""The detection rules on tiny static DataFrames, fed through parse_events like real Kafka rows."""

import json
from datetime import datetime, timedelta, timezone

from analyzer import rules

T0 = datetime(2026, 3, 2, 8, 0, tzinfo=timezone.utc)
# Two stations ~5 km apart: 0.0635° of longitude at 45°N ≈ 5.0 km.
STATIONS = [("ST-A", 20, 45.0, 5.0), ("ST-B", 20, 45.0, 5.0635), ("ST-C", 20, 45.0, 5.001)]


def iso(t: datetime) -> str:
    return t.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def ev(event_type: str, minute: float, event_id: str, late_min: float = 0, **payload) -> dict:
    t = T0 + timedelta(minutes=minute)
    return {
        "event_id": event_id,
        "event_type": event_type,
        "schema_version": 1,
        "seq": 1,
        "event_time": iso(t),
        "emitted_at": iso(t + timedelta(minutes=late_min, seconds=5)),
        "payload": payload,
    }


def status(station: str, minute: float, bikes: int, eid: str | None = None) -> dict:
    return ev("station_status", minute, eid or f"st-{station}-{minute}", station_id=station,
              bikes_available=bikes, ebikes_available=0, docks_available=20 - bikes,
              is_renting=True)  # fmt: skip


def start(trip: str, station: str, minute: float) -> dict:
    return ev("trip_started", minute, f"start-{trip}", trip_id=trip, bike_id="BK-1",
              bike_type="mechanical", station_id=station, rider_type="member")  # fmt: skip


def end(trip: str, station: str, from_station: str, minute: float, duration_s: int = 600) -> dict:
    return ev("trip_ended", minute, f"end-{trip}", trip_id=trip, bike_id="BK-1",
              bike_type="mechanical", station_id=station, start_station_id=from_station,
              duration_s=duration_s)  # fmt: skip


def parsed(spark, events: list[dict]):
    """Events as Kafka would deliver them (JSON bytes plus metadata), then parse_events."""
    rows = [(json.dumps(e).encode(), T0, "topic", 0, i) for i, e in enumerate(events)]
    kafka = spark.createDataFrame(
        rows, "value binary, timestamp timestamp, topic string, partition int, offset long"
    )
    return rules.parse_events(kafka)


def stations(spark):
    return spark.createDataFrame(
        STATIONS, "station_id string, capacity int, lat double, lon double"
    )


def test_parse_events_flattens_envelope_and_payload(spark):
    [row] = parsed(spark, [end("T1", "ST-B", "ST-A", 12, duration_s=720)]).collect()
    assert row.event_type == "trip_ended"
    # PySpark pitfall: collect() returns timestamps as naive datetimes in the Python
    # process's LOCAL timezone, whatever spark.sql.session.timeZone says. Compare instants.
    assert row.event_time.astimezone(timezone.utc) == T0 + timedelta(minutes=12)
    assert (row.trip_id, row.station_id, row.start_station_id, row.duration_s) == (
        "T1",
        "ST-B",
        "ST-A",
        720,
    )
    assert row.bikes_available is None  # not a field of trip_ended: null in the superset


def test_over_capacity_needs_the_reference_capacity(spark):
    events = parsed(spark, [status("ST-A", 0, 25, "too-many"), status("ST-A", 5, 12)])
    [alert] = rules.over_capacity(events, stations(spark)).collect()
    assert (alert.alert_type, alert.station_id, alert.event_id) == (
        "over_capacity",
        "ST-A",
        "too-many",
    )
    assert json.loads(alert.detail) == {"bikes_available": 25, "capacity": 20}


def test_teleport_flags_an_implausible_speed(spark):
    events = parsed(spark, [
        end("FAST", "ST-B", "ST-A", 5, duration_s=300),   # 5 km in 5 min: 60 km/h
        end("SLOW", "ST-B", "ST-A", 30, duration_s=1800),  # 5 km in 30 min: 10 km/h
    ])  # fmt: skip
    [alert] = rules.teleports(events, stations(spark)).collect()
    assert alert.entity_id == "FAST"
    assert 55 < json.loads(alert.detail)["kmh"] < 65


def test_late_events_compare_emission_and_event_time(spark):
    events = parsed(spark, [
        ev("trip_started", 0, "late", late_min=40, trip_id="T1", station_id="ST-A"),
        ev("trip_started", 1, "on-time", trip_id="T2", station_id="ST-A"),
    ])  # fmt: skip
    [alert] = rules.late_events(events, late_after_min=25).collect()
    assert alert.event_id == "late" and json.loads(alert.detail)["lateness_min"] >= 40


def test_station_windows_tell_frozen_and_silent_from_healthy(spark):
    events = []
    for minute in range(2, 60, 5):  # 12 reports per station per hour
        events.append(status("ST-A", minute, 10))  # ST-A: never changes...
        events.append(status("ST-C", minute, 10 + minute // 20))  # ST-C: changes
    events += [start(f"A{i}", "ST-A", 10 + i) for i in range(3)]  # ...though bikes leave it
    events += [start(f"C{i}", "ST-C", 10 + i) for i in range(3)]
    events += [end(f"B{i}", "ST-B", "ST-A", 20 + i) for i in range(3)]  # ST-B: trips, no reports

    windows = rules.station_windows(parsed(spark, events), "60 minutes")
    alerts = {a.station_id: a.alert_type for a in rules.window_alerts(windows, 12).collect()}
    assert alerts == {"ST-A": "frozen_station", "ST-B": "silent_station"}

    [c] = windows.where("station_id = 'ST-C'").collect()
    assert (c.status_reports, c.departures, c.min_bikes, c.max_bikes) == (12, 3, 10, 12)


def test_trip_pairing_reports_both_kinds_of_orphan(spark):
    events = parsed(spark, [
        start("OK", "ST-A", 0), end("OK", "ST-B", "ST-A", 15),
        start("NO_END", "ST-A", 1),
        end("NO_START", "ST-C", "ST-A", 20),
        start("TOO_LONG", "ST-A", 2), end("TOO_LONG", "ST-B", "ST-A", 2 + 5 * 60),  # beyond 3 h
    ])  # fmt: skip
    pairs = rules.trip_pairs(events, watermark="30 minutes", max_duration="3 hours")
    orphans = sorted(
        (a.entity_id, json.loads(a.detail)["missing"]) for a in rules.orphan_alerts(pairs).collect()
    )
    assert orphans == [
        ("NO_END", "trip_ended"),
        ("NO_START", "trip_started"),
        ("TOO_LONG", "trip_ended"),  # its end is too far away to count as a match...
        ("TOO_LONG", "trip_started"),  # ...so each half is an orphan
    ]


def test_alert_ids_are_deterministic_and_records_keep_nulls(spark):
    events = parsed(spark, [status("ST-A", 0, 25, "too-many")])
    first = rules.over_capacity(events, stations(spark)).collect()[0].alert_id
    again = rules.over_capacity(events, stations(spark)).collect()[0].alert_id
    assert first == again  # a replayed batch yields the same id: consumers can dedup

    [record] = rules.to_kafka_records(rules.over_capacity(events, stations(spark))).collect()
    value = json.loads(record.value)
    assert record.key == "ST-A"
    assert value["window_start"] is None  # explicit null, not a missing key
    assert value["event_time"] == "2026-03-02T08:00:00.000Z"
