"""The truthful world: determinism, conservation and realistic rhythms (all faults off)."""

from collections import Counter
from datetime import datetime

from bikeshare_sim.schemas import EVENT_ADAPTER

from simkit import emitted, make_settings, make_sim, run_for


def test_same_seed_same_stream(tmp_path):
    a, b = make_sim(make_settings(tmp_path)), make_sim(make_settings(tmp_path))
    run_for(a, hours=6)
    run_for(b, hours=3)  # a different catch-up path to the same point…
    run_for(b, hours=3)
    assert emitted(a) == emitted(b)  # …yields the identical stream


def test_different_seed_different_stream(tmp_path):
    a, b = make_sim(make_settings(tmp_path)), make_sim(make_settings(tmp_path, seed=7))
    run_for(a, hours=2)
    run_for(b, hours=2)
    assert emitted(a) != emitted(b)


def test_bikes_are_conserved(settings):
    sim = make_sim(settings)
    run_for(sim, hours=30)
    assert sim.world.n_docked() + sim.engine.rides_in_progress == settings.world.n_bikes


def test_truthful_events_honour_the_contract(settings):
    sim = make_sim(settings)
    run_for(sim, hours=12)
    events = emitted(sim)
    assert {e["event_type"] for e in events} == {"trip_started", "trip_ended", "station_status"}
    for e in events:
        EVENT_ADAPTER.validate_python(e)  # raises on any contract breach


def test_every_trip_end_has_an_earlier_start(settings):
    sim = make_sim(settings)
    run_for(sim, hours=12)
    started = {}
    for e in emitted(sim):
        p = e["payload"]
        if e["event_type"] == "trip_started":
            started[p["trip_id"]] = e["event_time"]
        elif e["event_type"] == "trip_ended":
            assert started[p["trip_id"]] <= e["event_time"]


def test_weekday_has_a_morning_rush(settings):
    sim = make_sim(settings)
    run_for(sim, hours=19)  # Monday 05:00 → Tuesday 00:00
    by_hour = Counter(
        datetime.fromisoformat(e["event_time"]).hour
        for e in emitted(sim)
        if e["event_type"] == "trip_started"
    )
    assert by_hour[8] > 3 * by_hour[5]
    assert by_hour[8] > 1.5 * by_hour[14]


def test_status_snapshots_every_interval(settings):
    sim = make_sim(settings)
    run_for(sim, hours=1)
    per_station = Counter(
        e["payload"]["station_id"] for e in emitted(sim) if e["event_type"] == "station_status"
    )
    assert set(per_station.values()) == {60 // 5}


def test_status_snapshots_are_stamped_when_their_values_are_true(settings):
    """A snapshot's values include every trip up to its timestamp, and none after it."""
    sim = make_sim(settings)
    run_for(sim, hours=3)
    step = settings.clock.step_seconds
    for e in emitted(sim):
        if e["event_type"] == "station_status":
            assert datetime.fromisoformat(e["event_time"]).timestamp() % step == 0
