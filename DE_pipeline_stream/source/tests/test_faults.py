"""Each fault leaves its signature in the stream and an entry in the ground-truth log."""

from collections import Counter
from datetime import datetime, timedelta

import pytest
from pydantic import ValidationError

from bikeshare_sim.schemas import EVENT_ADAPTER

from simkit import emitted, make_settings, make_sim, run_for


def ts(value: str) -> datetime:
    return datetime.fromisoformat(value)


def sim_with_only(tmp_path, name: str, rate: float):
    settings = make_settings(tmp_path)  # every fault off...
    settings.faults[name] = settings.faults[name].model_copy(update={"enabled": True, "rate": rate})
    return make_sim(settings)  # ...except this one


def log_of(sim, name: str) -> list[dict]:
    return [f for f in sim.faults.log.entries if f["fault_type"] == name]


def drain(sim, name: str, hours: float) -> None:
    """Stop injecting, then run long enough for every held-back event to be released."""
    sim.faults.update(name, enabled=False)
    run_for(sim, hours=hours)


def test_faults_never_change_the_truthful_world(tmp_path):
    clean = make_sim(make_settings(tmp_path))
    faulty = make_sim(make_settings(tmp_path, faults_on=True))
    run_for(clean, hours=12)
    run_for(faulty, hours=12)
    assert faulty.faults.log.entries  # faults did happen...
    assert faulty.world.docked == clean.world.docked  # ...yet every bike is where it would be
    assert faulty.engine.counters == clean.engine.counters


def test_duplicate_reuses_the_event_id(tmp_path):
    sim = sim_with_only(tmp_path, "duplicate", rate=0.05)
    run_for(sim, hours=2)
    drain(sim, "duplicate", hours=0.2)  # copies trail by up to 5 minutes
    twice = {i for i, n in Counter(e["event_id"] for e in emitted(sim)).items() if n == 2}
    assert twice and twice == {f["event_id"] for f in log_of(sim, "duplicate")}


def test_late_event_is_emitted_much_later(tmp_path):
    sim = sim_with_only(tmp_path, "late_event", rate=0.05)
    run_for(sim, hours=2)
    drain(sim, "late_event", hours=3.5)
    late_ids = {f["event_id"] for f in log_of(sim, "late_event")}
    assert late_ids
    for e in emitted(sim):
        lag = ts(e["emitted_at"]) - ts(e["event_time"])
        if e["event_id"] in late_ids:
            assert lag >= timedelta(minutes=30)
        else:
            assert lag <= timedelta(seconds=10)  # one engine step


def test_schema_drift_breaks_the_contract(tmp_path):
    sim = sim_with_only(tmp_path, "schema_drift", rate=0.05)
    run_for(sim, hours=1)
    drifted = {f["event_id"] for f in log_of(sim, "schema_drift")}
    assert drifted
    for e in emitted(sim):
        if e["event_id"] in drifted:
            with pytest.raises(ValidationError):
                EVENT_ADAPTER.validate_python(e)
        else:
            EVENT_ADAPTER.validate_python(e)


def test_impossible_status_exceeds_capacity_or_goes_negative(tmp_path):
    sim = sim_with_only(tmp_path, "impossible_status", rate=0.1)
    run_for(sim, hours=1)
    capacity = {s.station_id: s.capacity for s in sim.world.stations}
    hit = {f["event_id"] for f in log_of(sim, "impossible_status")}
    assert hit
    for e in emitted(sim):
        if e["event_id"] in hit:
            bikes = e["payload"]["bikes_available"]
            assert bikes > capacity[e["payload"]["station_id"]] or bikes < 0


def test_teleport_trigger_forces_one_implausibly_fast_trip(settings):
    sim = make_sim(settings)  # every fault disabled: the trigger still fires, exactly once
    sim.faults.trigger("teleport")
    run_for(sim, hours=3)
    [fault] = log_of(sim, "teleport")
    [trip] = [e for e in emitted(sim) if e["event_id"] == fault["event_id"]]
    p, w = trip["payload"], sim.world
    km = w.distance_km[w.index[p["start_station_id"]], w.index[p["station_id"]]]
    assert km / (p["duration_s"] / 3600) > 45


def test_orphan_trip_drops_one_side(tmp_path):
    sim = sim_with_only(tmp_path, "orphan_trip", rate=0.05)
    run_for(sim, hours=3)
    seen = Counter(
        (e["payload"]["trip_id"], e["event_type"])
        for e in emitted(sim)
        if "trip_id" in e["payload"]
    )
    faults = log_of(sim, "orphan_trip")
    assert faults
    for f in faults:
        dropped = "trip_started" if f["details"]["dropped"] == "start" else "trip_ended"
        assert seen[(f["details"]["trip_id"], dropped)] == 0


def test_frozen_station_repeats_its_status(settings):
    sim = make_sim(settings)
    run_for(sim, hours=1)
    sim.faults.trigger("frozen_station", "ST-001")
    run_for(sim, hours=1)  # an episode lasts at least 1 hour
    [fault] = log_of(sim, "frozen_station")
    during = [
        e["payload"]
        for e in emitted(sim)
        if e["event_type"] == "station_status"
        and e["payload"]["station_id"] == "ST-001"
        and ts(e["event_time"]) >= fault["injected_at"]
    ]
    assert len(during) >= 10 and all(p == during[0] for p in during)


def test_silent_station_stops_reporting(settings):
    sim = make_sim(settings)
    run_for(sim, minutes=30)
    sim.faults.trigger("silent_station", "ST-002")
    run_for(sim, hours=1)
    [fault] = log_of(sim, "silent_station")
    reports = [
        e
        for e in emitted(sim)
        if e["event_type"] == "station_status"
        and e["payload"]["station_id"] == "ST-002"
        and ts(e["event_time"]) >= fault["injected_at"]
    ]
    assert reports == []


def test_stream_stall_holds_everything_then_flushes(settings):
    sim = make_sim(settings)
    run_for(sim, minutes=30)
    sim.faults.trigger("stream_stall")
    seq_before = sim.log.last_seq
    run_for(sim, minutes=10)  # a stall lasts at least 15 minutes
    assert sim.log.last_seq == seq_before
    run_for(sim, hours=1)
    [fault] = log_of(sim, "stream_stall")
    ends_at = fault["details"]["ends_at"]
    backlog = [e for e in emitted(sim) if fault["injected_at"] <= ts(e["event_time"]) < ends_at]
    assert backlog and all(ts(e["emitted_at"]) >= ends_at for e in backlog)


def test_back_to_back_stalls_lose_nothing(tmp_path):
    clean = make_sim(make_settings(tmp_path))
    stalled = make_sim(make_settings(tmp_path))
    stalled.faults.trigger("stream_stall")
    run_for(stalled, minutes=5)
    # The running stall ends at the next step, and a new one is armed for that same step.
    [episode] = stalled.faults._episodes
    episode.ends_at = stalled.engine.cursor
    stalled.faults.trigger("stream_stall")
    run_for(stalled, hours=3)  # both stalls are long over
    run_for(clean, hours=3, minutes=5)
    assert len(emitted(stalled)) == len(emitted(clean))  # same world, nothing lost on the way
