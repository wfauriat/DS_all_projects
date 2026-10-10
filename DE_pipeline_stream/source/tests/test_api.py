"""The HTTP surface, through FastAPI's TestClient (the real app, with its real background task)."""

import asyncio
import time
from datetime import UTC, datetime

import pytest
from fastapi.testclient import TestClient

from bikeshare_sim.api import create_app, sse_frames
from bikeshare_sim.eventlog import EventLog
from bikeshare_sim.simulation import Simulation

from simkit import make_sim, run_for


@pytest.fixture
def client(settings):
    settings.clock.speed = 1  # the clock barely moves by itself; tests fast-forward explicitly
    with TestClient(create_app(settings)) as c:
        yield c


def wait_caught_up(client: TestClient, timeout: float = 10) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if client.get("/admin/clock").json()["backlog_s"] < 20:
            return
        time.sleep(0.05)
    raise AssertionError("the engine did not catch up with the clock")


def test_health_and_reference_data(client):
    assert client.get("/health").json()["status"] == "ok"
    stations = client.get("/v1/stations").json()
    assert len(stations) == 12
    assert {"station_id", "capacity", "lat", "lon", "zone", "updated_at"} <= stations[0].keys()
    assert len(client.get("/v1/bikes").json()) == 150


def test_fast_forward_then_replay(client):
    client.post("/admin/clock/advance", json={"hours": 2})
    wait_caught_up(client)
    page = client.get("/v1/events", params={"after_seq": 0, "limit": 50}).json()
    assert len(page["events"]) == 50
    assert page["next_after_seq"] == 50
    assert page["gap"] is None
    assert len(client.get("/v1/weather").json()) >= 2


def test_clock_controls(client):
    assert client.patch("/admin/clock", json={"paused": True}).json()["paused"] is True
    frozen = client.get("/admin/clock").json()["sim_time"]
    time.sleep(0.05)
    assert client.get("/admin/clock").json()["sim_time"] == frozen
    resumed = client.patch("/admin/clock", json={"paused": False, "speed": 600}).json()
    assert resumed["paused"] is False and resumed["speed"] == 600
    assert client.post("/admin/clock/advance", json={}).status_code == 422


def test_fault_admin_roundtrip(client):
    updated = client.patch("/admin/faults/duplicate", json={"enabled": True, "rate": 0.5}).json()
    assert updated["enabled"] is True and updated["rate"] == 0.5
    armed = client.post("/admin/faults/silent_station/trigger", json={"station_id": "ST-003"})
    assert armed.json()["armed"] == "silent_station"
    client.post("/admin/clock/advance", json={"minutes": 30})
    wait_caught_up(client)
    injected = {f["fault_type"] for f in client.get("/admin/fault-log").json()}
    assert {"duplicate", "silent_station"} <= injected
    assert client.patch("/admin/faults/nope", json={}).status_code == 404
    unknown_station = client.post(
        "/admin/faults/silent_station/trigger", json={"station_id": "ST-999"}
    )
    assert unknown_station.status_code == 404


def test_sse_frames_resume_and_report_gaps():
    """The SSE generator resumes after a seq, and says so when the buffer has moved on."""

    def event(i: int) -> dict:
        return {
            "event_id": f"id-{i}",
            "event_type": "trip_started",
            "schema_version": 1,
            "event_time": datetime(2026, 3, 2, tzinfo=UTC),
            "payload": {},
        }

    async def first_frames(n: int) -> list[str]:
        log = EventLog(maxlen=3)
        log.publish([event(i) for i in range(5)], emitted_at=datetime(2026, 3, 2, tzinfo=UTC))
        frames = sse_frames(log, after_seq=1, wanted=None)  # seq 2 already fell out of the buffer
        got = [await anext(frames) for _ in range(n)]
        await frames.aclose()
        return got

    frames = asyncio.run(first_frames(4))
    assert frames[0].startswith("event: gap") and '"oldest_available_seq": 3' in frames[0]
    assert [f.splitlines()[0] for f in frames[1:]] == ["id: 3", "id: 4", "id: 5"]


def test_state_survives_a_restart(settings):
    sim = make_sim(settings)
    run_for(sim, hours=3)
    sim.save()

    resumed = Simulation.load_or_create(settings)
    assert resumed.log.last_seq == sim.log.last_seq
    assert resumed.engine.cursor == sim.engine.cursor

    other_world = settings.model_copy(
        update={"world": settings.world.model_copy(update={"seed": 99})}
    )
    fresh = Simulation.load_or_create(other_world)
    assert fresh.log.last_seq == 0
    assert (settings.state_dir / "state.pkl.bak").exists()
