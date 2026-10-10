"""FastAPI app: the HTTP face of the simulation, and the entry point for every consumer.

    GET  /v1/stream              SSE: continuous events               ← bridge (layer 2)
    GET  /v1/events              JSON replay page by seq              ← debugging, backfills
    GET  /v1/stations, /v1/bikes reference data (current snapshot)    ← Airflow api_extract (layer 4)
    GET  /v1/weather             hourly observations (batch pull)     ← Airflow api_extract
    /admin/clock, /admin/faults, /admin/fault-log, /admin/stats       ← you (Makefile), smoke tests
    GET  /health                                                      ← Docker healthcheck

Concurrency model: one process, one asyncio event loop. The simulation runs as a
background task on that loop, and every route is `async def`. A route and a
simulation step therefore never run at the same time, and no locks are needed.
(A plain `def` route would run in a thread pool and race with the simulation.)

Run with `python -m bikeshare_sim` (see __main__.py) or `make source-run`.
"""

import asyncio
import json
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from datetime import datetime, timedelta
from typing import Annotated

from fastapi import APIRouter, Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import JSONResponse, StreamingResponse

from .config import Settings, load_settings
from .eventlog import EventLog
from .faults import FAULTS
from .schemas import (
    AdvanceRequest,
    BikeOut,
    ClockState,
    ClockUpdate,
    EventsPage,
    FaultTrigger,
    FaultUpdate,
    Health,
    StationOut,
    WeatherObservation,
)
from .simulation import Simulation

log = logging.getLogger("bikeshare_sim")

KEEPALIVE_S = 15.0  # an SSE comment line when idle, so proxies and clients keep the connection open


def create_app(settings: Settings | None = None) -> FastAPI:
    """App factory: __main__.py calls it with no argument (settings from env/TOML); tests pass settings."""
    settings = settings or load_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        sim = Simulation.load_or_create(settings)
        app.state.sim = sim
        # Kept for close_streams(), which runs in a signal handler, outside any coroutine.
        app.state.loop = asyncio.get_running_loop()
        app.state.runner = asyncio.create_task(sim.run(), name="simulation")
        app.state.runner.add_done_callback(_log_crash)
        log.info("simulation started: %s", sim.summary_line())
        try:
            yield
        finally:
            app.state.runner.cancel()
            with suppress(asyncio.CancelledError):
                await app.state.runner
            sim.save()
            log.info("state saved to %s", sim.state_path)

    app = FastAPI(
        title="bikeshare-sim",
        summary="Synthetic bike-share operator: SSE event stream, reference data, simulation controls.",
        version="1.0.0",
        lifespan=lifespan,
    )
    app.include_router(public)
    app.include_router(admin)
    return app


def _log_crash(task: asyncio.Task) -> None:
    if not task.cancelled() and task.exception():
        log.error("simulation task crashed", exc_info=task.exception())


def close_streams(app: FastAPI) -> None:
    """Called on SIGTERM/SIGINT (see __main__.py), before uvicorn waits for open
    connections. It ends the SSE streams so that shutdown, and the final save,
    complete cleanly."""
    sim, loop = getattr(app.state, "sim", None), getattr(app.state, "loop", None)
    if sim and loop:
        loop.call_soon_threadsafe(sim.log.close)


def get_sim(request: Request) -> Simulation:
    return request.app.state.sim


Sim = Annotated[Simulation, Depends(get_sim)]
public = APIRouter(tags=["consumers"])
admin = APIRouter(prefix="/admin", tags=["admin"])


# ── consumers ───────────────────────────────────────────────────────────────
@public.get("/health", response_model=Health)
async def health(sim: Sim, request: Request):
    running = not request.app.state.runner.done()
    body = Health(
        status="ok" if running else "degraded",
        simulation_running=running,
        sim_time=sim.clock.now(),
        last_seq=sim.log.last_seq,
    )
    return JSONResponse(body.model_dump(mode="json"), status_code=200 if running else 503)


@public.get("/v1/stream", response_class=StreamingResponse)
async def stream(
    sim: Sim,
    types: Annotated[
        str | None, Query(description="comma-separated event types, e.g. trip_started,trip_ended")
    ] = None,
    after_seq: Annotated[
        int | None, Query(description="resume after this seq (0 = from the oldest buffered)")
    ] = None,
    last_event_id: Annotated[str | None, Header(description="standard SSE resume header")] = None,
):
    """Server-Sent Events. Each frame is `id: <seq>`, `event: <event_type>`, `data: <json>`.

    Without after_seq or Last-Event-ID the stream starts live, from now.
    A `gap` event is sent if the resume point has already left the buffer.
    """
    if after_seq is None:
        after_seq = int(last_event_id) if last_event_id else sim.log.last_seq
    wanted = set(types.split(",")) if types else None
    return StreamingResponse(
        sse_frames(sim.log, after_seq, wanted),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


async def sse_frames(log_: EventLog, after_seq: int, wanted: set[str] | None) -> AsyncIterator[str]:
    """The SSE wire format, written out by hand so that it stays readable."""
    last = after_seq
    while not log_.closed:  # closed on server shutdown: end the response cleanly
        records, gap = log_.read_after(last, limit=500)
        if gap:
            yield f"event: gap\ndata: {json.dumps(gap)}\n\n"
        for seq, event_type, data in records:
            last = seq
            if wanted is None or event_type in wanted:
                yield f"id: {seq}\nevent: {event_type}\ndata: {data}\n\n"
        if not records and not await log_.wait_for_new(KEEPALIVE_S):
            yield ": keepalive\n\n"


@public.get("/v1/events", response_model=EventsPage)
async def events(
    sim: Sim,
    after_seq: int = 0,
    limit: Annotated[int, Query(ge=1, le=10_000)] = 1000,
    types: str | None = None,
):
    """Replay: the buffered events with seq > after_seq, as one JSON page."""
    records, gap = sim.log.read_after(after_seq, limit)
    wanted = set(types.split(",")) if types else None
    return {
        "events": [json.loads(d) for _, t, d in records if wanted is None or t in wanted],
        "next_after_seq": records[-1][0] if records else max(after_seq, sim.log.first_seq - 1),
        "gap": gap,
    }


@public.get("/v1/stations", response_model=list[StationOut])
async def stations(sim: Sim):
    """Reference data: the current state of every station (capacity changes over time)."""
    return [vars(s) for s in sim.world.stations]


@public.get("/v1/bikes", response_model=list[BikeOut])
async def bikes(sim: Sim):
    return [vars(b) for b in sim.world.bikes.values()]


@public.get("/v1/weather", response_model=list[WeatherObservation])
async def weather(sim: Sim, start: datetime | None = None, end: datetime | None = None):
    """Hourly observations in [start, end). Default: the last 24 simulated hours."""
    end = end or sim.engine.cursor
    start = start or end - timedelta(hours=24)
    return [w for w in sim.engine.weather if start <= w["observed_at"] < end]


# ── admin: simulation controls and ground truth ─────────────────────────────
def _clock_state(sim: Simulation) -> ClockState:
    return ClockState(
        sim_time=sim.clock.now(),
        engine_time=sim.engine.cursor,
        backlog_s=sim.backlog.total_seconds(),
        speed=sim.clock.speed,
        paused=sim.clock.paused,
    )


@admin.get("/clock", response_model=ClockState)
async def get_clock(sim: Sim):
    return _clock_state(sim)


@admin.patch("/clock", response_model=ClockState)
async def update_clock(sim: Sim, body: ClockUpdate):
    if body.speed is not None:
        sim.clock.set_speed(body.speed)
    if body.paused is True:
        sim.clock.pause()
    elif body.paused is False:
        sim.clock.resume()
    return _clock_state(sim)


@admin.post("/clock/advance", response_model=ClockState)
async def advance_clock(sim: Sim, body: AdvanceRequest):
    """Fast-forward. The events of the skipped period are generated in the background, as a burst."""
    delta = timedelta(hours=body.hours, minutes=body.minutes)
    if not timedelta(0) < delta <= timedelta(days=30):
        raise HTTPException(422, "advance by more than 0 and at most 30 days")
    sim.clock.advance(delta)
    return _clock_state(sim)


@admin.get("/faults")
async def list_faults(sim: Sim) -> list[dict]:
    return sim.faults.describe()


def _known_fault(name: str) -> str:
    if name not in FAULTS:
        raise HTTPException(404, f"unknown fault {name!r}; known: {sorted(FAULTS)}")
    return name


@admin.patch("/faults/{name}")
async def update_fault(sim: Sim, name: str, body: FaultUpdate) -> dict:
    sim.faults.update(_known_fault(name), enabled=body.enabled, rate=body.rate)
    return next(f for f in sim.faults.describe() if f["name"] == name)


@admin.post("/faults/{name}/trigger")
async def trigger_fault(sim: Sim, name: str, body: FaultTrigger | None = None) -> dict:
    """Force one injection: the next eligible event (per-event faults) or the next step (episodes)."""
    station_id = body.station_id if body else None
    try:
        sim.faults.trigger(_known_fault(name), station_id)
    except KeyError:
        raise HTTPException(404, f"unknown station {station_id!r}") from None
    return {"armed": name, "station_id": station_id, "sim_time": sim.clock.now()}


@admin.get("/fault-log")
async def fault_log(
    sim: Sim, after_id: int = 0, limit: Annotated[int, Query(ge=1, le=10_000)] = 1000
) -> list[dict]:
    """Ground truth: every injected fault with fault_id > after_id (pull it incrementally)."""
    return sim.faults.log.after(after_id, limit)


@admin.get("/stats")
async def stats(sim: Sim) -> dict:
    return {
        "sim_time": sim.clock.now(),
        "last_seq": sim.log.last_seq,
        "buffered_events": sim.log.buffered(),
        "emitted_by_type": dict(sim.emitted),
        "faults_injected": sim.faults.injected,
        "engine": dict(sim.engine.counters),
        "rides_in_progress": sim.engine.rides_in_progress,
        "bikes_docked": sim.world.n_docked(),
    }
