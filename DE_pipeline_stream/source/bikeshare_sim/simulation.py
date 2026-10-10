"""Simulation: wires clock → engine → fault layer → event log, and persists them.

This is the only object the API talks to. It runs as one asyncio task inside the
FastAPI process (started in api.py's lifespan):

    every tick (tick_ms of real time):
        target = clock.now()
        while engine.cursor < target:      # at most MAX_STEPS per tick, then yield
            t0, t1, truth = engine.step()
            emitted = faults.process(t0, t1, truth)
            log.publish(emitted, emitted_at=t1)

In steady state the engine trails the clock by less than one step. After a
fast-forward (clock.advance) it trails by the size of the jump and catches up as
fast as the CPU allows. Consumers see that burst as a backlog.

Persistence: state.pkl in settings.state_dir (default data/source/) holds the
world, the simulated time, the engine, the faults with their ground-truth log,
and the event buffer. It is written every SAVE_EVERY_S seconds and at shutdown.
"""

import asyncio
import hashlib
import logging
import os
import pickle
import time
from collections import Counter
from collections.abc import Callable
from datetime import timedelta
from pathlib import Path

import numpy as np

from .clock import SimClock
from .config import Settings
from .engine import Engine
from .eventlog import EventLog
from .faults import FaultLayer
from .world import build_world

log = logging.getLogger("bikeshare_sim")

MAX_STEPS_PER_TICK = 500  # bounds the time the loop blocks the API during a catch-up
REPORT_EVERY_S = 30
SAVE_EVERY_S = 60


class Simulation:
    def __init__(self, settings: Settings, wall: Callable[[], float] = time.monotonic) -> None:
        self.settings = settings
        seed = settings.world.seed
        self.world = build_world(settings.world, settings.clock.start)
        self.clock = SimClock(settings.clock.start, settings.clock.speed, wall)
        # Separate RNG streams: toggling faults never changes the truthful world.
        self.engine = Engine(self.world, settings, rng=np.random.default_rng([seed, 1]))
        self.faults = FaultLayer(settings.faults, self.world, rng=np.random.default_rng([seed, 2]))
        self.log = EventLog(settings.emission.buffer_size)
        self.emitted: Counter[str] = Counter()  # events published, by event_type

    # ── running ─────────────────────────────────────────────────────────────
    def catch_up(self, max_steps: int = MAX_STEPS_PER_TICK) -> int:
        """Run engine steps until the engine reaches the clock (or max_steps). Returns steps run."""
        target = self.clock.now()
        steps = 0
        while steps < max_steps and self.engine.cursor + self.engine.step_size <= target:
            t0, t1, truth = self.engine.step()
            emitted = self.faults.process(t0, t1, truth)
            self.log.publish(emitted, emitted_at=t1)
            self.emitted.update(e["event_type"] for e in emitted)
            steps += 1
        return steps

    async def run(self) -> None:
        """Background task: keep the engine in step with the clock, forever."""
        tick = self.settings.clock.tick_ms / 1000
        last_report = last_save = time.monotonic()
        while True:
            steps = self.catch_up()
            now = time.monotonic()
            if now - last_report >= REPORT_EVERY_S:
                log.info(self.summary_line())
                last_report = now
            if now - last_save >= SAVE_EVERY_S:
                self.save()
                last_save = now
            # Still behind (after a fast-forward)? Yield to the API/SSE, then carry on at once.
            await asyncio.sleep(0 if steps == MAX_STEPS_PER_TICK else tick)

    @property
    def backlog(self) -> timedelta:
        """How far the engine trails the clock (large right after a fast-forward)."""
        return max(self.clock.now() - self.engine.cursor, timedelta(0))

    def summary_line(self) -> str:
        return (
            f"sim={self.clock.now():%Y-%m-%d %H:%M} speed={self.clock.speed:g}x "
            f"paused={self.clock.paused} backlog={self.backlog.total_seconds():.0f}s "
            f"seq={self.log.last_seq} emitted={dict(self.emitted)} "
            f"faults={ {k: v for k, v in self.faults.injected.items() if v} }"
        )

    # ── persistence ─────────────────────────────────────────────────────────
    @staticmethod
    def fingerprint(settings: Settings) -> str:
        """Identity of a world. A saved state only resumes under the same fingerprint."""
        identity = (
            settings.world.model_dump_json()
            + settings.clock.start.isoformat()
            + str(settings.clock.step_seconds)
            + str(settings.emission.status_interval_min)
        )
        return hashlib.sha256(identity.encode()).hexdigest()[:16]

    @property
    def state_path(self) -> Path:
        return self.settings.state_dir / "state.pkl"

    def save(self) -> None:
        state = {
            "fingerprint": self.fingerprint(self.settings),
            # One pickle for everything, so the objects that share `world` stay shared.
            "world": self.world,
            "clock": self.clock,
            "engine": self.engine,
            "faults": self.faults,
            "log": self.log,
            "emitted": self.emitted,
        }
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.state_path.with_suffix(".tmp")
        tmp.write_bytes(pickle.dumps(state, protocol=pickle.HIGHEST_PROTOCOL))
        os.replace(tmp, self.state_path)  # atomic: a crash never leaves a half-written file

    @classmethod
    def load_or_create(cls, settings: Settings) -> "Simulation":
        sim = cls(settings)
        path = sim.state_path
        if not path.exists():
            log.info("no saved state at %s: starting a fresh world", path)
            return sim

        state = pickle.loads(path.read_bytes())
        if state.get("fingerprint") != cls.fingerprint(settings):
            backup = path.with_suffix(".pkl.bak")
            os.replace(path, backup)
            log.warning(
                "saved state is from another world config: moved to %s, starting fresh", backup
            )
            return sim

        for name in ("world", "clock", "engine", "faults", "log", "emitted"):
            setattr(sim, name, state[name])
        # Controls always come from config at startup (see config/source.toml).
        sim.clock.set_speed(settings.clock.speed)
        if sim.clock.paused:
            sim.clock.resume()
        sim.faults.configure(settings.faults)
        log.info("resumed saved state: %s", sim.summary_line())
        return sim
