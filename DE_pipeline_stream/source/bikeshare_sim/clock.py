"""Simulated clock: maps real (wall) time to simulated time, with a speed factor.

    sim_now = anchor_sim + (wall_now − anchor_wall) × speed

Every control (set_speed, pause, resume) first *re-anchors*: the current simulated
time becomes the new anchor, so time never jumps backwards or skips. advance() is
the deliberate exception. It jumps forward (a fast-forward), and the engine then
generates the skipped period as a burst (see simulation.py).

Who uses it: simulation.Simulation reads now(), and the admin routes in api.py
drive the controls. The clock itself generates nothing.
"""

import time
from collections.abc import Callable
from datetime import datetime, timedelta


class SimClock:
    def __init__(
        self,
        start: datetime,
        speed: float,
        wall: Callable[[], float] = time.monotonic,
    ) -> None:
        if start.tzinfo is None:
            raise ValueError("simulated start must be timezone-aware (UTC)")
        # time.monotonic rather than time.time: immune to NTP or manual clock changes.
        # Tests inject a fake wall clock to control time exactly.
        self._wall = wall
        self._anchor_sim = start
        self._anchor_wall = wall()
        self._speed = float(speed)
        self._paused = False

    def now(self) -> datetime:
        if self._paused:
            return self._anchor_sim
        elapsed = self._wall() - self._anchor_wall
        return self._anchor_sim + timedelta(seconds=elapsed * self._speed)

    @property
    def speed(self) -> float:
        return self._speed

    @property
    def paused(self) -> bool:
        return self._paused

    def set_speed(self, speed: float) -> None:
        if speed <= 0:
            raise ValueError("speed must be > 0 (use pause() to stop time)")
        self._reanchor()
        self._speed = float(speed)

    def pause(self) -> None:
        self._reanchor()
        self._paused = True

    def resume(self) -> None:
        self._reanchor()
        self._paused = False

    def advance(self, delta: timedelta) -> None:
        """Fast-forward: jump the simulated time ahead by `delta`."""
        if delta <= timedelta(0):
            raise ValueError("the clock only moves forward")
        self._reanchor()
        self._anchor_sim += delta

    def _reanchor(self) -> None:
        self._anchor_sim = self.now()
        self._anchor_wall = self._wall()

    # Persistence (pickle): a wall-clock anchor means nothing in another process,
    # so only the simulated time is saved, and the clock re-anchors when loaded.
    def __getstate__(self) -> dict:
        return {"sim_now": self.now(), "speed": self._speed, "paused": self._paused}

    def __setstate__(self, state: dict) -> None:
        self._wall = time.monotonic
        self._anchor_sim = state["sim_now"]
        self._anchor_wall = self._wall()
        self._speed = state["speed"]
        self._paused = state["paused"]
