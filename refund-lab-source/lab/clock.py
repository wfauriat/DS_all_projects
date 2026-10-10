"""Simulated time.

Two clocks run at once.  The consumer's pipeline runs on real time -- hourly,
unattended, for months.  The world runs on simulated time, much faster:

    sim_now = anchor_sim + (real_now - anchor_real) * ratio

Every mutation (pause, resume, ratio change, jump) re-anchors rather than
recomputing from the origin, so changing the acceleration never rewinds the
world.  State lives in ``.state/clock.json`` and a service restart resumes from
where it was, including the time the service spent down.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import threading
import time
from dataclasses import dataclass, field

from lab import config as cfgmod

SECONDS_PER_HOUR = 3600.0


@dataclass
class ClockState:
    sim_start: str
    sim_end: str
    anchor_sim: str
    anchor_real: float
    ratio: float                 # simulated days per real hour
    paused: bool = False
    jumps: list = field(default_factory=list)

    def to_json(self) -> str:
        return json.dumps(self.__dict__, indent=2, sort_keys=True) + "\n"


class Clock:
    def __init__(self, cfg, path=None):
        self.cfg = cfg
        self.path = path or cfgmod.CLOCK_PATH
        self._lock = threading.Lock()
        self.state = self._load()

    # -- persistence -----------------------------------------------------

    def _load(self) -> ClockState:
        if self.path.exists():
            st = ClockState(**json.loads(self.path.read_text(encoding="utf-8")))
            if st.ratio != self.cfg.clock.sim_days_per_real_hour and not st.paused:
                # adopt the new acceleration from here on, without rewinding
                now = self._sim_at(st, time.time())
                st.anchor_sim = now.isoformat()
                st.anchor_real = time.time()
                st.ratio = self.cfg.clock.sim_days_per_real_hour
                self._save(st)
            return st
        st = ClockState(
            sim_start=self.cfg.clock.sim_start.isoformat(),
            sim_end=self.cfg.sim_end.isoformat(),
            anchor_sim=self.cfg.clock.sim_start.isoformat(),
            anchor_real=time.time(),
            ratio=self.cfg.clock.sim_days_per_real_hour,
        )
        self._save(st)
        return st

    def _save(self, st: ClockState | None = None) -> None:
        st = st or self.state
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".json.tmp")
        tmp.write_text(st.to_json(), encoding="utf-8")
        os.replace(tmp, self.path)

    # -- reading ---------------------------------------------------------

    def _sim_at(self, st: ClockState, real_now: float) -> dt.datetime:
        anchor = dt.datetime.fromisoformat(st.anchor_sim)
        if st.paused:
            sim = anchor
        else:
            elapsed_h = (real_now - st.anchor_real) / SECONDS_PER_HOUR
            sim = anchor + dt.timedelta(days=elapsed_h * st.ratio)
        end = dt.datetime.fromisoformat(st.sim_end)
        return min(sim, end).replace(microsecond=0)

    def now(self) -> dt.datetime:
        with self._lock:
            return self._sim_at(self.state, time.time())

    @property
    def sim_start(self) -> dt.datetime:
        return dt.datetime.fromisoformat(self.state.sim_start)

    @property
    def sim_end(self) -> dt.datetime:
        return dt.datetime.fromisoformat(self.state.sim_end)

    def at_horizon_end(self) -> bool:
        return self.now() >= self.sim_end

    def describe(self) -> dict:
        with self._lock:
            now = self._sim_at(self.state, time.time())
            return {
                "sim_now": now.isoformat(),
                "sim_start": self.state.sim_start,
                "sim_end": self.state.sim_end,
                "sim_days_per_real_hour": self.state.ratio,
                "paused": self.state.paused,
                "at_horizon_end": now >= dt.datetime.fromisoformat(self.state.sim_end),
                "manual_jumps": self.state.jumps,
                "elapsed_sim_days": round(
                    (now - dt.datetime.fromisoformat(self.state.sim_start)).total_seconds() / 86400.0, 3
                ),
            }

    # -- mutation --------------------------------------------------------

    def _rebase(self, real_now: float | None = None) -> None:
        real_now = real_now if real_now is not None else time.time()
        self.state.anchor_sim = self._sim_at(self.state, real_now).isoformat()
        self.state.anchor_real = real_now

    def pause(self) -> dict:
        with self._lock:
            if not self.state.paused:
                self._rebase()
                self.state.paused = True
                self._save()
            return self.describe_locked()

    def resume(self) -> dict:
        with self._lock:
            if self.state.paused:
                self.state.paused = False
                self.state.anchor_real = time.time()
                self._save()
            return self.describe_locked()

    def set_ratio(self, ratio: float) -> dict:
        if ratio <= 0:
            raise ValueError("ratio must be > 0")
        with self._lock:
            self._rebase()
            self.state.ratio = float(ratio)
            self._save()
            return self.describe_locked()

    def advance(self, days: float) -> dict:
        if days <= 0:
            raise ValueError("days must be > 0")
        with self._lock:
            self._rebase()
            before = dt.datetime.fromisoformat(self.state.anchor_sim)
            after = min(before + dt.timedelta(days=float(days)), self.sim_end)
            self.state.anchor_sim = after.isoformat()
            self.state.jumps.append(
                {"at_sim": before.isoformat(), "to_sim": after.isoformat(),
                 "days": round((after - before).total_seconds() / 86400.0, 4)}
            )
            self._save()
            return self.describe_locked()

    def describe_locked(self) -> dict:
        now = self._sim_at(self.state, time.time())
        return {
            "sim_now": now.isoformat(),
            "sim_start": self.state.sim_start,
            "sim_end": self.state.sim_end,
            "sim_days_per_real_hour": self.state.ratio,
            "paused": self.state.paused,
            "at_horizon_end": now >= dt.datetime.fromisoformat(self.state.sim_end),
            "manual_jumps": self.state.jumps,
            "elapsed_sim_days": round(
                (now - dt.datetime.fromisoformat(self.state.sim_start)).total_seconds() / 86400.0, 3
            ),
        }
