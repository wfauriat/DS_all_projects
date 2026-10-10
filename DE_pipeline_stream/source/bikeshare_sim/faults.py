"""Fault layer: turns the truthful event stream into what the vendor actually emits.

    engine (truth) ──► FaultLayer.process() ──► event log (what consumers see)
                              │
                              └──► FaultLog (ground truth: what was corrupted, when and how)

Two kinds of fault:
    per-event  each eligible event is hit with probability `rate`
    episode    `rate` = expected episodes per simulated day. An episode lasts a
               while and affects one station, or the whole stream for stream_stall.
Any fault can also be forced once with trigger() (POST /admin/faults/{name}/trigger).

The fault layer has its own RNG. Toggling faults therefore never changes the
truthful world: the same trips happen either way, only their emission differs.

The ground-truth log is what the pipeline's detections are scored against
(dbt mart_detection_scorecard, layer 5).
"""

import copy
import heapq
from dataclasses import dataclass
from datetime import datetime, timedelta

import numpy as np

from .config import FaultConfig
from .world import World

PER_EVENT = "per_event"
EPISODE = "episode"

# name → (kind, description). Descriptions are served by GET /admin/faults.
FAULTS: dict[str, tuple[str, str]] = {
    "duplicate": (PER_EVENT, "an event is emitted twice with the same event_id"),
    "late_event": (PER_EVENT, "an event is held back for 30–180 simulated minutes"),
    "schema_drift": (PER_EVENT, "a payload field is renamed, mistyped or missing"),
    "impossible_status": (PER_EVENT, "station_status reports > capacity or < 0 bikes"),
    "teleport": (PER_EVENT, "a short trip ends at a far station (implied speed > 45 km/h)"),
    "orphan_trip": (PER_EVENT, "a trip_started or trip_ended is never emitted"),
    "frozen_station": (EPISODE, "a station keeps repeating an identical status"),
    "silent_station": (EPISODE, "a station stops publishing station_status"),
    "stream_stall": (EPISODE, "the whole stream stops, then flushes its backlog at once"),
}
TELEPORT_MIN_SPEED_KMH = 45.0


@dataclass
class Episode:
    fault_id: int
    fault_type: str
    station_id: str | None  # None for stream_stall (whole stream)
    starts_at: datetime
    ends_at: datetime


class FaultLog:
    """Append-only ground truth of every injected fault. Served by GET /admin/fault-log."""

    def __init__(self) -> None:
        self.entries: list[dict] = []

    def add(self, fault_type: str, at: datetime, event: dict | None = None, **details) -> int:
        fault_id = len(self.entries) + 1
        self.entries.append(
            {
                "fault_id": fault_id,
                "fault_type": fault_type,
                "injected_at": at,
                "event_id": event["event_id"] if event else None,
                "event_type": event["event_type"] if event else None,
                "details": details,
            }
        )
        return fault_id

    def after(self, fault_id: int, limit: int) -> list[dict]:
        return self.entries[fault_id : fault_id + limit]  # fault_id n sits at index n-1


class FaultLayer:
    def __init__(self, configs: dict[str, FaultConfig], world: World, rng: np.random.Generator):
        self.world = world
        self.rng = rng
        self.log = FaultLog()
        self.configure(configs)
        self.injected: dict[str, int] = dict.fromkeys(FAULTS, 0)

        self._forced: dict[str, str | None] = {}  # one-shot triggers: name → station_id
        self._episodes: list[Episode] = []
        self._delayed: list[tuple[datetime, int, dict]] = []  # held-back events (late, duplicates)
        self._delayed_counter = 0
        self._stall_buffer: list[dict] = []
        self._last_status: dict[str, dict] = {}  # latest truthful payload per station
        self._frozen_payload: dict[str, dict] = {}

    # ── control (admin API) ─────────────────────────────────────────────────
    def configure(self, configs: dict[str, FaultConfig]) -> None:
        unknown = set(configs) - set(FAULTS)
        if unknown:
            raise ValueError(
                f"unknown faults in config: {sorted(unknown)}; known: {sorted(FAULTS)}"
            )
        self.configs = {name: configs.get(name, FaultConfig(enabled=False)) for name in FAULTS}

    def update(self, name: str, enabled: bool | None = None, rate: float | None = None) -> None:
        cfg = self.configs[name]
        self.configs[name] = cfg.model_copy(
            update={k: v for k, v in (("enabled", enabled), ("rate", rate)) if v is not None}
        )

    def trigger(self, name: str, station_id: str | None = None) -> None:
        """Force one injection. Per-event faults hit the next eligible event;
        episodes start at the next step."""
        if station_id is not None and station_id not in self.world.index:
            raise KeyError(station_id)
        self._forced[name] = station_id

    def describe(self) -> list[dict]:
        return [
            {
                "name": name,
                "kind": kind,
                "description": description,
                "enabled": self.configs[name].enabled,
                "rate": self.configs[name].rate,
                "params": self.configs[name].model_extra or {},
                "injected": self.injected[name],
                "armed": name in self._forced,
                "active_episodes": [
                    {"fault_id": e.fault_id, "station_id": e.station_id, "ends_at": e.ends_at}
                    for e in self._episodes
                    if e.fault_type == name
                ],
            }
            for name, (kind, description) in FAULTS.items()
        ]

    # ── the pipeline, called once per engine step ───────────────────────────
    def process(self, t0: datetime, t1: datetime, events: list[dict]) -> list[dict]:
        """Return the events to emit at t1: corrupted, delayed or buffered as the faults dictate."""
        flushed = self._update_episodes(t0, t1)

        out: list[dict] = []
        for event in events:
            out.extend(self._corrupt(event, t1))
        out.extend(self._release_delayed(t1))

        if self._active("stream_stall"):
            # Nothing leaves the vendor during a stall. If a stall ended and a new
            # one started in this very step, the backlog just flushed goes back in.
            self._stall_buffer.extend(flushed + out)
            return []
        return flushed + out

    def _corrupt(self, event: dict, t: datetime) -> list[dict]:
        """Pass one event through every per-event injector, in this order.
        Each injector returns the events to carry on with: [] drops, [e] keeps."""
        injectors = (
            self._silent_station,
            self._frozen_station,
            self._orphan_trip,
            self._teleport,
            self._impossible_status,
            self._schema_drift,
            self._duplicate,
            self._late_event,
        )
        batch = [event]
        for injector in injectors:
            batch = [out for e in batch for out in injector(e, t)]
        return batch

    # ── per-event injectors ─────────────────────────────────────────────────
    def _silent_station(self, e: dict, t: datetime) -> list[dict]:
        if e["event_type"] == "station_status" and self._active(
            "silent_station", e["payload"]["station_id"]
        ):
            return []
        return [e]

    def _frozen_station(self, e: dict, t: datetime) -> list[dict]:
        if e["event_type"] != "station_status":
            return [e]
        station_id = e["payload"]["station_id"]
        if self._active("frozen_station", station_id) and station_id in self._frozen_payload:
            e["payload"] = dict(self._frozen_payload[station_id])  # stale values, fresh timestamp
        else:
            self._last_status[station_id] = dict(e["payload"])
        return [e]

    def _orphan_trip(self, e: dict, t: datetime) -> list[dict]:
        if e["event_type"] in ("trip_started", "trip_ended") and self._hit("orphan_trip"):
            side = "start" if e["event_type"] == "trip_started" else "end"
            self._record("orphan_trip", t, e, trip_id=e["payload"]["trip_id"], dropped=side)
            return []
        return [e]

    def _teleport(self, e: dict, t: datetime) -> list[dict]:
        if e["event_type"] != "trip_ended":
            return [e]
        p = e["payload"]
        # Only a short trip can be made implausibly fast: the reported station
        # must be further than 45 km/h × duration from the start station.
        needed_km = TELEPORT_MIN_SPEED_KMH * p["duration_s"] / 3600 * 1.1
        start = self.world.index[p["start_station_id"]]
        far = np.flatnonzero(self.world.distance_km[start] >= needed_km)
        if far.size == 0 or not self._hit("teleport"):
            return [e]
        fake = self.world.stations[int(self.rng.choice(far))]
        self._record(
            "teleport", t, e, trip_id=p["trip_id"], true_station_id=p["station_id"],
            reported_station_id=fake.station_id,
        )  # fmt: skip
        p["station_id"] = fake.station_id
        return [e]

    def _impossible_status(self, e: dict, t: datetime) -> list[dict]:
        if e["event_type"] != "station_status" or not self._hit("impossible_status"):
            return [e]
        p = e["payload"]
        capacity = self.world.stations[self.world.index[p["station_id"]]].capacity
        if self.rng.random() < 0.7:
            p["bikes_available"] = capacity + int(self.rng.integers(1, 9))
            p["docks_available"] = 0
        else:
            p["bikes_available"] = -int(self.rng.integers(1, 4))
        self._record(
            "impossible_status", t, e, station_id=p["station_id"],
            bikes_available=p["bikes_available"], capacity=capacity,
        )  # fmt: skip
        return [e]

    def _schema_drift(self, e: dict, t: datetime) -> list[dict]:
        if not self._hit("schema_drift"):
            return [e]
        p = e["payload"]
        match e["event_type"]:
            case "station_status":  # a GBFS-style name leaks into the payload
                p["num_bikes_available"] = p.pop("bikes_available")
                variant = "renamed bikes_available → num_bikes_available"
            case "trip_ended":  # a number becomes a string with a unit
                p["duration_s"] = f"{p['duration_s']}s"
                variant = "duration_s sent as a string"
            case _:  # a required field is missing
                p["bike_id"] = None
                variant = "bike_id is null"
        self._record("schema_drift", t, e, variant=variant)
        return [e]

    def _duplicate(self, e: dict, t: datetime) -> list[dict]:
        if self._hit("duplicate"):
            delay = timedelta(
                seconds=float(
                    self.rng.uniform(0, self.configs["duplicate"].param("max_delay_s", 300))
                )
            )
            self._hold(copy.deepcopy(e), t + delay)
            self._record("duplicate", t, e, copy_emitted_at=t + delay)
        return [e]

    def _late_event(self, e: dict, t: datetime) -> list[dict]:
        if not self._hit("late_event"):
            return [e]
        lo, hi = self.configs["late_event"].param("delay_min", [30, 180])
        release = t + timedelta(minutes=float(self.rng.uniform(lo, hi)))
        self._hold(e, release)
        self._record("late_event", t, e, emitted_at=release)
        return []

    # ── episodes ────────────────────────────────────────────────────────────
    def _update_episodes(self, t0: datetime, t1: datetime) -> list[dict]:
        """End expired episodes and start new ones. Returns the backlog if a stream stall just ended."""
        flushed: list[dict] = []
        for ep in [e for e in self._episodes if e.ends_at <= t0]:
            self._episodes.remove(ep)
            if ep.fault_type == "stream_stall":
                flushed, self._stall_buffer = self._stall_buffer, []

        step_days = (t1 - t0).total_seconds() / 86_400
        for name, (kind, _) in FAULTS.items():
            if kind != EPISODE:
                continue
            forced = name in self._forced
            cfg = self.configs[name]
            if forced or (cfg.enabled and self.rng.random() < cfg.rate * step_days):
                self._start_episode(name, t0, station_id=self._forced.pop(name, None))
        return flushed

    def _start_episode(self, name: str, t0: datetime, station_id: str | None) -> None:
        cfg = self.configs[name]
        if name == "stream_stall":
            if self._active("stream_stall"):
                return
            lo, hi = cfg.param("duration_min", [15, 60])
            duration = timedelta(minutes=float(self.rng.uniform(lo, hi)))
        else:
            if station_id is None:
                busy = {e.station_id for e in self._episodes if e.fault_type == name}
                free = [s.station_id for s in self.world.stations if s.station_id not in busy]
                station_id = str(self.rng.choice(free))
            if name == "frozen_station":
                if station_id not in self._last_status:
                    return  # nothing to freeze yet
                self._frozen_payload[station_id] = dict(self._last_status[station_id])
            lo, hi = cfg.param("duration_h", [1, 6])
            duration = timedelta(hours=float(self.rng.uniform(lo, hi)))

        fault_id = self._record(name, t0, None, station_id=station_id, ends_at=t0 + duration)
        self._episodes.append(Episode(fault_id, name, station_id, t0, t0 + duration))

    def _active(self, name: str, station_id: str | None = None) -> bool:
        return any(
            e.fault_type == name and (station_id is None or e.station_id == station_id)
            for e in self._episodes
        )

    # ── helpers ─────────────────────────────────────────────────────────────
    def _hit(self, name: str) -> bool:
        """Should this eligible event be corrupted? A pending trigger always wins."""
        if name in self._forced:
            del self._forced[name]
            return True
        cfg = self.configs[name]
        return cfg.enabled and self.rng.random() < cfg.rate

    def _hold(self, event: dict, release_at: datetime) -> None:
        self._delayed_counter += 1
        heapq.heappush(self._delayed, (release_at, self._delayed_counter, event))

    def _release_delayed(self, t1: datetime) -> list[dict]:
        released = []
        while self._delayed and self._delayed[0][0] < t1:
            released.append(heapq.heappop(self._delayed)[2])
        return released

    def _record(self, name: str, t: datetime, event: dict | None, **details) -> int:
        self.injected[name] += 1
        return self.log.add(name, t, event, **details)
