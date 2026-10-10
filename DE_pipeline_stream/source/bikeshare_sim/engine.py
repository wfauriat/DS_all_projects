"""Engine: advances the world in fixed simulated steps and produces the truthful events.

    clock (where we should be) ──► Simulation.catch_up() ──► Engine.step() × N
                                                                  │
    truthful events of [t0, t1) ◄─────────────────────────────────┘
      → faults.FaultLayer (corruption) → eventlog.EventLog (emission) → SSE

Fixed steps (10 simulated seconds by default) make the event stream a pure
function of (seed, config). It is the same at 1× or at 3600×, and the same
whether the run was paused or fast-forwarded.

Truthful event types (payloads documented in schemas.py):
    trip_started    a bike leaves a station
    trip_ended      a bike is docked. That may be at another station than
                    planned, if the target was full.
    station_status  a periodic snapshot of each station, every `status_interval_min`

Operator rebalancing and station capacity changes happen silently. They show up
only in station_status snapshots and in /v1/stations, as with a real vendor.
"""

import heapq
import uuid
from collections import Counter
from datetime import datetime, timedelta

import numpy as np

from .config import Settings
from .world import DEPARTURE_SHAPE, WeatherModel, World, period_of, weather_demand_factor


class Engine:
    def __init__(self, world: World, settings: Settings, rng: np.random.Generator) -> None:
        self.world = world
        self.rng = rng
        self.member_share = settings.world.member_share
        self.step_size = timedelta(seconds=settings.clock.step_seconds)

        start = settings.clock.start
        # Simulated time up to which events have been generated. It trails the clock.
        self.cursor: datetime = start

        # Status snapshots are staggered, so stations do not all report in the same second.
        n = len(world.stations)
        self._status_every = timedelta(minutes=settings.emission.status_interval_min)
        self._next_status = [start + self._status_every * (i / n) for i in range(n)]

        self._rebalance_every = timedelta(hours=settings.emission.rebalancing_every_h)
        self._next_rebalance = start + self._rebalance_every
        self._capacity_changes_per_day = settings.emission.capacity_changes_per_day

        # Bikes in transit: a min-heap of (arrival time, trip number, ride).
        self._rides: list[tuple[datetime, int, dict]] = []
        self._trip_counter = 0

        self._weather_model = WeatherModel(rng)
        self.weather: list[dict] = []  # hourly observations, served by /v1/weather
        self._next_weather = start.replace(minute=0, second=0, microsecond=0)

        # Internal truths, never emitted (exposed at /admin/stats).
        self.counters: Counter[str] = Counter()

    # ── the one public operation ────────────────────────────────────────────
    def step(self) -> tuple[datetime, datetime, list[dict]]:
        """Simulate [cursor, cursor + step) and return (t0, t1, events sorted by event_time)."""
        t0, t1 = self.cursor, self.cursor + self.step_size
        self._observe_weather(t1)
        events = [*self._departures(t0), *self._arrivals(t1), *self._status_snapshots(t1)]
        self._rebalance_if_due(t1)
        self._maybe_expand_a_station(t1)
        events.sort(key=lambda e: e["event_time"])
        self.cursor = t1
        return t0, t1, events

    @property
    def rides_in_progress(self) -> int:
        return len(self._rides)

    # ── trips ───────────────────────────────────────────────────────────────
    def _departures(self, t0: datetime) -> list[dict]:
        w = self.world
        minute = t0.hour * 60 + t0.minute
        weekend = int(t0.weekday() >= 5)
        per_second = (
            w.base_rate / 86_400
            * DEPARTURE_SHAPE[w.zone_idx, weekend, minute]
            * weather_demand_factor(self.weather[-1])
        )  # fmt: skip
        counts = self.rng.poisson(per_second * self.step_size.total_seconds())

        events = []
        for i in np.flatnonzero(counts):
            for _ in range(int(counts[i])):
                if not w.docked[i]:
                    self.counters["unmet_demand"] += 1  # an empty station turns riders away
                    continue
                started_at = t0 + self.step_size * float(self.rng.random())
                events.append(self._start_trip(int(i), started_at))
        return events

    def _start_trip(self, origin: int, started_at: datetime) -> dict:
        w = self.world
        docked = w.docked[origin]
        bike = w.bikes[docked.pop(int(self.rng.integers(len(docked))))]
        destination = self._choose_destination(origin, started_at)
        rider_type = "member" if self.rng.random() < self.member_share else "casual"
        duration = self._ride_duration(origin, destination, bike.bike_type, rider_type)

        self._trip_counter += 1
        ride = {
            "trip_id": f"TR-{self._trip_counter:08d}",
            "bike_id": bike.bike_id,
            "bike_type": bike.bike_type,
            "origin": origin,
            "destination": destination,
            "started_at": started_at,
        }
        heapq.heappush(self._rides, (started_at + duration, self._trip_counter, ride))
        return self._event(
            "trip_started",
            started_at,
            {
                "trip_id": ride["trip_id"],
                "bike_id": bike.bike_id,
                "bike_type": bike.bike_type,
                "station_id": w.stations[origin].station_id,
                "rider_type": rider_type,
            },
        )

    def _choose_destination(self, origin: int, t: datetime) -> int:
        w = self.world
        weights = w.pull_by_period[period_of(t)] * w.decay[origin]
        weights[origin] *= 0.1  # a few round trips (mostly leisure rides)
        return int(self.rng.choice(len(weights), p=weights / weights.sum()))

    def _ride_duration(self, origin: int, dest: int, bike_type: str, rider_type: str) -> timedelta:
        km = (
            max(float(self.world.distance_km[origin, dest]), 0.6) * 1.35
        )  # streets are not straight
        speed_kmh = self.rng.normal(19.0 if bike_type == "electric" else 14.0, 2.0)
        if rider_type == "casual":
            speed_kmh *= 0.8
        minutes = km / max(speed_kmh, 6.0) * 60 * self.rng.lognormal(0.0, 0.25)
        if origin == dest:
            minutes += self.rng.exponential(20.0)  # a loop around the park
        return timedelta(minutes=max(minutes, 2.0))

    def _arrivals(self, t1: datetime) -> list[dict]:
        w = self.world
        events = []
        while self._rides and self._rides[0][0] < t1:
            arrives_at, number, ride = heapq.heappop(self._rides)
            dest = ride["destination"]
            if len(w.docked[dest]) >= w.stations[dest].capacity:
                # Target is full: the rider goes on to the nearest station with a free dock.
                alt = self._nearest_free_dock(dest)
                extra = timedelta(minutes=float(w.distance_km[dest, alt]) * 1.35 / 12.0 * 60 + 1)
                ride["destination"] = alt
                self.counters["redirected_full_station"] += 1
                heapq.heappush(self._rides, (arrives_at + extra, number, ride))
                continue

            w.docked[dest].append(ride["bike_id"])
            events.append(
                self._event(
                    "trip_ended",
                    arrives_at,
                    {
                        "trip_id": ride["trip_id"],
                        "bike_id": ride["bike_id"],
                        "bike_type": ride["bike_type"],
                        "station_id": w.stations[dest].station_id,
                        "start_station_id": w.stations[ride["origin"]].station_id,
                        "duration_s": round((arrives_at - ride["started_at"]).total_seconds()),
                    },
                )
            )
        return events

    def _nearest_free_dock(self, dest: int) -> int:
        w = self.world
        for j in np.argsort(w.distance_km[dest]):
            if j != dest and len(w.docked[j]) < w.stations[j].capacity:
                return int(j)
        raise RuntimeError("every station is full")  # prevented by the n_bikes check in world.py

    # ── station snapshots ───────────────────────────────────────────────────
    def _status_snapshots(self, t1: datetime) -> list[dict]:
        """Snapshots due in this step, stamped with the step's END.

        The values are read after the whole step's trips are applied, so they are
        the station's state at t1. Stamping a snapshot with its scheduled time
        inside the step would report trips from its own future: a snapshot at
        08:00:03 would already count a bike taken at 08:00:07. A consumer
        checking "snapshot N vs snapshot N-1 vs the trips in between" (dbt's
        int_status_sequence) would then be misled.
        """
        events = []
        for i in range(len(self.world.stations)):
            while self._next_status[i] < t1:
                events.append(self._event("station_status", t1, self._status(i)))
                self._next_status[i] += self._status_every
        return events

    def _status(self, i: int) -> dict:
        w = self.world
        station, docked = w.stations[i], w.docked[i]
        return {
            "station_id": station.station_id,
            "bikes_available": len(docked),
            "ebikes_available": sum(1 for b in docked if w.bikes[b].bike_type == "electric"),
            "docks_available": station.capacity - len(docked),
            "is_renting": True,
        }

    # ── silent operator activity ────────────────────────────────────────────
    def _rebalance_if_due(self, t1: datetime) -> None:
        """Every few hours, trucks move bikes from the fullest stations to the emptiest."""
        if t1 < self._next_rebalance:
            return
        self._next_rebalance += self._rebalance_every
        w = self.world
        occupancy = [len(d) / s.capacity for d, s in zip(w.docked, w.stations)]
        donors = sorted(
            (i for i, o in enumerate(occupancy) if o > 0.8), key=lambda i: -occupancy[i]
        )
        takers = sorted((i for i, o in enumerate(occupancy) if o < 0.2), key=lambda i: occupancy[i])
        for d, r in zip(donors, takers):
            surplus = len(w.docked[d]) - w.stations[d].capacity // 2
            room = w.stations[r].capacity // 2 - len(w.docked[r])
            for _ in range(min(surplus, room, 20)):  # one truck carries up to 20 bikes
                w.docked[r].append(w.docked[d].pop())
                self.counters["rebalanced_bikes"] += 1

    def _maybe_expand_a_station(self, t1: datetime) -> None:
        """Occasionally a station gets more docks: slowly changing reference data."""
        p = self._capacity_changes_per_day * self.step_size.total_seconds() / 86_400
        if self.rng.random() < p:
            station = self.world.stations[int(self.rng.integers(len(self.world.stations)))]
            station.capacity += int(self.rng.choice([2, 4, 6]))
            station.updated_at = t1
            self.counters["capacity_changes"] += 1

    def _observe_weather(self, t1: datetime) -> None:
        while self._next_weather < t1:
            self.weather.append(self._weather_model.observe(self._next_weather))
            self._next_weather += timedelta(hours=1)

    # ── event construction ──────────────────────────────────────────────────
    def _event(self, event_type: str, event_time: datetime, payload: dict) -> dict:
        """Build a truthful event. `seq` and `emitted_at` are added at emission (eventlog.py)."""
        return {
            # UUID bytes come from the seeded RNG, so ids are reproducible across runs.
            "event_id": str(uuid.UUID(bytes=self.rng.bytes(16), version=4)),
            "event_type": event_type,
            "schema_version": 1,
            "event_time": event_time,
            "payload": payload,
        }
