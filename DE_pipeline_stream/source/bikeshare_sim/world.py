"""World model: the fictional city of Riverton, with its stations, bikes, demand and weather.

Everything here is generated from the seed, so it is deterministic. The module
holds state only: no clock, no I/O. engine.py moves bike ids between the
`World.docked` lists as trips start and end, and asks WeatherModel for an
observation once per simulated hour.

The station and bike attributes are also the reference data that the API
serves at /v1/stations and /v1/bikes.
"""

import math
from dataclasses import dataclass
from datetime import datetime, timedelta

import numpy as np

from .config import WorldConfig

ZONES = ("residential", "business", "transit", "leisure")

# ── Demand: when riders leave ───────────────────────────────────────────────
# Each zone's daily departure curve is a sum of Gaussian bumps
# (peak hour, relative height, width in hours) on top of a small night floor.
# The curves are normalised to a daily mean of 1, so a station's departure
# rate at time t is (its average departures per day) × shape(t).
_BUMPS = {
    #              weekday bumps                              weekend bumps
    "residential": ([(8.0, 1.0, 1.0), (18.5, 0.35, 1.5)], [(11.0, 0.5, 2.5), (17.0, 0.45, 2.5)]),
    "business": ([(12.5, 0.35, 1.0), (18.0, 1.0, 1.2)], [(14.0, 0.3, 3.0)]),
    "transit": ([(8.0, 0.8, 1.0), (18.0, 0.8, 1.3)], [(13.0, 0.4, 3.0)]),
    "leisure": ([(13.0, 0.3, 2.0), (19.0, 0.45, 2.0)], [(15.0, 1.0, 3.0)]),
}
_NIGHT_FLOOR = 0.03


def _build_shape_table() -> np.ndarray:
    """Return shape[zone, is_weekend, minute_of_day], each curve with a daily mean of 1."""
    hours = np.arange(24 * 60) / 60.0
    table = np.zeros((len(ZONES), 2, hours.size))
    for z, zone in enumerate(ZONES):
        for weekend, bumps in enumerate(_BUMPS[zone]):
            curve = np.full_like(hours, _NIGHT_FLOOR)
            for peak, height, width in bumps:
                curve += height * np.exp(-0.5 * ((hours - peak) / width) ** 2)
            table[z, weekend] = curve / curve.mean()
    return table


DEPARTURE_SHAPE = _build_shape_table()

# Bigger and busier places generate more departures (on top of station size).
ZONE_DEMAND_FACTOR = {"residential": 1.0, "business": 1.1, "transit": 1.8, "leisure": 0.8}

# ── Demand: where riders go ─────────────────────────────────────────────────
# How attractive each destination zone is, by period, before the distance
# decay is applied. Mornings pull riders to offices and stations; evenings
# pull them back home. This is what creates the commute imbalance that the
# operator's rebalancing trucks have to fix.
DESTINATION_PULL = {
    "am_peak": {"residential": 0.4, "business": 3.0, "transit": 2.0, "leisure": 0.4},
    "pm_peak": {"residential": 3.0, "business": 0.5, "transit": 1.5, "leisure": 1.0},
    "off_peak": {"residential": 1.0, "business": 1.0, "transit": 1.0, "leisure": 1.5},
    "weekend": {"residential": 1.0, "business": 0.6, "transit": 0.8, "leisure": 2.5},
}
DISTANCE_DECAY_KM = 1.8


def period_of(t: datetime) -> str:
    if t.weekday() >= 5:
        return "weekend"
    if 6 <= t.hour < 10:
        return "am_peak"
    if 16 <= t.hour < 20:
        return "pm_peak"
    return "off_peak"


# ── Entities ────────────────────────────────────────────────────────────────
@dataclass
class Station:
    station_id: str
    name: str
    lat: float
    lon: float
    capacity: int
    zone: str
    installed_at: datetime
    # Last change to a reference attribute (capacity). dbt snapshots build SCD2 history from it.
    updated_at: datetime


@dataclass
class Bike:
    bike_id: str
    bike_type: str  # "mechanical" | "electric"
    commissioned_at: datetime


@dataclass
class World:
    stations: list[Station]
    bikes: dict[str, Bike]
    docked: list[list[str]]  # docked[i] = ids of the bikes currently parked at station i
    distance_km: np.ndarray  # (n, n) straight-line distances between stations
    zone_idx: np.ndarray  # (n,) index into ZONES
    base_rate: np.ndarray  # (n,) average departures per simulated day
    pull_by_period: dict[str, np.ndarray]  # period → (n,) destination attractiveness
    decay: np.ndarray  # (n, n) distance decay exp(-d / DISTANCE_DECAY_KM)

    def __post_init__(self) -> None:
        self.index = {s.station_id: i for i, s in enumerate(self.stations)}

    def n_docked(self) -> int:
        return sum(len(d) for d in self.docked)


# ── Weather ─────────────────────────────────────────────────────────────────
class WeatherModel:
    """Hourly weather: seasonal and daily temperature cycles, plus rain from a Markov chain."""

    def __init__(self, rng: np.random.Generator) -> None:
        self.rng = rng
        self.raining = False
        self.anomaly_c = 0.0  # slowly wandering deviation from the seasonal norm

    def observe(self, hour: datetime) -> dict:
        doy = hour.timetuple().tm_yday
        seasonal = 12.0 - 9.0 * math.cos(2 * math.pi * (doy - 20) / 365.25)
        daily = 4.5 * math.sin(2 * math.pi * (hour.hour - 9) / 24)  # warmest ~15:00
        self.anomaly_c = 0.92 * self.anomaly_c + self.rng.normal(0, 0.7)
        # Rain is a two-state Markov chain: it starts rarely and tends to persist.
        self.raining = self.rng.random() < (0.75 if self.raining else 0.04)
        precipitation = round(float(self.rng.exponential(1.2)), 1) if self.raining else 0.0
        return {
            "observed_at": hour,
            "temperature_c": round(seasonal + daily + self.anomaly_c, 1),
            "precipitation_mm": precipitation,
            "wind_kmh": round(abs(float(self.rng.normal(12, 6))), 1),
        }


def weather_demand_factor(obs: dict) -> float:
    """Multiplier on all departures: rain and cold keep riders home."""
    factor = 1.0 / (1.0 + 0.5 * obs["precipitation_mm"])
    t = obs["temperature_c"]
    if t < 3:
        factor *= 0.6
    elif t < 8 or t > 30:
        factor *= 0.8
    return factor


# ── Building the city ───────────────────────────────────────────────────────
_NAME_PARTS = (
    ("North", "South", "East", "West", "Old", "New", "Upper", "Lower", "Little", "Great"),
    ("Market", "Mill", "Bridge", "Park", "Harbour", "Chapel", "Garden", "Quarry",
     "Meadow", "Arcade", "Foundry", "Orchard", "Library", "Castle", "Brewery", "Canal"),
)  # fmt: skip
_TRANSIT_NAMES = ("Central Station", "Riverside Station", "North Station")
# Transit hubs sit at fixed spots (km from the centre, for a 4 km radius).
_TRANSIT_SPOTS = ((0.2, 0.1), (2.6, -0.5), (-2.3, 1.1))


def build_world(cfg: WorldConfig, start: datetime) -> World:
    rng = np.random.default_rng([cfg.seed, 0])  # world-building stream (engine: 1, faults: 2)
    n = cfg.n_stations

    xy = _place_stations(n, cfg.city_radius_km, rng)
    n_hubs = min(len(_TRANSIT_SPOTS), n)
    zones = _assign_zones(xy, cfg.city_radius_km, n_hubs, rng)
    names = _station_names(zones, rng)
    capacity = rng.integers(15, 41, size=n) + np.where(np.array(zones) == "transit", 10, 0)

    lat0, lon0 = cfg.city_center
    km_per_deg_lon = 111.32 * math.cos(math.radians(lat0))
    stations = []
    for i in range(n):
        installed = start - timedelta(days=int(rng.integers(90, 1500)))
        stations.append(
            Station(
                station_id=f"ST-{i + 1:03d}",
                name=names[i],
                lat=round(lat0 + xy[i, 1] / 111.32, 6),
                lon=round(lon0 + xy[i, 0] / km_per_deg_lon, 6),
                capacity=int(capacity[i]),
                zone=zones[i],
                installed_at=installed,
                updated_at=installed,
            )
        )

    if cfg.n_bikes > 0.85 * capacity.sum():
        raise ValueError(
            f"{cfg.n_bikes} bikes do not fit comfortably in {capacity.sum()} docks; "
            "lower n_bikes or raise n_stations"
        )
    bikes = {}
    for j in range(cfg.n_bikes):
        bike_id = f"BK-{j + 1:04d}"
        bikes[bike_id] = Bike(
            bike_id=bike_id,
            bike_type="electric" if rng.random() < cfg.ebike_share else "mechanical",
            commissioned_at=start - timedelta(days=int(rng.integers(30, 900))),
        )

    distance = np.hypot(xy[:, None, 0] - xy[None, :, 0], xy[:, None, 1] - xy[None, :, 1])
    zone_idx = np.array([ZONES.index(z) for z in zones])
    size_factor = capacity / capacity.mean()
    base_rate = (
        cfg.trips_per_station_day * size_factor * np.array([ZONE_DEMAND_FACTOR[z] for z in zones])
    )

    return World(
        stations=stations,
        bikes=bikes,
        docked=_initial_docking(list(bikes), capacity),
        distance_km=distance,
        zone_idx=zone_idx,
        base_rate=base_rate,
        pull_by_period={
            period: np.array([pull[z] for z in zones]) for period, pull in DESTINATION_PULL.items()
        },
        decay=np.exp(-distance / DISTANCE_DECAY_KM),
    )


def _place_stations(n: int, radius_km: float, rng: np.random.Generator) -> np.ndarray:
    """Return (n, 2) x/y positions in km: transit hubs first, then random points spaced ≥ 300 m apart."""
    scale = radius_km / 4.0
    points = [(x * scale, y * scale) for x, y in _TRANSIT_SPOTS[: min(len(_TRANSIT_SPOTS), n)]]
    for _ in range(200_000):
        if len(points) == n:
            return np.array(points)
        r = radius_km * math.sqrt(rng.random())  # sqrt → uniform over the disc's area
        a = 2 * math.pi * rng.random()
        p = (r * math.cos(a), r * math.sin(a))
        if all(math.dist(p, q) >= 0.3 for q in points):
            points.append(p)
    raise ValueError(f"cannot place {n} stations within {radius_km} km; raise city_radius_km")


def _assign_zones(
    xy: np.ndarray, radius_km: float, n_hubs: int, rng: np.random.Generator
) -> list[str]:
    zones = []
    for i, (x, y) in enumerate(xy):
        if i < n_hubs:
            zones.append("transit")
        elif math.hypot(x, y) < 0.4 * radius_km:  # city centre: offices and a few parks
            zones.append("business" if rng.random() < 0.8 else "leisure")
        else:  # outskirts: housing and a few parks or riversides
            zones.append("residential" if rng.random() < 0.82 else "leisure")
    return zones


def _station_names(zones: list[str], rng: np.random.Generator) -> list[str]:
    pool = [f"{a} {b}" for a in _NAME_PARTS[0] for b in _NAME_PARTS[1]]
    rng.shuffle(pool)
    hubs = iter(_TRANSIT_NAMES)
    others = iter(pool)
    return [next(hubs) if z == "transit" else next(others) for z in zones]


def _initial_docking(bike_ids: list[str], capacity: np.ndarray) -> list[list[str]]:
    """Spread the bikes so that every station starts at the same occupancy ratio."""
    targets = np.floor(capacity * len(bike_ids) / capacity.sum()).astype(int)
    remainder = len(bike_ids) - int(targets.sum())
    for i in np.argsort(-(capacity - targets))[:remainder]:
        targets[i] += 1
    docked, k = [], 0
    for t in targets:
        docked.append(bike_ids[k : k + t])
        k += t
    return docked
