"""Seed draw, fault selection, scheduling and sealing.

OPERATOR: do not read this file.

The seed is drawn from ``os.urandom`` at ``lab init`` and is written only into
the sealed ledger.  Nothing at runtime prints it, and no accessor exposes the
plan.  The run-id is ``sha256(seed)[:8]`` and is safe to quote in bug reports.
"""

from __future__ import annotations

import base64
import datetime as dt
import hashlib
import json
import os
import random
import zlib
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np

from .catalog import ALWAYS_ON, CATALOG, SAMPLEABLE

SEAL_HEADER = (
    "# SEALED LEDGER -- refund-lab-source\n"
    "# Do not decode this by hand.  Doing so ends the exercise and cannot be\n"
    "# undone.  Use:  python grade.py --incidents <path> --reveal\n"
)

# How long each fault's effect lasts, in simulated days, as (lo, hi).  A value
# of None means "from the moment it starts until the end of the horizon".
_DURATION_DAYS: dict[str, tuple[float, float] | None] = {
    # data
    "dup_exact_payments": (1.0, 5.0),
    "dup_near_orders": (1.0, 7.0),
    "mojibake_city": None,
    "id_leading_zeros": None,
    "field_rename_midstream": None,
    "tz_mixed": None,
    "late_arriving_events": (10.0, 45.0),
    "dst_duplicate_hour": (1.0, 1.0),
    "unit_switch_price": None,
    "category_rename": None,
    "new_category_late": None,
    "sentinel_values": None,
    "covariate_shift": None,
    "mnar_missing": None,
    "leakage_column": None,
    "survivorship_customers": None,
    "label_censoring": None,
    # operational
    "backfill_restatement": None,
    "late_partition": (1.0, 1.0),
    "missing_delivery": (1.0, 1.0),
    "duplicate_delivery": (1.0, 1.0),
    "partial_write": (1.0, 1.0),
    "column_added": None,
    "column_reordered": None,
    "encoding_change": (5.0, 40.0),
    "compression_change": (5.0, 40.0),
    "clock_skew": (2.0, 10.0),
    "sustained_outage": (3.0 / 24.0, 9.0 / 24.0),
    "silent_row_deletion": None,
    "id_reuse": None,
}

# Faults that must sit inside a particular slice of the horizon, expressed as
# fractions of it.  Everything else is free to land anywhere in the schedulable
# region.
_WINDOW_FRACTION: dict[str, tuple[float, float]] = {
    "new_category_late": (0.80, 0.95),
    "covariate_shift": (0.60, 0.90),
    "leakage_column": (0.15, 0.75),
}

# Faults whose effect is continuous for the whole run rather than an event.
_SPANS_HORIZON = {"label_censoring", "backfill_restatement"}

# Leave a clean stretch at the start (so there is a baseline to compare with)
# and at the end (so there is time to notice).
_LEAD_IN_DAYS = 45
_TAIL_DAYS = 30


@dataclass
class PlannedFault:
    id: str
    family: str
    group: str
    description: str
    sim_start: dt.datetime
    sim_end: dt.datetime
    params: dict = field(default_factory=dict)
    affected_keys: list = field(default_factory=list)
    n_affected: int = 0

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "family": self.family,
            "group": self.group,
            "description": self.description,
            "sim_start": self.sim_start.replace(microsecond=0).isoformat(),
            "sim_end": self.sim_end.replace(microsecond=0).isoformat(),
            "params": self.params,
            "affected_keys": self.affected_keys[:200],
            "n_affected": self.n_affected,
        }

    @staticmethod
    def from_dict(d: dict) -> "PlannedFault":
        return PlannedFault(
            id=d["id"],
            family=d["family"],
            group=d["group"],
            description=d.get("description", ""),
            sim_start=dt.datetime.fromisoformat(d["sim_start"]),
            sim_end=dt.datetime.fromisoformat(d["sim_end"]),
            params=d.get("params", {}),
            affected_keys=d.get("affected_keys", []),
            n_affected=int(d.get("n_affected", 0)),
        )


# ---------------------------------------------------------------------------
# seed
# ---------------------------------------------------------------------------


def draw_seed() -> bytes:
    return os.urandom(16)


def run_id(seed: bytes) -> str:
    return hashlib.sha256(seed).hexdigest()[:8]


def rngs(seed: bytes) -> tuple[np.random.Generator, random.Random]:
    n = int.from_bytes(seed, "big")
    return np.random.default_rng(n), random.Random(n ^ 0x5EED)


# ---------------------------------------------------------------------------
# selection and scheduling
# ---------------------------------------------------------------------------


def select(pyrng: random.Random, n_sampled: int) -> list[str]:
    k = max(0, min(n_sampled - len(ALWAYS_ON), len(SAMPLEABLE)))
    chosen = pyrng.sample(SAMPLEABLE, k=k)
    return list(ALWAYS_ON) + chosen


def _dst_boundaries(start: dt.datetime, end: dt.datetime) -> list[dt.datetime]:
    """European civil-time transitions inside the horizon.

    March: 02:00 does not exist.  October: 02:00 happens twice.
    """
    out = []
    for year in range(start.year, end.year + 1):
        for month, hour in ((3, 2), (10, 3)):
            d = dt.date(year, month, 31 if month == 3 else 31)
            while d.month != month:
                d -= dt.timedelta(days=1)
            while d.weekday() != 6:  # last Sunday
                d -= dt.timedelta(days=1)
            ts = dt.datetime.combine(d, dt.time(hour))
            if start <= ts <= end:
                out.append(ts)
    return sorted(out)


def schedule(
    fault_ids: list[str],
    rng: np.random.Generator,
    pyrng: random.Random,
    sim_start: dt.datetime,
    horizon_days: int,
) -> list[PlannedFault]:
    horizon_end = sim_start + dt.timedelta(days=horizon_days)
    lo = _LEAD_IN_DAYS
    hi = max(lo + 1.0, horizon_days - _TAIL_DAYS)

    # Real incidents arrive in bunches.  Draw a handful of cluster centres and
    # pull roughly half the schedule towards one of them.
    n_clusters = int(rng.integers(2, 5))
    centres = [float(rng.uniform(lo, hi)) for _ in range(n_clusters)]

    def draw_day(fid: str) -> float:
        if fid in _WINDOW_FRACTION:
            f0, f1 = _WINDOW_FRACTION[fid]
            return float(rng.uniform(f0 * horizon_days, f1 * horizon_days))
        if pyrng.random() < 0.45:
            centre = pyrng.choice(centres)
            return float(np.clip(centre + rng.normal(0, 4.0), lo, hi))
        return float(rng.uniform(lo, hi))

    dst = _dst_boundaries(sim_start, horizon_end)

    planned: list[PlannedFault] = []
    for fid in fault_ids:
        family, group, desc = CATALOG[fid]
        if fid in _SPANS_HORIZON:
            t0, t1 = sim_start, horizon_end
        else:
            day = draw_day(fid)
            t0 = sim_start + dt.timedelta(days=day)
            if fid == "dst_duplicate_hour" and dst:
                t0 = min(dst, key=lambda x: abs((x - t0).total_seconds()))
                t0 -= dt.timedelta(hours=2)
            dur = _DURATION_DAYS.get(fid)
            if dur is None:
                t1 = horizon_end
            else:
                t1 = t0 + dt.timedelta(days=float(rng.uniform(*dur)))
            t1 = min(t1, horizon_end)
        planned.append(
            PlannedFault(
                id=fid, family=family, group=group, description=desc,
                sim_start=t0.replace(microsecond=0),
                sim_end=t1.replace(microsecond=0),
            )
        )
    planned.sort(key=lambda p: p.sim_start)
    return planned


# ---------------------------------------------------------------------------
# sealing
# ---------------------------------------------------------------------------


def seal(obj: dict) -> str:
    blob = json.dumps(obj, sort_keys=True, separators=(",", ":")).encode()
    return base64.b64encode(zlib.compress(blob, 9)).decode()


def unseal(text: str) -> dict:
    body = "".join(
        line.strip() for line in text.splitlines() if line and not line.startswith("#")
    )
    return json.loads(zlib.decompress(base64.b64decode(body)))


def write_ledger(path: Path, ledger: dict) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(SEAL_HEADER + seal(ledger) + "\n", encoding="utf-8")
    return sealed_digest(path)


def read_ledger(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(
            f"no ledger at {path}; run `python lab.py init` first"
        )
    return unseal(path.read_text(encoding="utf-8"))


def sealed_digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()
