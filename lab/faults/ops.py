"""Operational-fault implementations.

OPERATOR: do not read this file.

Three of these act on the store (rows get revised, removed, or their identifier
handed to somebody else).  The rest are properties of *delivery*: the record may
be perfectly correct, but the file was late, or truncated, or written twice, or
the API was not answering.  Those are recorded here as neutral directives which
the serving layer executes without knowing why.
"""

from __future__ import annotations

import datetime as dt
import random

import numpy as np

from .data import CustomerIndex, _keys, _sample


# ---------------------------------------------------------------------------
# store-level
# ---------------------------------------------------------------------------


class StorePlanter:
    def __init__(self, world, rng: np.random.Generator, pyrng: random.Random, cfg):
        self.world = world
        self.rng = rng
        self.pyrng = pyrng
        self.cfg = cfg

    def backfill_restatement(self, pf):
        sim_end = self.cfg.sim_end
        touched = {"orders": 0, "payments": 0, "customers": 0}
        keys: list[str] = []

        base_orders = list({o["order_id"]: o for o in self.world.orders if o["version"] == 1}.values())
        for o in _sample(self.rng, base_orders, len(base_orders) * 0.012):
            kt = o["knowledge_time"] + dt.timedelta(days=float(self.rng.uniform(10, 60)))
            if kt > sim_end:
                continue
            rev = dict(o)
            rev["items"] = [dict(it) for it in o["items"]]
            rev["version"] = o["version"] + 1
            rev["knowledge_time"] = kt
            adj = 1.0 + float(self.rng.uniform(-0.15, 0.15))
            rev["order_total_cents"] = max(1, int(o["order_total_cents"] * adj))
            self.world.orders.append(rev)
            touched["orders"] += 1
            keys.append(o["order_id"])

        base_pays = list({p["payment_id"]: p for p in self.world.payments if p["version"] == 1}.values())
        for p in _sample(self.rng, base_pays, len(base_pays) * 0.02):
            kt = p["knowledge_time"] + dt.timedelta(days=float(self.rng.uniform(5, 40)))
            if kt > sim_end:
                continue
            rev = dict(p)
            rev["version"] = p["version"] + 1
            rev["knowledge_time"] = kt
            if p["status"] == "pending":
                rev["status"] = "captured"
            else:
                rev["amount_cents"] = max(1, int(p["amount_cents"] * (1.0 + float(self.rng.uniform(-0.08, 0.08)))))
            self.world.payments.append(rev)
            touched["payments"] += 1

        cidx = CustomerIndex(self.world)
        base_custs = list({(r["customer_id"], r["valid_from"]): r
                           for r in self.world.customers if r["version"] == 1}.values())
        for r in _sample(self.rng, base_custs, len(base_custs) * 0.008):
            kt = r["knowledge_time"] + dt.timedelta(days=float(self.rng.uniform(14, 90)))
            if kt > sim_end:
                continue
            cidx.restate(r, kt, {
                "lifetime_value_cents": max(0, int(r["lifetime_value_cents"] * (1.0 + float(self.rng.uniform(-0.3, 0.3)))))
            })
            touched["customers"] += 1

        pf.params = {
            "always_on": True,
            "tables": touched,
            "detail": ("historical rows gain a higher version weeks after the fact; "
                       "the value visible at an earlier as_of is unchanged"),
        }
        pf.affected_keys = sorted(set(keys))[:200]
        pf.n_affected = sum(touched.values())

    def silent_row_deletion(self, pf):
        t0 = pf.sim_start
        span = float(self.rng.uniform(3, 10))
        lo = t0 - dt.timedelta(days=float(self.rng.uniform(30, 400)) + span)
        hi = lo + dt.timedelta(days=span)
        victims = [o for o in self.world.orders
                   if lo <= o["occurred_at"] < hi and o["version"] == 1]
        keep = float(self.rng.uniform(0.30, 0.60))
        picks = _sample(self.rng, victims, len(victims) * keep)
        highest = {}
        for o in self.world.orders:
            k = o["order_id"]
            if k not in highest or o["version"] > highest[k]["version"]:
                highest[k] = o
        for o in picks:
            tomb = dict(highest[o["order_id"]])
            tomb["items"] = [dict(it) for it in tomb["items"]]
            tomb["version"] = tomb["version"] + 1
            tomb["knowledge_time"] = t0
            tomb["_deleted"] = 1
            self.world.orders.append(tomb)
        pf.params = {
            "table": "orders", "at": t0.isoformat(),
            "valid_window": [lo.isoformat(), hi.isoformat()],
            "n_removed": len(picks),
            "detail": "no tombstone reaches the consumer; payments still reference them",
        }
        pf.affected_keys = _keys(picks, "order_id")[:200]
        pf.n_affected = len(picks)

    def id_reuse(self, pf):
        from lab.generator import CITIES, COUNTRIES, COUNTRY_WEIGHTS, SEGMENTS

        t0 = pf.sim_start
        gap = dt.timedelta(days=float(self.rng.uniform(7, 60)))
        cidx = CustomerIndex(self.world)
        candidates = [
            cid for cid in cidx.ids()
            if cidx.visible_interval(cid, t0) is not None
            and cidx.all_intervals(cid)[0]["valid_from"] < t0 - dt.timedelta(days=120)
        ]
        picks = _sample(self.rng, candidates, int(self.rng.integers(5, 21)))
        for cid in picks:
            cidx.retire(cid, t0)
            country = self.pyrng.choices(COUNTRIES, weights=COUNTRY_WEIGHTS)[0]
            fresh = {
                "customer_id": cid,
                "version": self.world.next_version[cid],
                "valid_from": (t0 + gap).replace(microsecond=0),
                "knowledge_time": (t0 + gap + dt.timedelta(minutes=int(self.rng.integers(5, 200)))),
                "_deleted": 0,
                "signup_date": (t0 + gap).date().isoformat(),
                "country": country,
                "city": self.pyrng.choice(CITIES[country]),
                "segment": self.pyrng.choices(SEGMENTS, weights=[70, 22, 8])[0],
                "lifetime_value_cents": int(max(0, self.rng.gamma(2.0, 4000))),
                "is_active": 1,
            }
            self.world.next_version[cid] += 1
            self.world.customers.append(fresh)
        pf.params = {
            "table": "customers", "retired_at": t0.isoformat(),
            "reissued_at": (t0 + gap).isoformat(), "n_ids": len(picks),
            "detail": "same customer_id, different human; historical facts now join to the wrong one",
        }
        pf.affected_keys = sorted(picks)
        pf.n_affected = len(picks)


STORE_ORDER = ["silent_row_deletion", "id_reuse", "backfill_restatement"]


def apply_to_store(world, by_id: dict, rng, pyrng, cfg) -> None:
    planter = StorePlanter(world, rng, pyrng, cfg)
    for name in STORE_ORDER:
        pf = by_id.get(name)
        if pf is not None:
            getattr(planter, name)(pf)
    for name, fn in _DELIVERY.items():
        pf = by_id.get(name)
        if pf is not None:
            fn(pf, rng, pyrng, cfg)


# ---------------------------------------------------------------------------
# delivery / serving directives
# ---------------------------------------------------------------------------

FEEDS = ("payments", "catalog")


def _pick_feed(rng) -> str:
    return FEEDS[int(rng.integers(0, len(FEEDS)))]


def _late_partition(pf, rng, pyrng, cfg):
    delay = int(rng.integers(1, 3))
    pf.sim_end = pf.sim_start + dt.timedelta(days=delay)
    pf.params = {
        "feed": "payments", "day": pf.sim_start.date().isoformat(),
        "delay_days": delay,
        "detail": "the file for that day appears in the directory N days later",
    }
    pf.n_affected = 1


def _missing_delivery(pf, rng, pyrng, cfg):
    pf.params = {
        "feed": _pick_feed(rng), "day": pf.sim_start.date().isoformat(),
        "detail": "no file for that day, ever",
    }
    pf.n_affected = 1


def _duplicate_delivery(pf, rng, pyrng, cfg):
    day = pf.sim_start.date().isoformat()
    pf.params = {
        "feed": "payments", "day": day, "suffix": f"_{int(rng.integers(1, 4)):02d}",
        "detail": "identical payload, second filename",
    }
    pf.n_affected = 1


def _partial_write(pf, rng, pyrng, cfg):
    pf.params = {
        "feed": "payments", "day": pf.sim_start.date().isoformat(),
        "keep_fraction": round(float(rng.uniform(0.25, 0.75)), 4),
        "detail": "written straight to the final path and cut off mid-row",
    }
    pf.n_affected = 1


def _column_added(pf, rng, pyrng, cfg):
    feed = _pick_feed(rng)
    pf.params = {
        "feed": feed,
        "column": "settlement_batch_id" if feed == "payments" else "catalog_revision",
        "after": pf.sim_start.date().isoformat(),
        "position": int(rng.integers(1, 4)),
    }
    pf.n_affected = 0


def _column_reordered(pf, rng, pyrng, cfg):
    pf.params = {
        "feed": _pick_feed(rng),
        "after": pf.sim_start.date().isoformat(),
        "permutation_seed": int(rng.integers(1, 10**6)),
    }
    pf.n_affected = 0


def _encoding_change(pf, rng, pyrng, cfg):
    pf.params = {
        "feed": _pick_feed(rng),
        "from": pf.sim_start.date().isoformat(),
        "to": pf.sim_end.date().isoformat(),
        "encoding": "cp1252",
    }
    pf.n_affected = 0


def _compression_change(pf, rng, pyrng, cfg):
    feed = _pick_feed(rng)
    pf.params = {
        "feed": feed,
        "from": pf.sim_start.date().isoformat(),
        "to": pf.sim_end.date().isoformat(),
        "gzip": feed != "payments",   # flip whatever that feed normally does
        "detail": "filename and extension unchanged",
    }
    pf.n_affected = 0


def _clock_skew(pf, rng, pyrng, cfg):
    secs = int(rng.integers(90, 1500)) * (1 if rng.random() < 0.5 else -1)
    pf.params = {
        "from": pf.sim_start.isoformat(), "to": pf.sim_end.isoformat(),
        "seconds": secs,
        "detail": "applies to timestamps as rendered, on both channels; the store is unchanged",
    }
    pf.n_affected = 0


def _sustained_outage(pf, rng, pyrng, cfg):
    pf.params = {
        "from": pf.sim_start.isoformat(), "to": pf.sim_end.isoformat(),
        "status": 503,
        "detail": "read endpoints only; /v1/health keeps saying ok",
    }
    pf.n_affected = 0


_DELIVERY = {
    "late_partition": _late_partition,
    "missing_delivery": _missing_delivery,
    "duplicate_delivery": _duplicate_delivery,
    "partial_write": _partial_write,
    "column_added": _column_added,
    "column_reordered": _column_reordered,
    "encoding_change": _encoding_change,
    "compression_change": _compression_change,
    "clock_skew": _clock_skew,
    "sustained_outage": _sustained_outage,
}
