"""Data-defect implementations.

OPERATOR: do not read this file.

Ported from the operator's original ``Planter``, but re-plumbed: instead of
mutating a materialised extract, each defect now acts on the bitemporal store.
A defect that "starts" at simulated time T is expressed either as a restatement
carrying ``knowledge_time = T`` (so the old value remains correct for any
``as_of`` before T) or as a property of rows that only become knowable after T.

Two of the seventeen never touch the store at all: they are properties of the
knowledge timeline itself, which the generator already produces.
"""

from __future__ import annotations

import datetime as dt
import random

import numpy as np

REFUND_WINDOW_DAYS = 30


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


class CustomerIndex:
    def __init__(self, world):
        self.world = world
        self.by_id: dict[str, list[dict]] = {}
        for r in world.customers:
            self.by_id.setdefault(r["customer_id"], []).append(r)
        for rows in self.by_id.values():
            rows.sort(key=lambda r: (r["valid_from"], r["version"]))

    def ids(self) -> list[str]:
        return list(self.by_id)

    def visible_interval(self, cid: str, at: dt.datetime) -> dict | None:
        """The row a reader would see for ``cid`` at knowledge time ``at``.

        Same rule as the store: highest version within each valid-time
        interval, most recent interval wins, and a tombstoned winner means the
        customer is simply not there.
        """
        winners: dict[dt.datetime, dict] = {}
        for r in self.by_id.get(cid, ()):
            if r["knowledge_time"] <= at and r["valid_from"] <= at:
                cur = winners.get(r["valid_from"])
                if cur is None or r["version"] > cur["version"]:
                    winners[r["valid_from"]] = r
        if not winners:
            return None
        row = winners[max(winners)]
        return None if row["_deleted"] else row

    def all_intervals(self, cid: str) -> list[dict]:
        return self.by_id.get(cid, [])

    def retire(self, cid: str, at: dt.datetime) -> int:
        """Take a customer out of the dimension for good.

        Anything already published gets a tombstone; anything that had not been
        published yet never will be, because the row it would have revised is
        gone.  Without the second half the customer walks back in later.
        """
        keep, drop = [], []
        for row in self.by_id.get(cid, ()):
            (drop if (row["knowledge_time"] > at or row["valid_from"] > at) else keep).append(row)
        if drop:
            doomed = set(map(id, drop))
            self.world.customers[:] = [r for r in self.world.customers if id(r) not in doomed]
            self.by_id[cid] = keep
        n = 0
        for row in list(keep):
            if row["_deleted"] == 0:
                self.restate(row, at, {"_deleted": 1})
                n += 1
        return n

    def restate(self, base: dict, at: dt.datetime, changes: dict) -> dict:
        cid = base["customer_id"]
        row = dict(base)
        row.update(changes)
        row["version"] = self.world.next_version[cid]
        self.world.next_version[cid] += 1
        row["knowledge_time"] = at
        self.world.customers.append(row)
        self.by_id[cid].append(row)
        self.by_id[cid].sort(key=lambda r: (r["valid_from"], r["version"]))
        return row


def _sample(rng: np.random.Generator, seq, n: int) -> list:
    n = max(0, min(int(n), len(seq)))
    if n == 0:
        return []
    idx = rng.choice(len(seq), size=n, replace=False)
    return [seq[int(i)] for i in idx]


def _keys(rows, field: str) -> list[str]:
    return sorted({r[field] for r in rows})


# ---------------------------------------------------------------------------
# the seventeen
# ---------------------------------------------------------------------------


class Planter:
    def __init__(self, world, rng: np.random.Generator, pyrng: random.Random, cfg):
        self.world = world
        self.w = {
            "customers": world.customers,
            "orders": world.orders,
            "payments": world.payments,
            "events": world.events,
            "labels": world.labels,
            "products": world.products,
        }
        self.rng = rng
        self.pyrng = pyrng
        self.cfg = cfg
        self.cidx = CustomerIndex(world)

    # -- ingestion -------------------------------------------------------

    def dup_exact_payments(self, pf):
        t0, t1 = pf.sim_start, pf.sim_end
        window = [p for p in self.w["payments"] if t0 <= p["knowledge_time"] < t1]
        picks = _sample(self.rng, window, len(window) * 0.35)
        for p in picks:
            self.w["payments"].append(dict(p))
        pf.params = {
            "table": "payments",
            "window": [t0.isoformat(), t1.isoformat()],
            "n_duplicated_rows": len(picks),
            "detail": "byte-identical second copy, same payment_id and version",
        }
        pf.affected_keys = _keys(picks, "payment_id")
        pf.n_affected = len(picks)

    def dup_near_orders(self, pf):
        t0, t1 = pf.sim_start, pf.sim_end
        orders = self.w["orders"]
        window = [o for o in orders if t0 <= o["occurred_at"] < t1 and o["version"] == 1]
        picks = _sample(self.rng, window, max(4, len(window) * 0.25))
        next_seq = max(int(o["order_id"].split("-")[1]) for o in orders) + 1
        labels_by_order = {l["order_id"]: l for l in self.w["labels"]}
        added = []
        for j, src in enumerate(picks):
            twin = dict(src)
            twin["order_id"] = f"ORD-{next_seq + j:08d}"
            twin["occurred_at"] = src["occurred_at"] + dt.timedelta(seconds=int(self.rng.integers(3, 40)))
            twin["knowledge_time"] = src["knowledge_time"] + dt.timedelta(seconds=int(self.rng.integers(3, 40)))
            twin["items"] = [dict(it) for it in src["items"]]
            added.append(twin)
            src_label = labels_by_order.get(src["order_id"])
            if src_label is not None:
                lab = dict(src_label)
                lab["order_id"] = twin["order_id"]
                self.w["labels"].append(lab)
        orders.extend(added)
        pf.params = {
            "table": "orders",
            "window": [t0.isoformat(), t1.isoformat()],
            "n_twin_orders": len(added),
            "detail": "same customer + same basket, seconds apart, fresh order_id",
        }
        pf.affected_keys = _keys(added, "order_id")
        pf.n_affected = len(added)

    def mojibake_city(self, pf):
        t0 = pf.sim_start
        hit = set()

        def mangle(s):
            try:
                return s.encode("utf-8").decode("latin-1")
            except (UnicodeEncodeError, UnicodeDecodeError):
                return s

        # everything written after the bug lands is affected in place ...
        for r in list(self.w["customers"]):
            if r["knowledge_time"] >= t0 and any(ord(ch) > 127 for ch in str(r["city"])):
                if self.rng.random() < 0.62:
                    r["city"] = mangle(r["city"])
                    hit.add(r["customer_id"])
        # ... and what was already published gets quietly rewritten
        for cid in self.cidx.ids():
            cur = self.cidx.visible_interval(cid, t0)
            if cur is None or not any(ord(ch) > 127 for ch in str(cur["city"])):
                continue
            if self.rng.random() < 0.62:
                self.cidx.restate(cur, t0, {"city": mangle(cur["city"])})
                hit.add(cid)
        pf.params = {"table": "customers", "column": "city", "from": t0.isoformat(),
                     "n_customers": len(hit)}
        pf.affected_keys = sorted(hit)
        pf.n_affected = len(hit)

    def id_leading_zeros(self, pf):
        pf.params = {
            "channel": "drop", "feed": "payments", "column": "customer_id",
            "from": pf.sim_start.isoformat(),
            "detail": "customer_id emitted as int64, leading zeros gone",
        }
        pf.n_affected = 0

    def field_rename_midstream(self, pf):
        pf.params = {
            "channel": "api", "table": "orders", "from_name": "channel",
            "to_name": "order_channel", "after": pf.sim_start.isoformat(),
            "detail": "applies to orders whose occurred_at is after the boundary",
        }
        pf.n_affected = sum(1 for o in self.w["orders"] if o["occurred_at"] > pf.sim_start)

    # -- time ------------------------------------------------------------

    def tz_mixed(self, pf):
        pf.params = {
            "channel": "api", "table": "orders", "column": "occurred_at",
            "after": pf.sim_start.isoformat(), "offset": "+00:00",
        }
        pf.n_affected = sum(1 for o in self.w["orders"] if o["occurred_at"] > pf.sim_start)

    def late_arriving_events(self, pf):
        t0, t1 = pf.sim_start, pf.sim_end
        hit = []
        for e in self.w["events"]:
            if t0 <= e["occurred_at"] < t1 and self.rng.random() < 0.25:
                e["knowledge_time"] = e["knowledge_time"] + dt.timedelta(
                    days=float(self.rng.uniform(1.0, 4.0))
                )
                hit.append(e)
        pf.params = {
            "table": "events", "window": [t0.isoformat(), t1.isoformat()],
            "n_events_delayed": len(hit),
            "detail": "knowledge_time pushed 1-4 days past the event",
        }
        pf.affected_keys = _keys(hit[:400], "event_id")
        pf.n_affected = len(hit)

    def dst_duplicate_hour(self, pf):
        target = (pf.sim_start + dt.timedelta(hours=2)).date()
        hit = []
        for o in self.w["orders"]:
            if o["occurred_at"].date() == target and self.rng.random() < 0.5:
                o["occurred_at"] = o["occurred_at"].replace(hour=2)
                hit.append(o)
        pf.params = {
            "table": "orders", "date": target.isoformat(), "n_rows": len(hit),
            "detail": "wall times piled into the 02:00 hour of a civil-time transition",
        }
        pf.affected_keys = _keys(hit[:400], "order_id")
        pf.n_affected = len(hit)

    # -- semantic --------------------------------------------------------

    def unit_switch_price(self, pf):
        pf.params = {
            "channel": "api", "table": "orders", "column": "order_total_cents",
            "after": pf.sim_start.isoformat(),
            "detail": "integer cents -> float dollars, column name unchanged",
        }
        pf.n_affected = sum(1 for o in self.w["orders"] if o["occurred_at"] > pf.sim_start)

    def category_rename(self, pf):
        t0 = pf.sim_start
        us_ids = sorted({r["customer_id"] for r in self.w["customers"] if r["country"] == "US"})
        picks = set(_sample(self.rng, us_ids, len(us_ids) * 0.45))
        for r in list(self.w["customers"]):
            if r["knowledge_time"] >= t0 and r["country"] == "US" and r["customer_id"] in picks:
                r["country"] = "USA"
        for cid in picks:
            cur = self.cidx.visible_interval(cid, t0)
            if cur is not None and cur["country"] == "US":
                self.cidx.restate(cur, t0, {"country": "USA"})
        pf.params = {"table": "customers", "column": "country", "map": "US -> USA",
                     "from": t0.isoformat(), "n_customers": len(picks)}
        pf.affected_keys = sorted(picks)
        pf.n_affected = len(picks)

    def new_category_late(self, pf):
        t0 = pf.sim_start
        hit = []
        for o in self.w["orders"]:
            if o["occurred_at"] > t0 and self.rng.random() < 0.18:
                o["payment_method"] = "bnpl"
                hit.append(o)
        pf.params = {"table": "orders", "column": "payment_method", "value": "bnpl",
                     "after": t0.isoformat(), "n_rows": len(hit)}
        pf.affected_keys = _keys(hit[:400], "order_id")
        pf.n_affected = len(hit)

    def sentinel_values(self, pf):
        t0 = pf.sim_start
        counts: dict[str, int] = {}
        hit = set()

        def hit_row(row, r):
            if r < 0.04:
                row["lifetime_value_cents"] = -999
                counts["lifetime_value_cents=-999"] = counts.get("lifetime_value_cents=-999", 0) + 1
            elif r < 0.06:
                row["signup_date"] = "1900-01-01"
                counts["signup_date=1900-01-01"] = counts.get("signup_date=1900-01-01", 0) + 1
            elif r < 0.085:
                row["city"] = self.pyrng.choice(["N/A", "", "null", "NULL", "-"])
                counts["city=sentinel"] = counts.get("city=sentinel", 0) + 1
            else:
                return False
            hit.add(row["customer_id"])
            return True

        for row in list(self.w["customers"]):
            if row["knowledge_time"] >= t0:
                hit_row(row, float(self.rng.random()))
        for cid in self.cidx.ids():
            cur = self.cidx.visible_interval(cid, t0)
            if cur is None:
                continue
            r = float(self.rng.random())
            if r < 0.085:
                nxt = dict(cur)
                if hit_row(nxt, r):
                    self.cidx.restate(cur, t0, {k: nxt[k] for k in
                                                ("lifetime_value_cents", "signup_date", "city")})
        pf.params = {"table": "customers", "from": t0.isoformat(), "counts": counts}
        pf.affected_keys = sorted(hit)[:200]
        pf.n_affected = len(hit)

    # -- statistical -----------------------------------------------------

    def covariate_shift(self, pf):
        t0 = pf.sim_start
        factors: dict[str, float] = {}
        for o in self.w["orders"]:
            if o["occurred_at"] > t0:
                f = 1.0 + 0.55 * float(self.rng.random())
                o["order_total_cents"] = int(o["order_total_cents"] * f)
                for it in o["items"]:
                    it["unit_price_cents"] = int(it["unit_price_cents"] * f)
                factors[o["order_id"]] = f
        for p in self.w["payments"]:
            f = factors.get(p["order_id"])
            if f is not None:
                p["amount_cents"] = int(p["amount_cents"] * f)
        pf.params = {"table": "orders", "column": "order_total_cents",
                     "after": t0.isoformat(), "n_rows": len(factors),
                     "detail": "basket sizes inflate in the later period"}
        pf.n_affected = len(factors)

    def mnar_missing(self, pf):
        t0 = pf.sim_start
        hit = []
        for o in self.w["orders"]:
            if o["occurred_at"] <= t0:
                continue
            p = 0.22 if o["_true_refund"] else 0.03
            if self.rng.random() < p:
                o["shipping_speed"] = None
                hit.append(o)
        pf.params = {"table": "orders", "column": "shipping_speed", "after": t0.isoformat(),
                     "n_missing": len(hit),
                     "detail": "P(missing) is ~7x higher for orders that end up refunded"}
        pf.affected_keys = _keys(hit[:400], "order_id")
        pf.n_affected = len(hit)

    def label_censoring(self, pf):
        horizon_end = self.cfg.sim_end
        never = [l for l in self.w["labels"] if l["knowledge_time"] > horizon_end]
        pf.params = {
            "table": "labels", "refund_window_days": REFUND_WINDOW_DAYS,
            "detail": (
                "a negative label is only knowable once the refund window closes; "
                "an order with no label row at as_of is undetermined, not negative"
            ),
            "n_unmatured_at_horizon_end": len(never),
        }
        pf.n_affected = len(never)

    # -- leakage / integrity ---------------------------------------------

    def leakage_column(self, pf):
        pf.params = {
            "channel": "api", "table": "orders",
            "columns": ["refund_processed_at", "support_ticket_count"],
            "after": pf.sim_start.isoformat(),
            "detail": "both are populated after the moment a prediction would be made",
        }
        pf.n_affected = 0

    def survivorship_customers(self, pf):
        t0 = pf.sim_start
        alive = [cid for cid in self.cidx.ids() if self.cidx.visible_interval(cid, t0) is not None]
        picks = _sample(self.rng, alive, max(3, len(alive) * 0.03))
        for cid in picks:
            self.cidx.retire(cid, t0)
        pf.params = {"table": "customers", "from": t0.isoformat(), "n_removed": len(picks),
                     "detail": "orders, payments and events still reference these customer_ids"}
        pf.affected_keys = sorted(picks)
        pf.n_affected = len(picks)


ORDER = [
    # store-shape changes first, so later planters see the final row set
    "dup_near_orders",
    "survivorship_customers",
    "covariate_shift",
    "new_category_late",
    "mnar_missing",
    "dst_duplicate_hour",
    "late_arriving_events",
    "category_rename",
    "mojibake_city",
    "sentinel_values",
    "dup_exact_payments",
    "label_censoring",
    # pure render directives, no store effect
    "field_rename_midstream",
    "tz_mixed",
    "unit_switch_price",
    "leakage_column",
    "id_leading_zeros",
]


def apply(world, by_id: dict, rng, pyrng, cfg) -> None:
    planter = Planter(world, rng, pyrng, cfg)
    for name in ORDER:
        pf = by_id.get(name)
        if pf is None:
            continue
        getattr(planter, name)(pf)
