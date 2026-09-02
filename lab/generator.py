"""The world.

Refactored from the operator's original ``generate.py``: same marketplace, same
heavy-tailed customer activity, same multi-country character-set mix, same
latent refund process.  What changed is that facts are emitted along a horizon
of years rather than a single 180-day materialisation, and every fact carries a
``knowledge_time`` -- the moment this source system could first have told you
about it.

Nothing in this module knows anything about faults.
"""

from __future__ import annotations

import bisect
import datetime as dt
import math
import random
from dataclasses import dataclass, field

import numpy as np

COUNTRIES = ["US", "GB", "DE", "FR", "SE", "BR", "PL", "CH", "IS", "ES"]
COUNTRY_WEIGHTS = [30, 14, 12, 10, 7, 9, 6, 5, 2, 5]
CITIES = {
    "US": ["Portland", "Austin", "Boston"],
    "GB": ["Manchester", "Bristol", "Leeds"],
    "DE": ["Düsseldorf", "München", "Köln"],
    "FR": ["Nîmes", "Besançon", "Angers"],
    "SE": ["Malmö", "Göteborg", "Örebro"],
    "BR": ["São Paulo", "Curitiba", "Belém"],
    "PL": ["Kraków", "Łódź", "Gdańsk"],
    "CH": ["Zürich", "Genève", "Basel"],
    "IS": ["Reykjavík", "Akureyri", "Selfoss"],
    "ES": ["Málaga", "Gijón", "Córdoba"],
}
SEGMENTS = ["consumer", "smb", "enterprise"]
CHANNELS = ["web", "mobile", "partner_api"]
METHODS = ["card", "wallet", "bank_transfer", "voucher"]
PROCESSORS = ["northpay", "swiftpay", "orbit"]
CATEGORIES = ["apparel", "electronics", "home", "beauty", "outdoor"]
EVENT_TYPES = ["page_view", "search", "add_to_cart", "remove_from_cart", "checkout_start"]

PRODUCT_WORDS = {
    "apparel": ["Merino", "Oxford", "Linen", "Quilted", "Rib-Knit", "Poplin"],
    "electronics": ["Compact", "Wireless", "Studio", "Portable", "Desktop", "Travel"],
    "home": ["Ceramic", "Stoneware", "Oak", "Brushed", "Woven", "Enamel"],
    "beauty": ["Botanical", "Hydrating", "Mineral", "Vitamin", "Overnight", "Daily"],
    "outdoor": ["Trail", "Alpine", "Packable", "Ripstop", "All-Weather", "Summit"],
}
PRODUCT_NOUNS = {
    "apparel": ["Shirt", "Jacket", "Trousers", "Scarf", "Jumper"],
    "electronics": ["Speaker", "Charger", "Headphones", "Hub", "Lamp"],
    "home": ["Mug", "Bowl", "Throw", "Kettle", "Tray"],
    "beauty": ["Serum", "Cleanser", "Balm", "Mask", "Mist"],
    "outdoor": ["Pack", "Bottle", "Shell", "Stove", "Mat"],
}

REFUND_WINDOW_DAYS = 30
_BASELINE_DAYS = 180.0  # the density the operator's original generator produced


def sigmoid(x: float) -> float:
    return 1.0 / (1.0 + math.exp(-x))


def iso(ts: dt.datetime | None) -> str | None:
    return None if ts is None else ts.replace(microsecond=0).isoformat()


@dataclass
class World:
    """In-memory world, ready to be versioned into the store."""

    sim_start: dt.datetime
    sim_end: dt.datetime
    products: list[dict] = field(default_factory=list)
    customers: list[dict] = field(default_factory=list)   # one row per SCD2 version
    orders: list[dict] = field(default_factory=list)
    payments: list[dict] = field(default_factory=list)
    events: list[dict] = field(default_factory=list)
    labels: list[dict] = field(default_factory=list)
    next_version: dict[str, int] = field(default_factory=dict)  # customer_id -> next version

    def customer_versions(self) -> dict[str, list[dict]]:
        out: dict[str, list[dict]] = {}
        for row in self.customers:
            out.setdefault(row["customer_id"], []).append(row)
        for rows in out.values():
            rows.sort(key=lambda r: (r["valid_from"], r["version"]))
        return out


def _day_weights(sim_start: dt.datetime, horizon_days: int) -> np.ndarray:
    """Trade is weekday-heavy; weekends run at about three quarters."""
    days = np.arange(horizon_days)
    weekday = np.array(
        [(sim_start + dt.timedelta(days=int(d))).weekday() for d in days]
    )
    w = np.where(weekday >= 5, 0.72, 1.0)
    return w / w.sum()


def _timestamps(
    rng: np.random.Generator,
    sim_start: dt.datetime,
    day_offsets: np.ndarray,
    hour_mu: float,
    hour_sd: float,
) -> list[dt.datetime]:
    hours = np.clip(rng.normal(hour_mu, hour_sd, size=len(day_offsets)), 0, 23).astype(int)
    mins = rng.integers(0, 60, size=len(day_offsets))
    secs = rng.integers(0, 60, size=len(day_offsets))
    base = sim_start.replace(hour=0, minute=0, second=0, microsecond=0)
    return [
        base + dt.timedelta(days=int(d), hours=int(h), minutes=int(m), seconds=int(s))
        for d, h, m, s in zip(day_offsets, hours, mins, secs)
    ]


def _weighted_prefix_pick(
    rng: np.random.Generator,
    cum_weights: np.ndarray,
    eligible: np.ndarray,
) -> np.ndarray:
    """Pick, for each row, one of the first ``eligible[i]`` customers.

    Customers are held in signup order, so "the first k" is "everyone who had
    signed up by then".  Weights are the heavy-tailed activity weights.
    """
    k = np.maximum(eligible, 1)
    upper = cum_weights[k - 1]
    u = rng.random(len(k)) * upper
    idx = np.searchsorted(cum_weights, u, side="left")
    return np.minimum(idx, k - 1)


def build_world(
    rng: np.random.Generator,
    pyrng: random.Random,
    sim_start: dt.datetime,
    horizon_days: int,
    scale: float,
) -> World:
    span = horizon_days / _BASELINE_DAYS
    sim_end = sim_start + dt.timedelta(days=horizon_days)

    n_cust = max(50, int(2000 * scale * span))
    n_orders = max(200, int(12000 * scale * span))
    n_events = max(1000, int(60000 * scale * span))
    n_products = max(40, int(240 * scale))

    world = World(sim_start=sim_start, sim_end=sim_end)

    # ---- product catalogue ------------------------------------------------
    seen_sku: set[str] = set()
    for _ in range(n_products):
        cat = pyrng.choices(CATEGORIES, weights=[28, 20, 20, 18, 14])[0]
        while True:
            sku = f"{cat[:3].upper()}-{int(rng.integers(1000, 9999))}"
            if sku not in seen_sku:
                seen_sku.add(sku)
                break
        price = int(max(199, rng.lognormal(8.4, 0.7)))
        world.products.append(
            {
                "sku": sku,
                "version": 1,
                "category": cat,
                "product_name": f"{pyrng.choice(PRODUCT_WORDS[cat])} {pyrng.choice(PRODUCT_NOUNS[cat])}",
                "list_price_cents": price,
                "active_from": (sim_start - dt.timedelta(days=int(rng.integers(30, 400)))).date().isoformat(),
                "knowledge_time": sim_start,
                "_deleted": 0,
            }
        )

    # list prices are revised now and then; the daily catalogue snapshot moves
    for base in list(world.products):
        n_rev = int(rng.poisson(0.5 * horizon_days / 365.25))
        for t in sorted(
            sim_start + dt.timedelta(days=float(d))
            for d in rng.uniform(0, horizon_days, size=min(n_rev, 6))
        ):
            rev = dict(base)
            rev["version"] = base["version"] + 1
            base = rev
            rev["list_price_cents"] = int(max(199, rev["list_price_cents"] * float(rng.uniform(0.88, 1.22))))
            rev["active_from"] = t.date().isoformat()
            rev["knowledge_time"] = t.replace(microsecond=0)
            world.products.append(rev)

    # ---- customers --------------------------------------------------------
    # Roughly a third of the book predates the horizon; the rest arrive during it.
    n_legacy = max(1, int(n_cust * 0.35))
    legacy_signup = [
        sim_start - dt.timedelta(days=int(d))
        for d in rng.integers(1, 700, size=n_legacy)
    ]
    new_signup = [
        sim_start + dt.timedelta(days=float(d))
        for d in rng.uniform(0, horizon_days, size=n_cust - n_legacy)
    ]
    signups = sorted(legacy_signup + new_signup)

    for i, signup in enumerate(signups, start=1):
        country = pyrng.choices(COUNTRIES, weights=COUNTRY_WEIGHTS)[0]
        cid = f"{i:07d}"
        valid_from = signup.replace(microsecond=0)
        world.customers.append(
            {
                "customer_id": cid,
                "version": 1,
                "valid_from": valid_from,
                "knowledge_time": max(sim_start, valid_from + dt.timedelta(minutes=int(rng.integers(2, 90)))),
                "_deleted": 0,
                "signup_date": signup.date().isoformat(),
                "country": country,
                "city": pyrng.choice(CITIES[country]),
                "segment": pyrng.choices(SEGMENTS, weights=[70, 22, 8])[0],
                "lifetime_value_cents": int(max(0, rng.gamma(2.0, 9000))),
                "is_active": 1,
            }
        )
        world.next_version[cid] = 2

    # ---- SCD2 churn on the dimension --------------------------------------
    # A customer's record is revised now and then: they move, they get
    # reclassified, their lifetime value is recomputed, they go dormant.
    base_rows = list(world.customers)
    for row in base_rows:
        cid = row["customer_id"]
        alive_days = (sim_end - max(row["valid_from"], sim_start)).total_seconds() / 86400.0
        if alive_days <= 30:
            continue
        n_rev = int(rng.poisson(0.25 * alive_days / 365.25))
        if n_rev == 0:
            continue
        when = sorted(
            max(row["valid_from"], sim_start) + dt.timedelta(days=float(d))
            for d in rng.uniform(15, alive_days, size=min(n_rev, 4))
        )
        cur = dict(row)
        for t in when:
            nxt = dict(cur)
            nxt["valid_from"] = t.replace(microsecond=0)
            nxt["version"] = world.next_version[cid]
            world.next_version[cid] += 1
            nxt["knowledge_time"] = t + dt.timedelta(hours=float(rng.uniform(1, 60)))
            r = rng.random()
            if r < 0.30:
                nxt["country"] = pyrng.choices(COUNTRIES, weights=COUNTRY_WEIGHTS)[0]
                nxt["city"] = pyrng.choice(CITIES[nxt["country"]])
            elif r < 0.48:
                nxt["city"] = pyrng.choice(CITIES[nxt["country"]])
            elif r < 0.66:
                nxt["segment"] = pyrng.choices(SEGMENTS, weights=[55, 30, 15])[0]
            elif r < 0.94:
                nxt["lifetime_value_cents"] = int(
                    max(0, nxt["lifetime_value_cents"] * float(rng.uniform(0.9, 2.1)))
                )
            else:
                nxt["is_active"] = 0
            world.customers.append(nxt)
            cur = nxt

    # ---- activity weights, in signup order --------------------------------
    weights = rng.pareto(1.4, size=n_cust) + 1.0
    cum_weights = np.cumsum(weights)
    signup_days = np.array(
        [(s - sim_start).total_seconds() / 86400.0 for s in signups]
    )
    cust_ids = [f"{i:07d}" for i in range(1, n_cust + 1)]

    dayw = _day_weights(sim_start, horizon_days)

    # ---- orders -----------------------------------------------------------
    o_days = rng.choice(horizon_days, size=n_orders, p=dayw)
    o_ts = _timestamps(rng, sim_start, o_days, 14, 4)
    o_ts.sort()
    o_day_frac = np.array([(t - sim_start).total_seconds() / 86400.0 for t in o_ts])
    eligible = np.searchsorted(signup_days, o_day_frac, side="right")
    o_cust = _weighted_prefix_pick(rng, cum_weights, eligible)

    # prior-order counts, per customer, in time order
    sort_idx = np.lexsort((o_day_frac, o_cust))
    sorted_cust = o_cust[sort_idx]
    first_at = np.searchsorted(sorted_cust, sorted_cust, side="left")
    prior_sorted = np.arange(len(sorted_cust)) - first_at
    prior = np.empty(n_orders, dtype=int)
    prior[sort_idx] = prior_sorted

    n_items = rng.integers(1, 5, size=n_orders)
    total_items = int(n_items.sum())
    item_prod = rng.integers(0, n_products, size=total_items)
    item_qty = rng.integers(1, 4, size=total_items)
    item_disc = rng.random(size=total_items)
    channels = rng.choice(len(CHANNELS), size=n_orders, p=np.array([52, 40, 8]) / 100.0)
    methods = rng.choice(len(METHODS), size=n_orders, p=np.array([60, 25, 12, 3]) / 100.0)
    speeds = rng.choice(2, size=n_orders, p=[0.78, 0.22])
    ingest_lag = rng.integers(30, 600, size=n_orders)          # seconds
    api_lag = rng.integers(1200, 14400, size=n_orders)         # partner feeds are slower

    cursor = 0
    for i, ts in enumerate(o_ts):
        k = int(n_items[i])
        items = []
        for j in range(cursor, cursor + k):
            p = world.products[int(item_prod[j])]
            unit = p["list_price_cents"]
            if item_disc[j] < 0.15:
                unit = int(unit * (0.70 + 0.25 * float(item_disc[j] / 0.15)))
            items.append(
                {
                    "sku": p["sku"],
                    "category": p["category"],
                    "qty": int(item_qty[j]),
                    "unit_price_cents": int(unit),
                }
            )
        cursor += k
        total = sum(it["qty"] * it["unit_price_cents"] for it in items)
        channel = CHANNELS[int(channels[i])]
        lag = int(api_lag[i]) if channel == "partner_api" else int(ingest_lag[i])
        world.orders.append(
            {
                "order_id": f"ORD-{i + 1:08d}",
                "version": 1,
                "customer_id": cust_ids[int(o_cust[i])],
                "occurred_at": ts,
                "knowledge_time": ts + dt.timedelta(seconds=lag),
                "_deleted": 0,
                "channel": channel,
                "payment_method": METHODS[int(methods[i])],
                "shipping_speed": ("standard", "express")[int(speeds[i])],
                "order_total_cents": total,
                "items": items,
                "_prior_orders": int(prior[i]),
                "_top_cat": max(items, key=lambda x: x["qty"] * x["unit_price_cents"])["category"],
            }
        )

    # ---- latent refund process (unchanged from the original) --------------
    totals = np.array([o["order_total_cents"] for o in world.orders], dtype=float)
    lt = np.log1p(totals)
    z_total = (lt - lt.mean()) / (lt.std() or 1.0)
    noise = rng.normal(0, 0.35, size=n_orders)
    unif = rng.random(n_orders)
    lag_days = np.clip(rng.exponential(11.0, size=n_orders), 0.4, 29.9)
    base_tickets = rng.poisson(0.25, size=n_orders)
    extra_tickets = rng.poisson(1.6, size=n_orders)
    settle = rng.uniform(1.0, 5.0, size=n_orders)
    neg_settle = rng.uniform(0.2, 2.0, size=n_orders)

    for i, o in enumerate(world.orders):
        logit = (
            -2.35
            + 0.78 * z_total[i]
            + 0.55 * (o["channel"] == "mobile")
            + 0.85 * (o["_top_cat"] == "apparel")
            + 0.30 * (o["shipping_speed"] == "express")
            - 0.45 * min(o["_prior_orders"], 6) * 0.25
            + float(noise[i])
        )
        p = sigmoid(logit)
        refunded = bool(unif[i] < p)
        o["_refund_propensity"] = round(p, 6)
        o["_true_refund"] = int(refunded)
        o["_refund_ts"] = o["occurred_at"] + dt.timedelta(days=float(lag_days[i])) if refunded else None
        o["_support_tickets"] = int(base_tickets[i]) + (int(extra_tickets[i]) if refunded else 0)

        if refunded:
            resolved = o["_refund_ts"]
            kt = resolved + dt.timedelta(days=float(settle[i]))
        else:
            resolved = o["occurred_at"] + dt.timedelta(days=REFUND_WINDOW_DAYS)
            kt = resolved + dt.timedelta(days=float(neg_settle[i]))
        world.labels.append(
            {
                "order_id": o["order_id"],
                "version": 1,
                "resolved_at": resolved.replace(microsecond=0),
                "knowledge_time": kt.replace(microsecond=0),
                "_deleted": 0,
                "refund_at": o["_refund_ts"].replace(microsecond=0) if refunded else None,
                "refunded_within_30d": int(refunded),
            }
        )

    # ---- payments ---------------------------------------------------------
    # The settlement extract carries a denormalised billing city, snapshotted
    # when the payment was taken.
    city_hist: dict[str, tuple[list[dt.datetime], list[str]]] = {}
    for row in sorted(world.customers, key=lambda r: (r["customer_id"], r["valid_from"])):
        froms, cities = city_hist.setdefault(row["customer_id"], ([], []))
        froms.append(row["valid_from"])
        cities.append(row["city"])

    def city_at(cid: str, when: dt.datetime) -> str:
        froms, cities = city_hist.get(cid, ([], []))
        if not froms:
            return ""
        i = bisect.bisect_right(froms, when) - 1
        return cities[max(i, 0)]

    has_pay = rng.random(n_orders) < 0.965
    pay_proc = rng.choice(len(PROCESSORS), size=n_orders, p=np.array([55, 30, 15]) / 100.0)
    pay_gap = rng.integers(5, 900, size=n_orders)
    pay_status = rng.random(n_orders)
    proc_slow = rng.exponential(1.0, size=n_orders)
    proc_stall = rng.random(n_orders)

    seq = 0
    for i, o in enumerate(world.orders):
        if not has_pay[i]:
            continue
        seq += 1
        proc = PROCESSORS[int(pay_proc[i])]
        paid = o["occurred_at"] + dt.timedelta(seconds=int(pay_gap[i]))
        # settlement files land on the processor's own timetable, not ours
        if proc == "northpay":
            delay = dt.timedelta(minutes=5 + 85 * float(proc_slow[i]) / 3.0)
        elif proc == "swiftpay":
            delay = dt.timedelta(hours=3 + 11 * float(proc_slow[i]) / 3.0)
        else:
            delay = dt.timedelta(hours=10 * float(proc_slow[i]))
            if proc_stall[i] < 0.05:
                delay += dt.timedelta(days=1 + 2 * float(proc_stall[i]) / 0.05)
        world.payments.append(
            {
                "payment_id": f"PAY-{seq:08d}",
                "version": 1,
                "order_id": o["order_id"],
                "customer_id": o["customer_id"],
                "occurred_at": paid.replace(microsecond=0),
                "knowledge_time": (paid + delay).replace(microsecond=0),
                "_deleted": 0,
                "amount_cents": o["order_total_cents"],
                "status": "captured" if pay_status[i] < 0.97 else "pending",
                "processor": proc,
                "billing_city": city_at(o["customer_id"], paid),
            }
        )

    # ---- web events -------------------------------------------------------
    e_days = rng.choice(horizon_days, size=n_events, p=dayw)
    e_ts = _timestamps(rng, sim_start, e_days, 15, 5)
    e_ts.sort()
    e_day_frac = np.array([(t - sim_start).total_seconds() / 86400.0 for t in e_ts])
    e_elig = np.searchsorted(signup_days, e_day_frac, side="right")
    e_cust = _weighted_prefix_pick(rng, cum_weights, e_elig)
    e_types = rng.choice(len(EVENT_TYPES), size=n_events, p=np.array([55, 18, 14, 6, 7]) / 100.0)
    e_sku = rng.integers(0, n_products, size=n_events)
    e_sess = rng.integers(1, 10**7, size=n_events)
    e_lag = rng.integers(30, 900, size=n_events)
    e_slow = rng.random(n_events)
    e_slow_h = rng.uniform(1, 6, size=n_events)

    for i, ts in enumerate(e_ts):
        lag = dt.timedelta(seconds=int(e_lag[i]))
        if e_slow[i] < 0.015:
            lag += dt.timedelta(hours=float(e_slow_h[i]))
        world.events.append(
            {
                "event_id": f"EV-{i + 1:09d}",
                "version": 1,
                "customer_id": cust_ids[int(e_cust[i])],
                "session_id": f"S-{int(e_sess[i]):08d}",
                "occurred_at": ts,
                "knowledge_time": ts + lag,
                "_deleted": 0,
                "event_type": EVENT_TYPES[int(e_types[i])],
                "sku": world.products[int(e_sku[i])]["sku"],
            }
        )

    return world
