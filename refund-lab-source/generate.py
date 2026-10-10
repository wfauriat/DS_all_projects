#!/usr/bin/env python3
"""
Adversarial synthetic data generator for the refund-prediction lab.

Builds a small, deliberately heterogeneous marketplace dataset, then plants a
random subset of known defects and seals the answer key.

    python generate.py --seed 1337

The manifest is written obfuscated. Do not decode it by hand; use grade.py.
"""

from __future__ import annotations

import argparse
import base64
import csv
import datetime as dt
import gzip
import io
import json
import math
import os
import random
import shutil
import zlib
from pathlib import Path

import numpy as np

# --------------------------------------------------------------------------
# World parameters
# --------------------------------------------------------------------------

CUTOFF = dt.datetime(2025, 6, 30, 23, 59, 59)  # observation window ends here
WINDOW_DAYS = 180
START = CUTOFF - dt.timedelta(days=WINDOW_DAYS)

COUNTRIES = ["US", "GB", "DE", "FR", "SE", "BR", "PL", "CH", "IS", "ES"]
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


def sigmoid(x: float) -> float:
    return 1.0 / (1.0 + math.exp(-x))


def iso(ts: dt.datetime) -> str:
    return ts.replace(microsecond=0).isoformat()


# --------------------------------------------------------------------------
# Clean world
# --------------------------------------------------------------------------


def build_world(rng: np.random.Generator, pyrng: random.Random, scale: float) -> dict:
    n_cust = max(50, int(2000 * scale))
    n_orders = max(200, int(12000 * scale))

    customers = []
    for i in range(1, n_cust + 1):
        country = pyrng.choices(COUNTRIES, weights=[30, 14, 12, 10, 7, 9, 6, 5, 2, 5])[0]
        signup = START - dt.timedelta(days=int(rng.integers(0, 700)))
        customers.append(
            {
                "customer_id": f"{i:07d}",
                "signup_date": signup.date().isoformat(),
                "country": country,
                "city": pyrng.choice(CITIES[country]),
                "segment": pyrng.choices(SEGMENTS, weights=[70, 22, 8])[0],
                "lifetime_value_cents": int(max(0, rng.gamma(2.0, 9000))),
                "is_active": 1,
            }
        )

    cust_ids = [c["customer_id"] for c in customers]
    # heavy-tailed customer activity
    weights = rng.pareto(1.4, size=len(cust_ids)) + 1.0
    weights = weights / weights.sum()

    orders = []
    order_seq = 0
    seen_count = {cid: 0 for cid in cust_ids}
    for _ in range(n_orders):
        cid = cust_ids[int(rng.choice(len(cust_ids), p=weights))]
        order_seq += 1
        # orders cluster in business hours, weekday-weighted
        day_off = int(rng.integers(0, WINDOW_DAYS))
        base_day = (START + dt.timedelta(days=day_off)).date()
        hour = int(np.clip(rng.normal(14, 4), 0, 23))
        ts = dt.datetime.combine(base_day, dt.time(hour, int(rng.integers(0, 60)), int(rng.integers(0, 60))))

        n_items = int(rng.integers(1, 5))
        items = []
        for k in range(n_items):
            cat = pyrng.choices(CATEGORIES, weights=[28, 20, 20, 18, 14])[0]
            unit = int(max(199, rng.lognormal(8.4, 0.7)))
            items.append(
                {
                    "sku": f"{cat[:3].upper()}-{int(rng.integers(1000, 9999))}",
                    "category": cat,
                    "qty": int(rng.integers(1, 4)),
                    "unit_price_cents": unit,
                }
            )
        total = sum(it["qty"] * it["unit_price_cents"] for it in items)
        prior = seen_count[cid]
        seen_count[cid] += 1

        orders.append(
            {
                "order_id": f"ORD-{order_seq:08d}",
                "customer_id": cid,
                "order_ts": iso(ts),
                "_ts": ts,
                "channel": pyrng.choices(CHANNELS, weights=[52, 40, 8])[0],
                "payment_method": pyrng.choices(METHODS, weights=[60, 25, 12, 3])[0],
                "shipping_speed": pyrng.choices(["standard", "express"], weights=[78, 22])[0],
                "order_total_cents": total,
                "items": items,
                "_prior_orders": prior,
                "_top_cat": max(items, key=lambda x: x["qty"] * x["unit_price_cents"])["category"],
            }
        )

    # ---- latent refund process -------------------------------------------
    totals = np.array([o["order_total_cents"] for o in orders], dtype=float)
    z_total = (np.log1p(totals) - np.log1p(totals).mean()) / np.log1p(totals).std()

    for i, o in enumerate(orders):
        logit = (
            -2.35
            + 0.78 * z_total[i]
            + 0.55 * (o["channel"] == "mobile")
            + 0.85 * (o["_top_cat"] == "apparel")
            + 0.30 * (o["shipping_speed"] == "express")
            - 0.45 * min(o["_prior_orders"], 6) * 0.25
            + float(rng.normal(0, 0.35))
        )
        p = sigmoid(logit)
        refunded = bool(rng.random() < p)
        o["_true_refund"] = refunded
        if refunded:
            lag = float(np.clip(rng.exponential(11.0), 0.4, 29.9))
            o["_refund_ts"] = o["_ts"] + dt.timedelta(days=lag)
        else:
            o["_refund_ts"] = None
        # a leaky operational signal that correlates with the outcome
        base_tickets = int(rng.poisson(0.25))
        o["_support_tickets"] = base_tickets + (int(rng.poisson(1.6)) if refunded else 0)

    # ---- payments ---------------------------------------------------------
    payments = []
    pay_seq = 0
    for o in orders:
        if rng.random() < 0.965:  # a few orders legitimately have no payment row
            pay_seq += 1
            paid = o["_ts"] + dt.timedelta(seconds=int(rng.integers(5, 900)))
            payments.append(
                {
                    "payment_id": f"PAY-{pay_seq:08d}",
                    "order_id": o["order_id"],
                    "paid_ts": iso(paid),
                    "amount_cents": o["order_total_cents"],
                    "status": "captured" if rng.random() < 0.97 else "pending",
                    "processor": pyrng.choices(PROCESSORS, weights=[55, 30, 15])[0],
                }
            )

    # ---- web events -------------------------------------------------------
    events = []
    ev_seq = 0
    n_events = max(1000, int(60000 * scale))
    for _ in range(n_events):
        cid = cust_ids[int(rng.choice(len(cust_ids), p=weights))]
        day_off = int(rng.integers(0, WINDOW_DAYS))
        base_day = (START + dt.timedelta(days=day_off)).date()
        ts = dt.datetime.combine(
            base_day,
            dt.time(int(np.clip(rng.normal(15, 5), 0, 23)), int(rng.integers(0, 60)), int(rng.integers(0, 60))),
        )
        ev_seq += 1
        events.append(
            {
                "event_id": f"EV-{ev_seq:09d}",
                "customer_id": cid,
                "session_id": f"S-{int(rng.integers(1, 10**7)):08d}",
                "ts": iso(ts),
                "_ts": ts,
                "_partition": base_day,
                "event_type": pyrng.choices(EVENT_TYPES, weights=[55, 18, 14, 6, 7])[0],
                "sku": f"{pyrng.choice(CATEGORIES)[:3].upper()}-{int(rng.integers(1000, 9999))}",
            }
        )

    return {"customers": customers, "orders": orders, "payments": payments, "events": events}


# --------------------------------------------------------------------------
# Defect library
# --------------------------------------------------------------------------

CATALOG = {
    "dup_exact_payments": ("ingestion", "Write-retry produced byte-identical duplicate payment rows."),
    "dup_near_orders": ("ingestion", "Same basket submitted twice seconds apart under different order_ids."),
    "mojibake_city": ("ingestion", "UTF-8 text decoded as latin-1 somewhere upstream."),
    "id_leading_zeros": ("ingestion", "A join key was round-tripped through an integer type."),
    "field_rename_midstream": ("ingestion", "A JSON field changed name partway through the file."),
    "tz_mixed": ("time", "Timestamps stop being naive and start carrying offsets (or vice versa)."),
    "late_arriving_events": ("time", "Events for day D physically stored in a later partition."),
    "dst_duplicate_hour": ("time", "A wall-clock hour repeats / is missing around a DST boundary."),
    "unit_switch_price": ("semantic", "A money column silently changes unit at a date boundary."),
    "category_rename": ("semantic", "A categorical value is renamed mid-stream, splitting one class in two."),
    "new_category_late": ("semantic", "A category value that exists only in the most recent period."),
    "sentinel_values": ("semantic", "Magic values standing in for NULL (-999, 1900-01-01, 'N/A', 'null')."),
    "covariate_shift": ("statistical", "Feature distribution differs between early and late periods."),
    "mnar_missing": ("statistical", "Missingness in a column depends on the outcome itself."),
    "leakage_column": ("leakage", "A column present at training time that is unavailable at prediction time."),
    "survivorship_customers": ("integrity", "Rows hard-deleted from a dimension, orphaning historical facts."),
    "label_censoring": ("statistical", "Labels near the window edge cannot have matured; absence != negative."),
}

ALWAYS_ON = ["label_censoring"]
SAMPLEABLE = [k for k in CATALOG if k not in ALWAYS_ON]


class Planter:
    """Applies defects to the world and records what it did."""

    def __init__(self, world, rng, pyrng):
        self.w = world
        self.rng = rng
        self.pyrng = pyrng
        self.notes: dict[str, dict] = {}
        # rendering flags consumed by the writers
        self.flags = {
            "orders_rename_after": None,
            "orders_rename_from": "channel",
            "orders_rename_to": "order_channel",
            "price_unit_switch_after": None,
            "tz_offset_after": None,
            "leak_columns": False,
            "payments_as_int_ids": False,
        }

    # -- ingestion ---------------------------------------------------------
    def dup_exact_payments(self):
        pays = self.w["payments"]
        n = max(5, int(len(pays) * 0.012))
        picks = self.rng.choice(len(pays), size=n, replace=False)
        for i in picks:
            pays.append(dict(pays[int(i)]))
        self.pyrng.shuffle(pays)
        return {"n_duplicated_rows": n, "table": "payments"}

    def dup_near_orders(self):
        orders = self.w["orders"]
        n = max(4, int(len(orders) * 0.006))
        picks = self.rng.choice(len(orders), size=n, replace=False)
        next_seq = max(int(o["order_id"].split("-")[1]) for o in orders) + 1
        added = []
        for j, i in enumerate(picks):
            src = self.w["orders"][int(i)]
            twin = json.loads(json.dumps({k: v for k, v in src.items() if not k.startswith("_")}))
            twin["order_id"] = f"ORD-{next_seq + j:08d}"
            twin["_ts"] = src["_ts"] + dt.timedelta(seconds=int(self.rng.integers(3, 40)))
            twin["order_ts"] = iso(twin["_ts"])
            twin["_true_refund"] = src["_true_refund"]
            twin["_refund_ts"] = src["_refund_ts"]
            twin["_support_tickets"] = src["_support_tickets"]
            twin["_top_cat"] = src["_top_cat"]
            twin["_prior_orders"] = src["_prior_orders"]
            added.append(twin)
        orders.extend(added)
        orders.sort(key=lambda o: o["_ts"])
        return {"n_twin_orders": n, "table": "orders",
                "detail": "same customer + same basket, seconds apart, fresh order_id"}

    def mojibake_city(self):
        hit = 0
        for c in self.w["customers"]:
            if any(ord(ch) > 127 for ch in c["city"]) and self.rng.random() < 0.62:
                c["city"] = c["city"].encode("utf-8").decode("latin-1")
                hit += 1
        return {"n_rows": hit, "table": "customers", "column": "city"}

    def id_leading_zeros(self):
        self.flags["payments_as_int_ids"] = True
        return {"table": "payments", "column": "customer_id_int", "detail": "customer_id emitted as int64"}

    def field_rename_midstream(self):
        cut = START + dt.timedelta(days=int(self.rng.integers(70, 130)))
        self.flags["orders_rename_after"] = cut
        return {"table": "orders", "from": "channel", "to": "order_channel", "after": cut.date().isoformat()}

    # -- time --------------------------------------------------------------
    def tz_mixed(self):
        cut = START + dt.timedelta(days=int(self.rng.integers(60, 140)))
        self.flags["tz_offset_after"] = cut
        return {"table": "orders", "column": "order_ts", "after": cut.date().isoformat(), "offset": "+00:00"}

    def late_arriving_events(self):
        n = 0
        for e in self.w["events"]:
            if self.rng.random() < 0.02:
                e["_partition"] = e["_partition"] + dt.timedelta(days=int(self.rng.integers(1, 3)))
                n += 1
        return {"table": "web_events", "n_events_misfiled": n, "detail": "partition dt > event ts date"}

    def dst_duplicate_hour(self):
        # 2025-03-30 02:00 Europe: clocks jump forward -> 02:xx should not exist
        target = dt.date(2025, 3, 30)
        n = 0
        for o in self.w["orders"]:
            if o["_ts"].date() == target and self.rng.random() < 0.5:
                o["_ts"] = o["_ts"].replace(hour=2)
                o["order_ts"] = iso(o["_ts"])
                n += 1
        return {"table": "orders", "date": target.isoformat(), "n_rows": n, "detail": "wall times inside DST gap"}

    # -- semantic ----------------------------------------------------------
    def unit_switch_price(self):
        cut = START + dt.timedelta(days=int(self.rng.integers(90, 150)))
        self.flags["price_unit_switch_after"] = cut
        return {
            "table": "orders",
            "column": "order_total_cents",
            "after": cut.date().isoformat(),
            "detail": "integer cents -> float dollars, column name unchanged",
        }

    def category_rename(self):
        n = 0
        for c in self.w["customers"]:
            if c["country"] == "US" and self.rng.random() < 0.45:
                c["country"] = "USA"
                n += 1
        return {"table": "customers", "column": "country", "map": "US -> USA", "n_rows": n}

    def new_category_late(self):
        cut = CUTOFF - dt.timedelta(days=35)
        n = 0
        for o in self.w["orders"]:
            if o["_ts"] > cut and self.rng.random() < 0.18:
                o["payment_method"] = "bnpl"
                n += 1
        return {"table": "orders", "column": "payment_method", "value": "bnpl", "after": cut.date().isoformat(), "n_rows": n}

    def sentinel_values(self):
        counts = {}
        for c in self.w["customers"]:
            r = self.rng.random()
            if r < 0.04:
                c["lifetime_value_cents"] = -999
                counts["lifetime_value_cents=-999"] = counts.get("lifetime_value_cents=-999", 0) + 1
            elif r < 0.06:
                c["signup_date"] = "1900-01-01"
                counts["signup_date=1900-01-01"] = counts.get("signup_date=1900-01-01", 0) + 1
            elif r < 0.085:
                c["city"] = self.pyrng.choice(["N/A", "", "null", "NULL", "-"])
                counts["city=sentinel"] = counts.get("city=sentinel", 0) + 1
        return {"table": "customers", "counts": counts}

    # -- statistical -------------------------------------------------------
    def covariate_shift(self):
        cut = CUTOFF - dt.timedelta(days=45)
        n = 0
        for o in self.w["orders"]:
            if o["_ts"] > cut:
                factor = 1.0 + 0.55 * float(self.rng.random())
                o["order_total_cents"] = int(o["order_total_cents"] * factor)
                for it in o["items"]:
                    it["unit_price_cents"] = int(it["unit_price_cents"] * factor)
                n += 1
        return {"table": "orders", "column": "order_total_cents", "after": cut.date().isoformat(),
                "n_rows": n, "detail": "basket sizes inflate in the recent period"}

    def mnar_missing(self):
        n = 0
        for o in self.w["orders"]:
            p = 0.22 if o["_true_refund"] else 0.03
            if self.rng.random() < p:
                o["shipping_speed"] = None
                n += 1
        return {"table": "orders", "column": "shipping_speed", "n_missing": n,
                "detail": "P(missing) is ~7x higher for refunded orders"}

    # -- leakage / integrity ----------------------------------------------
    def leakage_column(self):
        self.flags["leak_columns"] = True
        return {"table": "orders", "columns": ["refund_processed_at", "support_ticket_count"],
                "detail": "both are populated after the prediction moment"}

    def survivorship_customers(self):
        custs = self.w["customers"]
        n = max(3, int(len(custs) * 0.03))
        picks = set(int(i) for i in self.rng.choice(len(custs), size=n, replace=False))
        removed = [custs[i]["customer_id"] for i in picks]
        self.w["customers"] = [c for i, c in enumerate(custs) if i not in picks]
        return {"table": "customers", "n_removed": n,
                "detail": "orders/events still reference these customer_ids"}

    def label_censoring(self):
        return {"table": "labels", "detail": "orders after %s cannot have a matured 30d label"
                % (CUTOFF - dt.timedelta(days=30)).date().isoformat()}


# --------------------------------------------------------------------------
# Writers
# --------------------------------------------------------------------------


def write_customers(world, out: Path):
    cols = ["customer_id", "signup_date", "country", "city", "segment", "lifetime_value_cents", "is_active"]
    with open(out / "customers.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for c in world["customers"]:
            w.writerow({k: c[k] for k in cols})


def write_orders(world, out: Path, flags):
    with open(out / "orders.jsonl", "w", encoding="utf-8") as f:
        for o in sorted(world["orders"], key=lambda x: x["_ts"]):
            ts = o["_ts"]
            ts_str = iso(ts)
            if flags["tz_offset_after"] and ts > flags["tz_offset_after"]:
                ts_str = ts_str + "+00:00"

            total = o["order_total_cents"]
            if flags["price_unit_switch_after"] and ts > flags["price_unit_switch_after"]:
                total = round(total / 100.0, 2)

            rec = {
                "order_id": o["order_id"],
                "customer_id": o["customer_id"],
                "order_ts": ts_str,
                "payment_method": o["payment_method"],
                "shipping_speed": o["shipping_speed"],
                "order_total_cents": total,
                "items": o["items"],
            }
            ch_key = "channel"
            if flags["orders_rename_after"] and ts > flags["orders_rename_after"]:
                ch_key = flags["orders_rename_to"]
            rec[ch_key] = o["channel"]

            if flags["leak_columns"]:
                rec["refund_processed_at"] = iso(o["_refund_ts"]) if o["_refund_ts"] else None
                rec["support_ticket_count"] = o["_support_tickets"]

            f.write(json.dumps(rec, ensure_ascii=False) + "\n")


def write_payments(world, out: Path, flags) -> str:
    rows = []
    cust_of = {o["order_id"]: o["customer_id"] for o in world["orders"]}
    for p in world["payments"]:
        r = dict(p)
        cid = cust_of.get(p["order_id"], "0000000")
        if flags["payments_as_int_ids"]:
            r["customer_id"] = int(cid)
        else:
            r["customer_id"] = cid
        rows.append(r)

    cols = ["payment_id", "order_id", "customer_id", "paid_ts", "amount_cents", "status", "processor"]
    try:
        import pyarrow as pa  # noqa
        import pyarrow.parquet as pq

        table = pa.Table.from_pylist([{k: r[k] for k in cols} for r in rows])
        pq.write_table(table, out / "payments.parquet")
        return "payments.parquet"
    except Exception:
        with open(out / "payments.csv", "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=cols)
            w.writeheader()
            for r in rows:
                w.writerow({k: r[k] for k in cols})
        return "payments.csv"


def write_events(world, out: Path):
    root = out / "web_events"
    buckets: dict[dt.date, list] = {}
    for e in world["events"]:
        buckets.setdefault(e["_partition"], []).append(e)
    for day, evs in sorted(buckets.items()):
        d = root / f"dt={day.isoformat()}"
        d.mkdir(parents=True, exist_ok=True)
        # split across a couple of part files, like a real sink would
        chunks = min(4, 1 + len(evs) // 150)
        size = math.ceil(len(evs) / chunks)
        for ci in range(chunks):
            part = evs[ci * size : (ci + 1) * size]
            if not part:
                continue
            with gzip.open(d / f"part-{ci:05d}.jsonl.gz", "wt", encoding="utf-8") as f:
                for e in part:
                    f.write(
                        json.dumps(
                            {
                                "event_id": e["event_id"],
                                "customer_id": e["customer_id"],
                                "session_id": e["session_id"],
                                "ts": e["ts"],
                                "event_type": e["event_type"],
                                "sku": e["sku"],
                            }
                        )
                        + "\n"
                    )


def write_labels(world, out: Path):
    """Emitted by 'the upstream job': a refund is only recorded if it was
    observed before the extract ran. Orders near the edge look clean."""
    with open(out / "labels.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["order_id", "refunded_within_30d"])
        for o in sorted(world["orders"], key=lambda x: x["_ts"]):
            observed = o["_refund_ts"] is not None and o["_refund_ts"] <= CUTOFF
            w.writerow([o["order_id"], int(observed)])


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed", type=int, required=True)
    ap.add_argument("--out", default="data")
    ap.add_argument("--scale", type=float, default=1.0)
    ap.add_argument("--n-defects", type=int, default=None, help="default: 4-6 chosen by seed")
    args = ap.parse_args()

    rng = np.random.default_rng(args.seed)
    pyrng = random.Random(args.seed ^ 0x5EED)

    out = Path(args.out) / f"seed_{args.seed}"
    if out.exists():
        shutil.rmtree(out)
    out.mkdir(parents=True)

    world = build_world(rng, pyrng, args.scale)

    n_def = args.n_defects if args.n_defects is not None else int(rng.integers(4, 7))
    chosen = list(pyrng.sample(SAMPLEABLE, k=min(n_def, len(SAMPLEABLE))))
    planted = ALWAYS_ON + chosen

    p = Planter(world, rng, pyrng)
    manifest_entries = []
    for name in planted:
        params = getattr(p, name)()
        manifest_entries.append(
            {"id": name, "family": CATALOG[name][0], "description": CATALOG[name][1], "params": params}
        )

    write_customers(world, out)
    write_orders(world, out, p.flags)
    pay_file = write_payments(world, out, p.flags)
    write_events(world, out)
    write_labels(world, out)

    manifest = {
        "seed": args.seed,
        "scale": args.scale,
        "n_planted": len(manifest_entries),
        "defects": manifest_entries,
    }
    blob = base64.b64encode(zlib.compress(json.dumps(manifest).encode())).decode()
    (out / ".manifest.b64").write_text(
        "# SEALED ANSWER KEY -- do not decode by hand. Use: python grade.py --seed %d ...\n%s\n"
        % (args.seed, blob)
    )

    n_files = sum(1 for _ in out.rglob("*") if _.is_file())
    size = sum(f.stat().st_size for f in out.rglob("*") if f.is_file())
    print(f"wrote {out}")
    print(f"  customers.csv     {len(world['customers']):>7,} rows")
    print(f"  orders.jsonl      {len(world['orders']):>7,} rows")
    print(f"  {pay_file:<17} {len(world['payments']):>7,} rows")
    print(f"  web_events/       {len(world['events']):>7,} events")
    print(f"  labels.csv        {len(world['orders']):>7,} rows")
    print(f"  {n_files} files, {size/1e6:.1f} MB total")
    print(f"  {len(manifest_entries)} defects planted (sealed)")


if __name__ == "__main__":
    main()
