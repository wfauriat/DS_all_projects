#!/usr/bin/env python
"""Reconstruct what the source said on a past date.

    python asof.py order ORD-00012345 [--as-of 2027-01-05T00:00:00]
    python asof.py day   2027-01-05 --entity orders --as-of 2027-01-20T00:00:00
    python asof.py diff                 find keys whose story changed, and show both

The warehouse keeps every version the source ever served, so a belief query is
just ``knowledge_time <= X`` with the highest version that satisfies it.  No
part of this reads the source; it is answered entirely from ``src_*``.
"""
from __future__ import annotations

import argparse
import json

from pipeline import storage, util


def show(rows, cols=None):
    for r in rows:
        d = dict(r)
        if cols:
            d = {k: d[k] for k in cols if k in d}
        print(json.dumps(d, ensure_ascii=False, default=str))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["order", "day", "diff", "customer"])
    ap.add_argument("key", nargs="?")
    ap.add_argument("--as-of", dest="as_of")
    ap.add_argument("--entity", default="orders")
    ap.add_argument("--limit", type=int, default=10)
    a = ap.parse_args()
    st = storage.Store()
    now = a.as_of or st.get_state("last_sim_now") or "9999-01-01T00:00:00"

    if a.mode == "order":
        rows = st.belief_as_of("orders", now, key=a.key)
        print(f"# orders {a.key} as believed on {now}")
        show(rows, ["order_id", "version", "knowledge_time", "occurred_at", "customer_id",
                    "order_total_cents", "channel", "payment_method", "n_units"])
    elif a.mode == "customer":
        rows = st.belief_as_of("customers", now, key=a.key)
        print(f"# customer {a.key} as believed on {now}")
        show(rows, ["customer_id", "valid_from", "valid_to", "version", "knowledge_time",
                    "country", "city", "segment", "is_active"])
    elif a.mode == "day":
        rows = st.belief_as_of(a.entity, now, day=a.key)
        print(f"# {a.entity} for valid day {a.key}, as believed on {now}: {len(rows)} rows")
        show(rows[:a.limit])
    else:
        # every business key the source told us two different stories about
        print("# keys whose recorded history changed after we first stored it")
        for table, key in (("src_orders", "order_id"), ("src_labels", "order_id"),
                           ("src_customers", "customer_id"), ("src_events", "event_id")):
            rows = st.q(f"SELECT {key} k, COUNT(*) n, MIN(version) v0, MAX(version) v1, "
                        f"MIN(knowledge_time) k0, MAX(knowledge_time) k1 FROM {table} "
                        f"GROUP BY {key} HAVING MAX(version) > MIN(version) "
                        f"ORDER BY k1 DESC LIMIT ?", (a.limit,))
            print(f"\n## {table}: "
                  f"{st.q1(f'SELECT COUNT(*) FROM (SELECT {key} FROM {table} GROUP BY {key} '
                           f'HAVING MAX(version) > MIN(version))')} restated keys")
            for r in rows:
                print(f"  {r['k']}: v{r['v0']} known {r['k0']} -> v{r['v1']} known {r['k1']}")
                before = st.belief_as_of(table.replace("src_", ""), r["k0"], key=r["k"])
                after = st.belief_as_of(table.replace("src_", ""), r["k1"], key=r["k"])
                if before and after:
                    b, af = dict(before[0]), dict(after[0])
                    diff = {k: (b.get(k), af.get(k)) for k in b
                            if k not in ("first_seen_sim", "cycle_id", "row_hash", "version",
                                         "knowledge_time") and b.get(k) != af.get(k)}
                    print(f"     changed: {diff}")


if __name__ == "__main__":
    main()
