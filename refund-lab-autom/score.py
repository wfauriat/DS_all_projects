#!/usr/bin/env python
"""Serve the refund-risk score.

    python score.py ORD-00012345          one order, with the reasoning inputs
    python score.py --day 2027-04-02      every order placed that day
    python score.py --summary             calibration of the live model

The score comes from `predictions`, written when the order was ingested, by the
model that was current *at that moment*.  Re-scoring an old order with today's
model would answer a different question, so this deliberately reads back the
score as it was served rather than recomputing it.
"""
from __future__ import annotations

import argparse

from pipeline import storage, util


def one(st, order_id):
    o = st.q("SELECT * FROM fct_orders WHERE order_id=?", (order_id,))
    if not o:
        print(f"no such order: {order_id}")
        return
    o = dict(o[0])
    p = st.q("SELECT * FROM predictions WHERE order_id=? ORDER BY scored_at_sim DESC", (order_id,))
    print(f"order        {o['order_id']}  placed {o['occurred_at']}  customer {o['customer_id']}")
    print(f"basket       {o['n_items']} lines / {o['n_units']} units, "
          f"{(o['order_total_cents'] or 0) / 100:.2f} EUR, top category {o['top_category']}")
    print(f"channel      {o['channel']} / {o['payment_method']} / {o['shipping_speed']}")
    print(f"customer     {o['country']} / {o['segment']}")
    print(f"payment      {o['processor'] or '-'} {o['payment_status'] or '-'}"
          f"  settle lag {o['settle_lag_days']}")
    if p:
        for r in p:
            print(f"RISK         p(refund within 30d) = {r['p_refund']:.4f}   "
                  f"model {r['model_version']}, scored at {r['scored_at_sim']}")
    else:
        print("RISK         not scored (no model was live when this order arrived)")
    print(f"window closes {o['window_closed_at']}")
    if o["has_label"]:
        print(f"OUTCOME      refunded={o['refunded']} known at {o['label_knowledge_time']}")
    else:
        print("OUTCOME      undetermined -- the refund window has not closed, so the "
              "absence of a label is not a negative")


def day(st, d):
    rows = st.q("SELECT o.order_id, o.occurred_at, o.order_total_cents, p.p_refund, "
                "p.model_version, o.has_label, o.refunded FROM fct_orders o "
                "LEFT JOIN predictions p USING(order_id) WHERE o.order_day=? "
                "ORDER BY p.p_refund DESC NULLS LAST", (d,))
    print(f"{len(rows)} orders on {d}")
    print(f"{'order_id':<16}{'placed':<21}{'total':>10}{'p_refund':>10}  outcome")
    for r in rows:
        out = ("refunded" if r["refunded"] == 1 else "kept") if r["has_label"] else "undetermined"
        pr = f"{r['p_refund']:.4f}" if r["p_refund"] is not None else "-"
        print(f"{r['order_id']:<16}{r['occurred_at']:<21}"
              f"{(r['order_total_cents'] or 0) / 100:>10.2f}{pr:>10}  {out}")


def summary(st):
    for r in st.q("SELECT * FROM model_runs ORDER BY trained_at_sim"):
        print(f"{r['model_version']:<14} trained {r['trained_at_sim'][:10]}  "
              f"n={r['n_train']:<7} features={r['n_features']:<3} "
              f"holdout AUC={r['auc_holdout'] if r['auc_holdout'] is None else round(r['auc_holdout'], 3)}  "
              f"base rate={r['base_rate']:.4f}  {r['notes']}")
    print(f"\n{st.q1('SELECT COUNT(*) FROM predictions')} orders scored")
    print("\ndecile calibration over orders whose refund window has closed:")
    rows = st.q("SELECT p.p_refund, o.refunded FROM predictions p JOIN fct_orders o "
                "USING(order_id) WHERE o.has_label=1 AND o.refunded IS NOT NULL")
    if len(rows) < 100:
        print("  not enough matured, scored orders yet")
        return
    vals = sorted((r["p_refund"], r["refunded"]) for r in rows)
    n = len(vals)
    print(f"  {'decile':<8}{'n':>7}{'mean p':>10}{'actual':>10}")
    for i in range(10):
        chunk = vals[i * n // 10:(i + 1) * n // 10]
        if not chunk:
            continue
        mp = sum(c[0] for c in chunk) / len(chunk)
        ac = sum(c[1] for c in chunk) / len(chunk)
        print(f"  {i + 1:<8}{len(chunk):>7}{mp:>10.4f}{ac:>10.4f}")
    from pipeline.monitors import auc
    a = auc([c[0] for c in vals], [int(c[1]) for c in vals])
    print(f"\n  AUC over all matured scored orders: {a:.4f}  "
          f"(base rate {sum(c[1] for c in vals) / n:.4f})")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("order_id", nargs="?")
    ap.add_argument("--day")
    ap.add_argument("--summary", action="store_true")
    a = ap.parse_args()
    st = storage.Store()
    if a.summary:
        summary(st)
    elif a.day:
        day(st, a.day)
    elif a.order_id:
        one(st, a.order_id)
    else:
        summary(st)


if __name__ == "__main__":
    main()
