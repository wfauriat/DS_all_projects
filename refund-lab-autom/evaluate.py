#!/usr/bin/env python
"""Evaluate the refund-risk scores prospectively.

Not the training holdout -- that is a self-report. This scores every order using
the number that was actually written when the order was ingested, by whichever
model was live at that moment, and compares it against the outcome that arrived
weeks later. No order contributes until its 30-day window has closed and a label
has been served, so nothing here is censored and nothing is scored by a model
that could have seen it.
"""
from __future__ import annotations

import datetime as dt

import numpy as np
import pandas as pd

from pipeline import storage, util


def auc(score, y):
    order = np.argsort(score, kind="mergesort")
    s, yy = np.asarray(score)[order], np.asarray(y)[order]
    n1 = yy.sum()
    n0 = len(yy) - n1
    if n0 == 0 or n1 == 0:
        return float("nan")
    ranks = np.empty(len(s), float)
    i = 0
    while i < len(s):
        j = i
        while j + 1 < len(s) and s[j + 1] == s[i]:
            j += 1
        ranks[i:j + 1] = (i + j) / 2.0 + 1
        i = j + 1
    return (ranks[yy == 1].sum() - n1 * (n1 + 1) / 2.0) / (n0 * n1)


def ece(p, y, bins=10):
    """Expected calibration error, equal-count bins."""
    idx = np.argsort(p)
    p, y = np.asarray(p)[idx], np.asarray(y)[idx]
    out, n = 0.0, len(p)
    for b in np.array_split(np.arange(n), bins):
        if len(b) == 0:
            continue
        out += len(b) / n * abs(p[b].mean() - y[b].mean())
    return out


def main():
    st = storage.Store()
    df = pd.DataFrame([dict(r) for r in st.q(
        "SELECT p.order_id, p.p_refund, p.model_version, p.scored_at_sim, "
        "o.order_day, o.occurred_at, o.refunded, o.has_label, o.window_closed_at, "
        "o.order_total_eur, o.channel, o.payment_method, o.segment, o.country, "
        "o.top_category, o.processor, o.n_units "
        "FROM predictions p JOIN fct_orders o USING(order_id)")])
    print(f"orders scored                {len(df):>8,}")
    dup = len(df) - df["order_id"].nunique()
    print(f"  duplicate score rows       {dup:>8,}")

    total_orders = st.q1("SELECT COUNT(*) FROM fct_orders")
    print(f"orders in the warehouse      {total_orders:>8,}")
    print(f"  unscored (no live model / not yet ingested when first model trained) "
          f"{total_orders - df['order_id'].nunique():,}")

    ev = df[(df["has_label"] == 1) & df["refunded"].notna()].copy()
    ev["y"] = ev["refunded"].astype(int)
    ev["p"] = ev["p_refund"].astype(float)
    print(f"\nscored AND matured           {len(ev):>8,}   "
          f"(the evaluable set; base rate {ev['y'].mean():.4f})")

    # --- headline, prospective
    a = auc(ev["p"].values, ev["y"].values)
    brier = float(((ev["p"] - ev["y"]) ** 2).mean())
    base = ev["y"].mean()
    brier_base = float(((base - ev["y"]) ** 2).mean())
    print(f"\n{'':<28}{'AUC':>8}{'Brier':>10}{'ECE':>8}")
    print(f"{'model, as served':<28}{a:>8.4f}{brier:>10.5f}{ece(ev['p'], ev['y']):>8.4f}")
    print(f"{'constant = base rate':<28}{0.5:>8.4f}{brier_base:>10.5f}"
          f"{abs(base - ev['y'].mean()):>8.4f}")
    print(f"{'skill vs constant (Brier)':<28}{'':>8}{1 - brier / brier_base:>9.1%}")

    # --- does it hold up across the horizon, or decay?
    print("\nby calendar year of the order (prospective, out of sample):")
    print(f"  {'year':<6}{'n':>8}{'base':>9}{'mean p':>9}{'AUC':>8}{'lift@10%':>10}")
    ev["year"] = ev["order_day"].str[:4]
    for y, g in ev.groupby("year"):
        if len(g) < 500:
            continue
        top = g.nlargest(max(1, len(g) // 10), "p")
        lift = (top["y"].mean() / g["y"].mean()) if g["y"].mean() else float("nan")
        print(f"  {y:<6}{len(g):>8,}{g['y'].mean():>9.4f}{g['p'].mean():>9.4f}"
              f"{auc(g['p'].values, g['y'].values):>8.4f}{lift:>10.2f}x")

    # --- calibration
    print("\ncalibration, equal-count deciles:")
    print(f"  {'decile':<8}{'n':>8}{'mean p':>10}{'actual':>10}{'gap':>9}")
    ev = ev.sort_values("p")
    for i, b in enumerate(np.array_split(np.arange(len(ev)), 10), 1):
        g = ev.iloc[b]
        print(f"  {i:<8}{len(g):>8,}{g['p'].mean():>10.4f}{g['y'].mean():>10.4f}"
              f"{g['p'].mean() - g['y'].mean():>+9.4f}")

    # --- is the ranking useful where it matters?
    print("\nranking usefulness (refunds captured by the riskiest orders):")
    ev2 = ev.sort_values("p", ascending=False)
    tot_ref = ev2["y"].sum()
    for frac in (0.05, 0.10, 0.20, 0.50):
        k = int(len(ev2) * frac)
        got = ev2.head(k)["y"].sum()
        print(f"  top {frac:>4.0%} of orders  ->  {got / tot_ref:>6.1%} of all refunds "
              f"({got:,} of {tot_ref:,}),  lift {got / k / ev2['y'].mean():.2f}x")

    # --- what a single obvious feature would have got, for scale
    print("\nsingle-feature baselines on the same set (for scale):")
    for col in ("order_total_eur", "n_units"):
        v = pd.to_numeric(ev[col], errors="coerce")
        m = v.notna()
        if m.sum() > 1000:
            aa = auc(v[m].values, ev["y"][m].values)
            print(f"  {col:<22}{max(aa, 1 - aa):>8.4f}")
    for col in ("payment_method", "channel", "segment", "top_category", "processor"):
        rate = ev.groupby(col)["y"].transform("mean")
        print(f"  {col + ' (target rate)':<22}{auc(rate.values, ev['y'].values):>8.4f}")

    # --- did retraining track the drifting base rate?
    print("\nmodel history (holdout AUC is the model's own report at train time):")
    for r in st.q("SELECT model_version, trained_at_sim, n_train, auc_holdout, base_rate "
                  "FROM model_runs ORDER BY trained_at_sim"):
        served = ev[ev["model_version"] == r["model_version"]]
        got = auc(served["p"].values, served["y"].values) if len(served) > 300 else float("nan")
        print(f"  {r['model_version']:<13} train n={r['n_train']:>6,} "
              f"base={r['base_rate']:.4f} holdout={r['auc_holdout'] or float('nan'):.3f}   "
              f"served n={len(served):>6,} realised AUC={got:.3f}")


if __name__ == "__main__":
    main()
