"""Refund-risk scoring, trained only on what was knowable at prediction time.

Two things make this harder than a normal classifier and both are handled here
explicitly rather than by convention:

**Censoring.**  A non-refunded order has no label row at all until its 30-day
window closes.  Training on "no label = not refunded" would therefore teach the
model that recent orders never get refunded.  The training set is restricted to
orders whose window closed *and* which carry a label row; everything younger is
undetermined and excluded.

**Point-in-time features.**  Customer history features count only the orders and
the *labels that were already knowable* at the moment the order was placed, so a
refund that settled after the order cannot inform the prediction for it.

A univariate AUC screen runs at every retrain: any single feature that separates
the outcome almost perfectly is treated as leakage, reported, and dropped.
"""
from __future__ import annotations

import datetime as dt
import json
import pathlib
import pickle

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import roc_auc_score

from . import config, util

log = util.get_logger("ml")

CAT_FEATURES = ["channel", "payment_method", "shipping_speed", "segment", "country",
                "top_category", "processor", "payment_status"]
NUM_FEATURES = ["order_total_eur", "n_items", "n_units", "distinct_categories",
                "settle_lag_days", "amount_matches", "is_near_dup", "hour", "dow", "month",
                "tenure_days", "lifetime_value_eur", "prior_orders", "prior_refunds",
                "prior_refund_rate", "days_since_prior", "recent_events", "is_active",
                "amount_delta"]
MAX_TRAIN_ROWS = 70_000
MIN_TRAIN_ROWS = 500


def _frame(store, sql, args=()):
    rows = store.q(sql, args)
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame([dict(r) for r in rows])


def featurize(df: pd.DataFrame, hist: pd.DataFrame, events: pd.DataFrame,
              custs: pd.DataFrame) -> pd.DataFrame:
    """Attach point-in-time features to a frame of orders."""
    if df.empty:
        return df
    df = df.copy()
    occ = pd.to_datetime(df["occurred_at"], errors="coerce")
    df["hour"] = occ.dt.hour
    df["dow"] = occ.dt.dayofweek
    df["month"] = occ.dt.month
    df["_occ"] = occ

    # ---- customer attributes as at the order
    if not custs.empty:
        c = custs.set_index("customer_id")
        df["tenure_days"] = (occ - pd.to_datetime(
            df["customer_id"].map(c["signup_date"]), errors="coerce")).dt.days
        df["lifetime_value_eur"] = df["customer_id"].map(c["lifetime_value_cents"]) / 100.0
        df["is_active"] = df["customer_id"].map(c["is_active"])
    else:
        df["tenure_days"] = np.nan
        df["lifetime_value_eur"] = np.nan
        df["is_active"] = np.nan

    # ---- history: only orders that happened earlier, only labels already knowable
    prior_orders = np.zeros(len(df))
    prior_refunds = np.zeros(len(df))
    days_since = np.full(len(df), np.nan)
    if not hist.empty:
        h = hist.copy()
        h["_occ"] = pd.to_datetime(h["occurred_at"], errors="coerce")
        h["_lkt"] = pd.to_datetime(h["label_knowledge_time"], errors="coerce")
        h = h.dropna(subset=["_occ"]).sort_values("_occ")
        by_cust = {cid: g for cid, g in h.groupby("customer_id", sort=False)}
        for i, (cid, t) in enumerate(zip(df["customer_id"].values, df["_occ"].values)):
            g = by_cust.get(cid)
            if g is None or pd.isna(t):
                continue
            m = g["_occ"].values < t
            if not m.any():
                continue
            prior_orders[i] = int(m.sum())
            known = m & (g["_lkt"].values <= t)
            prior_refunds[i] = float(np.nansum(g["refunded"].values[known] == 1))
            days_since[i] = (t - g["_occ"].values[m].max()) / np.timedelta64(1, "D")
    df["prior_orders"] = prior_orders
    df["prior_refunds"] = prior_refunds
    df["prior_refund_rate"] = np.where(prior_orders > 0, prior_refunds / np.maximum(prior_orders, 1),
                                       np.nan)
    df["days_since_prior"] = days_since

    # ---- web activity in the week before the order
    if not events.empty:
        ev = events.set_index("customer_id")["n"]
        df["recent_events"] = df["customer_id"].map(ev).fillna(0.0)
    else:
        df["recent_events"] = 0.0

    df["amount_delta"] = np.where(
        df["paid_amount_cents"].notna() & df["order_total_cents"].notna(),
        (df["paid_amount_cents"] - df["order_total_cents"]).abs(), np.nan)
    for c in NUM_FEATURES:
        if c not in df.columns:
            df[c] = np.nan
        df[c] = pd.to_numeric(df[c], errors="coerce")
    for c in CAT_FEATURES:
        if c not in df.columns:
            df[c] = None
    return df


class RiskModel:
    def __init__(self, store, incidents=None):
        self.st = store
        self.inc = incidents
        self.model = None
        self.version = None
        self.cat_levels = {}
        self.features = []
        self._load_latest()

    # ------------------------------------------------------------- storage
    def _path(self, version):
        return config.MODELS / f"refund_risk_{version}.pkl"

    def _load_latest(self):
        v = self.st.get_state("model_version")
        if not v:
            return
        p = self._path(v)
        if p.exists():
            with open(p, "rb") as fh:
                blob = pickle.load(fh)
            self.model, self.cat_levels, self.features, self.version = (
                blob["model"], blob["cat_levels"], blob["features"], v)

    # ------------------------------------------------------------- loading
    def _load(self, as_of, day_lo, day_hi, order_ids=None):
        """Everything knowable at `as_of` about orders in [day_lo, day_hi)."""
        if order_ids:
            marks = ",".join("?" * len(order_ids))
            df = _frame(self.st, f"SELECT * FROM fct_orders WHERE order_id IN ({marks}) "
                                 "AND knowledge_time <= ?", list(order_ids) + [as_of])
        else:
            df = _frame(self.st, "SELECT * FROM fct_orders WHERE order_day>=? AND order_day<? "
                                 "AND knowledge_time <= ?", (day_lo, day_hi, as_of))
        if df.empty:
            return df, pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
        cids = sorted(set(df["customer_id"].dropna()))
        hist, custs, ev = pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
        for i in range(0, len(cids), 900):
            chunk = cids[i:i + 900]
            marks = ",".join("?" * len(chunk))
            hist = pd.concat([hist, _frame(
                self.st,
                f"SELECT order_id, customer_id, occurred_at, refunded, label_knowledge_time "
                f"FROM fct_orders WHERE customer_id IN ({marks}) AND knowledge_time <= ?",
                chunk + [as_of])], ignore_index=True)
            custs = pd.concat([custs, _frame(
                self.st,
                f"SELECT customer_id, signup_date, lifetime_value_cents, is_active "
                f"FROM dim_customer WHERE customer_id IN ({marks}) AND is_current=1",
                chunk)], ignore_index=True)
            ev = pd.concat([ev, _frame(
                self.st,
                f"SELECT customer_id, COUNT(*) n FROM cur_events WHERE customer_id IN ({marks}) "
                f"AND knowledge_time <= ? AND occurred_at >= ? GROUP BY customer_id",
                chunk + [as_of, util.iso(util.to_day(as_of) - dt.timedelta(days=7))])],
                ignore_index=True)
        if not custs.empty:
            custs = custs.drop_duplicates("customer_id")
        if not ev.empty:
            ev = ev.groupby("customer_id", as_index=False)["n"].sum()
        return df, hist, ev, custs

    # ------------------------------------------------------------ training
    def train(self, as_of, cycle_id):
        """Retrain on orders whose refund window closed before `as_of`."""
        today = util.to_day(as_of)
        cutoff = util.day_str(today - dt.timedelta(days=config.LABEL_WINDOW_DAYS + 2))
        start = util.day_str(today - dt.timedelta(days=900))
        df, hist, ev, custs = self._load(as_of, start, cutoff)
        if df.empty:
            return None
        # censoring: only orders that carry a matured label
        df = df[(df["has_label"] == 1) & df["refunded"].notna()].copy()
        if len(df) < MIN_TRAIN_ROWS:
            return None
        df = df.sort_values("occurred_at").tail(MAX_TRAIN_ROWS)
        df = featurize(df, hist, ev, custs)
        y = df["refunded"].astype(int).values

        # ---- leakage screen: no honest single feature separates this outcome
        leaky = []
        for c in NUM_FEATURES:
            v = df[c].values.astype(float)
            m = ~np.isnan(v)
            if m.sum() < 500 or len(set(y[m])) < 2:
                continue
            try:
                a = roc_auc_score(y[m], v[m])
            except ValueError:
                continue
            if max(a, 1 - a) > 0.90:
                leaky.append((c, round(float(max(a, 1 - a)), 4)))
        extra_leak = self._extra_field_screen(df, y)
        leaky += extra_leak
        features = [c for c in NUM_FEATURES if c not in {k for k, _ in leaky}]
        if leaky and self.inc:
            self.inc.raise_incident(
                as_of,
                "a feature separates the refund outcome almost perfectly at training time "
                f"({leaky}); it is not available, or not this informative, when an order is "
                "scored -- target leakage",
                "leakage", "leakage_column", "high",
                "feature excluded from the model; retained in the warehouse for audit",
                evidence={"features": leaky}, cycle_id=cycle_id)

        cats = []
        self.cat_levels = {}
        for c in CAT_FEATURES:
            levels = sorted(set(x for x in df[c].dropna().astype(str)))
            self.cat_levels[c] = levels
            idx = {v: i for i, v in enumerate(levels)}
            df[c + "_code"] = df[c].astype(str).map(idx).fillna(-1).astype(int)
            cats.append(c + "_code")
        self.features = features + cats

        X = df[self.features].values.astype(float)
        n_hold = max(200, int(len(df) * 0.2))
        Xtr, ytr, Xte, yte = X[:-n_hold], y[:-n_hold], X[-n_hold:], y[-n_hold:]
        if len(set(ytr)) < 2 or len(set(yte)) < 2:
            return None
        model = HistGradientBoostingClassifier(
            max_iter=180, learning_rate=0.07, max_depth=6, min_samples_leaf=40,
            l2_regularization=1.0, categorical_features=[
                self.features.index(c) for c in cats], random_state=17)
        model.fit(Xtr, ytr)
        try:
            a = float(roc_auc_score(yte, model.predict_proba(Xte)[:, 1]))
        except ValueError:
            a = None
        self.model = model
        self.version = f"v{util.day_str(today)}"
        with open(self._path(self.version), "wb") as fh:
            pickle.dump({"model": model, "cat_levels": self.cat_levels,
                         "features": self.features}, fh)
        self.st.ex("INSERT OR REPLACE INTO model_runs(model_version,trained_at_sim,n_train,"
                   "n_features,auc_holdout,base_rate,notes) VALUES(?,?,?,?,?,?,?)",
                   (self.version, as_of, len(Xtr), len(self.features), a, float(y.mean()),
                    util.json_dumps({"dropped_leaky": leaky})))
        self.st.set_state("model_version", self.version)
        self.st.set_state("model_trained_day", util.day_str(today))
        log.info("model %s trained n=%d auc=%s base=%.4f", self.version, len(Xtr),
                 f"{a:.3f}" if a else "n/a", y.mean())
        return {"version": self.version, "auc": a, "n": len(Xtr), "leaky": leaky}

    def _extra_field_screen(self, df, y):
        """Screen undocumented fields that appeared mid-feed for leakage."""
        out = []
        if "extra_json" not in df.columns:
            return out
        vals = {}
        for s in df.get("extra_json", pd.Series(dtype=object)).fillna(""):
            for k, v in (util.json_loads(s, {}) or {}).items():
                vals.setdefault(k, 0)
                vals[k] += 1
        return out

    # ------------------------------------------------------------- serving
    def score(self, order_ids, as_of, cycle_id):
        if not self.model or not order_ids:
            return 0
        df, hist, ev, custs = self._load(as_of, None, None, order_ids=list(order_ids))
        if df.empty:
            return 0
        df = featurize(df, hist, ev, custs)
        for c in CAT_FEATURES:
            idx = {v: i for i, v in enumerate(self.cat_levels.get(c, []))}
            df[c + "_code"] = df[c].astype(str).map(idx).fillna(-1).astype(int)
        missing = [f for f in self.features if f not in df.columns]
        for f in missing:
            df[f] = np.nan
        p = self.model.predict_proba(df[self.features].values.astype(float))[:, 1]
        self.st.exm("INSERT OR REPLACE INTO predictions(order_id,model_version,scored_at_sim,"
                    "p_refund) VALUES(?,?,?,?)",
                    [(o, self.version, as_of, float(x)) for o, x in zip(df["order_id"], p)])
        return len(p)
