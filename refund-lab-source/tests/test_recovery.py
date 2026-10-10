"""Can the world's own statistics be recovered from the world's own data?

Everything else in this suite checks that faults appear and that clean data
stays clean.  Nothing checked that the structure the generator *claims* to
inject is actually there and measurable.  It wasn't: if the growth term
vanished from ``_day_weights`` tomorrow, or the refund logit lost a coefficient,
every other test would still pass and the exercise would quietly stop being an
exercise.

So this file grades the lab against itself, on a fault-free world at the
operator's real scale and full horizon.  No planted faults, no hostility, no
delivery layer -- just the generator's output measured against the constants it
was built from.

Tolerances are calibrated from the observed seed-to-seed spread, and two of them
are deliberately *not* the injected value:

* the weekend factor reads slightly low because the two big promo days always
  fall on a Friday and a Monday, which lifts the weekday side;
* the day-to-day shock has rho 0.88 in the latent process, but what a consumer
  can observe is attenuated to about half that by ordinary sampling noise.

Both are properties of the world, not errors, so they are asserted as measured.
"""

from __future__ import annotations

import collections
import datetime as dt
import random

import numpy as np
import pytest

import lab.generator as G
from lab.generator import build_world

SIM_START = dt.datetime(2026, 1, 1)
HORIZON = 2200
SCALE = 1.0

# The refund model, restated here on purpose. These are duplicated from the
# logit in build_world so that changing the model forces a deliberate change
# here too, rather than silently altering what the operator is asked to learn.
REFUND_COEFFICIENTS = {
    "intercept": -2.35,
    "z_log_total": 0.78,
    "mobile": 0.55,
    "apparel_top_cat": 0.85,
    "express": 0.30,
    "prior_orders": -0.45 * 0.25,
}


@pytest.fixture(scope="module")
def world():
    rng, pyrng = np.random.default_rng(20260101), random.Random(20260101)
    return build_world(rng, pyrng, SIM_START, HORIZON, SCALE)


@pytest.fixture(scope="module")
def daily(world):
    cnt = collections.Counter(o["occurred_at"].date() for o in world.orders)
    days = sorted(cnt)
    return days, np.array([cnt[d] for d in days], float), cnt


# ---------------------------------------------------------------------------
# volume
# ---------------------------------------------------------------------------


@pytest.mark.slow
def test_weekend_factor_is_recoverable(daily):
    days, c, _ = daily
    wd = np.array([d.weekday() for d in days])
    got = c[wd >= 5].mean() / c[wd < 5].mean()
    assert 0.66 < got < 0.76, f"weekend/weekday {got:.3f}, injected {G.WEEKEND_FACTOR}"


@pytest.mark.slow
def test_growth_is_recoverable(daily):
    """Measured on whole-year totals: immune to seasonality and to log bias."""
    days, c, _ = daily
    yr = np.array([d.year for d in days])
    totals = {y: c[yr == y].sum() for y in sorted(set(yr)) if (yr == y).sum() > 360}
    full = sorted(totals)
    yoy = [totals[full[i + 1]] / totals[full[i]] for i in range(len(full) - 1)]
    assert len(yoy) >= 4, "need several whole years to see a trend"
    got = float(np.exp(np.mean(np.log(yoy))))
    assert abs(got - (1 + G.ANNUAL_GROWTH)) < 0.04, \
        f"growth {got:.3f}/yr, injected {1 + G.ANNUAL_GROWTH:.3f}"
    # and the constant itself has to still describe a growing business, or the
    # forecasting exercise the README promises has quietly been switched off
    assert got > 1.05, f"growth {got:.3f}/yr is too flat to forecast"


@pytest.mark.slow
def test_the_year_has_a_shape(daily):
    days, c, _ = daily
    logc = np.log(c)
    logc -= np.log(1 + G.ANNUAL_GROWTH) * np.array(
        [(d - SIM_START.date()).days / 365.25 for d in days])
    wd = np.array([d.weekday() for d in days])
    logc -= np.array([logc[wd == k].mean() for k in range(7)])[wd]
    mo = np.array([d.month for d in days])
    index = np.array([np.exp(logc[mo == k].mean()) for k in range(1, 13)])
    assert index.max() / index.min() > 1.35, "no annual cycle worth forecasting"
    assert index[0] < index.mean(), "January should be the hangover"
    # February to October carry no hard-coded multiplier, so whatever shape they
    # have is the sinusoid alone. Measured across seeds: 1.27-1.32 with it,
    # 1.09-1.14 without, so the threshold sits between the two populations
    # rather than on top of the null.
    plain = index[1:10]
    assert plain.max() / plain.min() > 1.20, \
        f"outside Q4 the year is flat ({plain.max()/plain.min():.3f}): the annual cycle is gone"


@pytest.mark.slow
def test_promotional_days_stand_out(daily):
    _, _, cnt = daily
    lifts = []
    for y in range(SIM_START.year, SIM_START.year + HORIZON // 365):
        bf = G._nth_weekday(y, 11, 3, 4) + dt.timedelta(days=1)
        peers = [bf - dt.timedelta(days=7), bf - dt.timedelta(days=14)]
        if bf in cnt and all(p in cnt for p in peers):
            lifts.append(cnt[bf] / float(np.mean([cnt[p] for p in peers])))
    assert len(lifts) >= 4
    assert np.mean(lifts) > 2.0, f"the biggest day of the year lifts only {np.mean(lifts):.1f}x"


@pytest.mark.slow
def test_demand_has_memory(daily):
    """The latent shock has rho 0.88; sampling noise attenuates what is visible.

    Measured against the exact calendar rather than a monthly demeaning. The Q4
    multipliers are step changes inside a month, so crude deseasonalising leaves
    calendar structure in the residual and any series looks autocorrelated.
    """
    days, c, _ = daily
    cal = G._calendar_weights(SIM_START, len(days))
    expected = cal / cal.sum() * c.sum()
    resid = np.log(c) - np.log(expected)
    ac1 = float(np.corrcoef(resid[:-1], resid[1:])[0, 1])
    ac7 = float(np.corrcoef(resid[:-7], resid[7:])[0, 1])
    assert 0.30 < ac1 < 0.75, f"lag-1 autocorrelation {ac1:.2f}"
    assert ac7 > 0.10, f"lag-7 autocorrelation {ac7:.2f}: shocks do not persist"


# ---------------------------------------------------------------------------
# the refund process
# ---------------------------------------------------------------------------


@pytest.mark.slow
def test_realised_refunds_match_their_own_propensities(world):
    y = np.array([o["_true_refund"] for o in world.orders], float)
    p = np.array([o["_refund_propensity"] for o in world.orders], float)
    se = float(np.sqrt(p.mean() * (1 - p.mean()) / len(y)))
    assert abs(y.mean() - p.mean()) < 4 * se, \
        f"realised {y.mean():.4f} vs propensity {p.mean():.4f} (se {se:.4f})"
    assert 0.09 < y.mean() < 0.16, f"refund rate {y.mean():.4f} is not a plausible base rate"


@pytest.mark.slow
def test_the_refund_model_is_learnable_from_the_data(world):
    """Fit the injected model back out of the world and compare coefficients.

    This is the exercise's whole premise: the target has to be predictable from
    what the source exposes. Attenuation of about 2% is expected, because the
    logit carries unobserved noise the fit cannot see.
    """
    orders = world.orders
    y = np.array([o["_true_refund"] for o in orders], float)
    tot = np.array([o["order_total_cents"] for o in orders], float)
    lt = np.log1p(tot)
    X = np.column_stack([
        np.ones(len(y)),
        (lt - lt.mean()) / lt.std(),
        [o["channel"] == "mobile" for o in orders],
        [o["_top_cat"] == "apparel" for o in orders],
        [o["shipping_speed"] == "express" for o in orders],
        [min(o["_prior_orders"], 6) for o in orders],
    ]).astype(float)

    b = np.zeros(X.shape[1])
    for _ in range(30):                                    # IRLS
        mu = 1.0 / (1.0 + np.exp(-(X @ b)))
        s = np.clip(mu * (1 - mu), 1e-9, None)
        b += np.linalg.solve(X.T @ (X * s[:, None]), X.T @ (y - mu))

    for got, (name, injected) in zip(b, REFUND_COEFFICIENTS.items()):
        slack = 0.15 if name == "intercept" else 0.10
        assert abs(got - injected) < slack, \
            f"{name}: recovered {got:+.3f}, injected {injected:+.3f}"


@pytest.mark.slow
def test_matured_labels_never_contradict_the_truth(world):
    latent = {o["order_id"]: o["_true_refund"] for o in world.orders}
    matured = [l for l in world.labels if l["knowledge_time"] <= world.sim_end]
    assert len(matured) > 0.9 * len(world.labels)
    wrong = [l["order_id"] for l in matured
             if l["refunded_within_30d"] != latent[l["order_id"]]]
    assert not wrong, f"{len(wrong)} matured labels disagree with the latent outcome"


# ---------------------------------------------------------------------------
# the catalogue
# ---------------------------------------------------------------------------


@pytest.mark.slow
def test_catalogue_is_concentrated_but_not_dominated(world):
    units = collections.Counter()
    for o in world.orders:
        for it in o["items"]:
            units[it["sku"]] += it["qty"]
    v = np.array(sorted(units.values(), reverse=True), float)
    top10 = v[: max(1, len(v) // 10)].sum() / v.sum()
    assert 0.25 < top10 < 0.55, f"top decile carries {top10:.0%} of units"
    assert v[0] / v.sum() < 0.06, \
        "one SKU carries too much: basket value would drift with the assortment"


@pytest.mark.slow
def test_the_catalogue_turns_over(world):
    base = [p for p in world.products if p["version"] == 1]
    launched = [p for p in base if p["active_from"] >= SIM_START.date().isoformat()]
    delisted = [p for p in base if p["discontinued_from"]]
    assert 0.3 * len(base) < len(launched) < 0.8 * len(base)
    assert delisted, "nothing is ever delisted"
    assert all(p["_deleted"] == 0 for p in base), "delisting must not remove the row"
