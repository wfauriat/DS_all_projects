"""The baseline must never counterfeit a fault.

The lab is only worth anything if a signal the operator finds is either a
planted fault or nothing at all.  Two ways that breaks:

* the fault-free world drifts enough to look like a planted defect, so the
  operator reports a false positive they had no way of avoiding;
* a planted fault lands somewhere it leaves no trace, so it is impossible to
  find and silently costs them a mark.

Both have happened during development.  These tests exist so they cannot come
back quietly.
"""

from __future__ import annotations

import datetime as dt
import gzip

import numpy as np
import pytest

from lab import drop as dropmod
from lab.faults.catalog import CATALOG

SIM_START = dt.datetime(2026, 1, 1)

# Faults whose whole expression is a change of *rendering* or *delivery*: they
# legitimately touch no rows, so they are exempt from the "left a trace" check.
RENDER_ONLY = {
    "id_leading_zeros", "leakage_column", "column_added", "column_reordered",
    "encoding_change", "compression_change", "clock_skew", "sustained_outage",
    "late_partition", "missing_delivery", "duplicate_delivery", "partial_write",
}


# ---------------------------------------------------------------------------
# the baseline does not counterfeit a covariate shift
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("seed", [b"\x01" * 16, b"\x02" * 16])
def test_fault_free_basket_value_is_stable(labs, seed):
    """Catalogue turnover must not move average order value.

    A planted covariate_shift multiplies basket value by 1.00-1.55x.  If the
    assortment churning over can do the same on its own, the fault is not
    detectable and every quiet period looks suspicious.
    """
    lab = labs.make(f"fair_basket_{seed[0]}", [], horizon=1100, scale=0.3,
                    sim_start=SIM_START.date().isoformat(), seed=seed)
    st = lab.state()
    rows = st.store.con.execute(
        "SELECT occurred_at, order_total_cents FROM orders WHERE _deleted = 0"
    ).fetchall()
    day = np.array([(dt.datetime.fromisoformat(r[0]) - SIM_START).days for r in rows])
    tot = np.array([r[1] for r in rows], float)

    half = tot[day >= 550].mean() / tot[day < 550].mean()
    assert 0.90 < half < 1.10, f"baseline basket drifted {half:.3f}x over the horizon"

    windows = [tot[(day >= a) & (day < a + 90)].mean() for a in range(0, 1000, 90)]
    swing = max(windows) / min(windows)
    assert swing < 1.25, f"baseline basket swings {swing:.2f}x across 90-day windows"


def test_fault_free_world_deletes_nothing(clean_lab):
    """Product delisting is explicit, never a silent disappearance.

    Otherwise routine catalogue lifecycle reads as silent_row_deletion.
    """
    st = clean_lab.state()
    for table in ("orders", "customers", "payments", "events", "labels", "products"):
        n = st.store.con.execute(
            f"SELECT COUNT(*) FROM {table} WHERE _deleted = 1"
        ).fetchone()[0]
        assert n == 0, f"{table} lost rows with no fault planted"

    discontinued = st.store.con.execute(
        "SELECT COUNT(*) FROM products WHERE discontinued_from IS NOT NULL"
    ).fetchone()[0]
    assert discontinued > 0, "no product ever gets delisted"
    still_listed = st.store.con.execute(
        "SELECT COUNT(*) FROM products WHERE discontinued_from IS NOT NULL AND _deleted = 0"
    ).fetchone()[0]
    assert still_listed == discontinued, "delisting must be a column, not a vanishing row"


# ---------------------------------------------------------------------------
# every planted fault leaves something to find
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def loaded(labs):
    return labs.make("fair_all", list(CATALOG), horizon=900, scale=0.25,
                     sim_start=SIM_START.date().isoformat())


def test_every_row_level_fault_actually_touches_rows(loaded):
    empty = [pf.id for pf in loaded.plan
             if pf.id not in RENDER_ONLY and pf.n_affected == 0]
    assert not empty, f"planted but left no trace: {empty}"


def test_every_delivery_fault_names_a_feed_and_a_date(loaded):
    for pf in loaded.plan:
        if pf.id in RENDER_ONLY:
            assert pf.params, f"{pf.id} recorded no parameters at all"


def test_the_catalogue_feed_carries_non_ascii(loaded):
    """An encoding change on a pure-ASCII feed has no signature."""
    st = loaded.state()
    st.clock.advance(60)
    while dropmod.due_boundaries(st):
        dropmod.run_once(st)
    for feed in ("payments", "catalog"):
        spec = dropmod.FEEDS[feed]
        found = False
        for path in sorted((loaded.drop_dir / spec.dirname).glob("*"))[:40]:
            raw = path.read_bytes()
            if raw[:2] == b"\x1f\x8b":
                raw = gzip.decompress(raw)
            if any(b > 127 for b in raw):
                found = True
                break
        assert found, f"the {feed} feed is pure ASCII; an encoding change would be invisible"
