"""Phase 1: the bitemporal store."""

from __future__ import annotations

import datetime as dt

import pytest

from lab.world import ALLOWLIST, SPECS, Store


def _keys(store, table, as_of):
    spec = SPECS[table]
    return {tuple(r[c] for c in spec.key) for r in store.snapshot(table, as_of)}


def test_knowledge_is_monotone(clean_lab):
    """With nothing planted, later knowledge is strictly more knowledge."""
    store = Store(clean_lab.state_dir / "world.db")
    start = clean_lab.cfg.clock.sim_start
    stamps = [(start + dt.timedelta(days=d)).isoformat() for d in (60, 150, 300, 450, 500)]
    for table in ("orders", "customers", "payments", "events", "labels"):
        seen = set()
        for ts in stamps:
            now = _keys(store, table, ts)
            assert seen <= now, f"{table} lost keys between snapshots"
            seen = now
        assert seen, f"{table} never became visible"


def test_as_of_actually_filters(clean_lab):
    store = Store(clean_lab.state_dir / "world.db")
    start = clean_lab.cfg.clock.sim_start
    early = store.count("orders", (start + dt.timedelta(days=100)).isoformat(), None, None)
    late = store.count("orders", (start + dt.timedelta(days=400)).isoformat(), None, None)
    assert 0 < early < late


def test_no_latent_column_is_reachable(clean_lab):
    """The allowlist is the whole defence.  Nothing underscored may escape."""
    store = Store(clean_lab.state_dir / "world.db")
    as_of = clean_lab.cfg.sim_end.isoformat()
    for table in SPECS:
        rows = store.page(table, as_of, limit=50)
        assert rows, f"{table} produced no rows to check"
        for row in rows:
            payload = store.serialize(table, row, as_of)
            assert not [k for k in payload if k.startswith("_")]
            assert set(payload) <= set(ALLOWLIST[table])


def test_latent_columns_exist_in_the_store(clean_lab):
    """...but they are there, otherwise the previous test proves nothing."""
    store = Store(clean_lab.state_dir / "world.db")
    cols = [r[1] for r in store.con.execute("PRAGMA table_info(orders)")]
    assert {"_true_refund", "_refund_propensity", "_refund_ts"} <= set(cols)


def test_labels_never_mature_early(clean_lab):
    store = Store(clean_lab.state_dir / "world.db")
    rows = store.con.execute(
        "SELECT resolved_at, knowledge_time, refund_at, refunded_within_30d FROM labels LIMIT 5000"
    ).fetchall()
    assert rows
    for r in rows:
        assert r["knowledge_time"] > r["resolved_at"]
        if r["refunded_within_30d"]:
            assert r["refund_at"] is not None and r["knowledge_time"] > r["refund_at"]
        else:
            assert r["refund_at"] is None


@pytest.mark.slow
def test_row_counts_match_the_original_generator(labs):
    """Same densities as the operator's generate.py at an equivalent horizon."""
    import random
    import sys

    import numpy as np

    sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent.parent))
    import generate as original

    from lab.generator import build_world

    rng, pyrng = np.random.default_rng(11), random.Random(11)
    ref = original.build_world(rng, pyrng, 1.0)

    rng, pyrng = np.random.default_rng(11), random.Random(11)
    ours = build_world(rng, pyrng, dt.datetime(2025, 1, 2), 180, 1.0)

    assert len({c["customer_id"] for c in ours.customers}) == len(ref["customers"])
    assert len({o["order_id"] for o in ours.orders}) == len(ref["orders"])
    assert len(ours.events) == len(ref["events"])
    assert abs(len(ours.payments) - len(ref["payments"])) / len(ref["payments"]) < 0.02

    ref_rate = sum(o["_true_refund"] for o in ref["orders"]) / len(ref["orders"])
    our_rate = sum(o["_true_refund"] for o in ours.orders) / len(ours.orders)
    assert abs(our_rate - ref_rate) < 0.03
