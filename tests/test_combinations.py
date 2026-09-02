"""Insurance: any subset of the catalogue has to survive being drawn together.

A sealed run picks 11 of 30 at random and the operator never gets to retry, so
an interaction bug between two faults would silently ruin a three-month
exercise.  These build the whole catalogue at once, and then a spread of random
draws, and check that the world still holds together.
"""

from __future__ import annotations

import datetime as dt
import json

import pytest

from lab import drop as dropmod
from lab.faults import schedule as sched
from lab.faults.catalog import ALWAYS_ON, CATALOG, SAMPLEABLE
from lab.world import SPECS, Store

ALL = list(CATALOG)


def _consistency_checks(lab):
    st = lab.state(hostility=0)
    store = st.store
    as_of = lab.cfg.sim_end.isoformat()

    for table, spec in SPECS.items():
        rows = store.snapshot(table, as_of)
        seen: dict[tuple, int] = {}
        for r in rows:
            key = tuple(r[c] for c in spec.key)
            seen[key] = seen.get(key, 0) + 1
        # more than one visible row per key only ever means a duplicated write,
        # and then the rows must be identical
        for key, n in seen.items():
            if n > 1:
                same = [dict(r) for r in rows if tuple(r[c] for c in spec.key) == key]
                stripped = {json.dumps({k: str(v) for k, v in d.items() if k != "row_id"},
                                       sort_keys=True) for d in same}
                assert len(stripped) == 1, (table, key)

    # a tombstoned entity must not walk back in later
    for table in ("orders", "customers"):
        spec = SPECS[table]
        dead = store.con.execute(
            f"SELECT DISTINCT {', '.join(spec.key)} FROM {table} WHERE _deleted = 1"
        ).fetchall()
        for row in dead[:200]:
            key = tuple(row)
            hits = [r for r in store.snapshot(table, as_of)
                    if tuple(r[c] for c in spec.key) == key]
            if hits:
                # only legitimate if a higher version un-deleted it, which the
                # store never does
                assert all(r["_deleted"] == 0 for r in hits)
                newest = store.con.execute(
                    f"SELECT MAX(version) v FROM {table} WHERE "
                    + " AND ".join(f"{c} = ?" for c in spec.key), key).fetchone()["v"]
                assert hits[0]["version"] == newest

    # versions agree with knowledge order
    for table, spec in SPECS.items():
        bad = store.con.execute(f"""
            SELECT COUNT(*) n FROM {table} a JOIN {table} b
              ON {' AND '.join(f'a.{c} = b.{c}' for c in spec.key)}
             AND a.version < b.version
            WHERE a.knowledge_time > b.knowledge_time""").fetchone()["n"]
        assert bad == 0, table
    return st


def test_the_entire_catalogue_at_once(labs, client_factory):
    lab = labs.make("combo_all", ALL, horizon=600, scale=0.05,
                    sim_start="2026-01-01")
    st = _consistency_checks(lab)

    # the API still answers and still leaks nothing
    c = client_factory(st)
    st.clock.advance(590)
    tok = st.cfg.api.token
    hdr = {"Authorization": f"Bearer {tok}"}
    for ep in ("orders", "customers", "events", "labels"):
        r = c.get(f"/v1/{ep}", params={"limit": 300}, headers=hdr)
        assert r.status_code in (200, 503), (ep, r.status_code)
        if r.status_code == 200:
            assert '"_' not in json.dumps(r.json())

    # the drop still writes, and every file is readable as something
    while dropmod.due_boundaries(st):
        dropmod.run_once(st)
    written = list(lab.drop_dir.rglob("*.csv*"))
    assert len(written) > 500
    assert not [p for p in written if p.suffix == ".part"]


@pytest.mark.parametrize("n", range(6))
def test_random_draws_build_and_serve(labs, client_factory, n):
    seed = bytes([n]) * 16
    rng, pyrng = sched.rngs(seed)
    ids = sched.select(pyrng, 11)
    assert set(ALWAYS_ON) <= set(ids) and len(ids) == 11
    assert set(ids) - set(ALWAYS_ON) <= set(SAMPLEABLE)

    lab = labs.make(f"combo_{n}", ids, horizon=500, scale=0.04,
                    sim_start="2026-01-01", seed=seed)
    st = _consistency_checks(lab)
    c = client_factory(st)
    st.clock.advance(490)
    hdr = {"Authorization": f"Bearer {st.cfg.api.token}"}
    r = c.get("/v1/orders", params={"limit": 200}, headers=hdr)
    assert r.status_code in (200, 503)
    while dropmod.due_boundaries(st):
        dropmod.run_once(st)
    assert list(lab.drop_dir.rglob("*.csv*"))
