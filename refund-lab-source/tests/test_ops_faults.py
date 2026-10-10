"""Phase 5: the drop channel, and every operational fault force-fired.

Each fault is built into its own world at a time this file chooses, and then
the documented signature is asserted.  Nothing here touches a sealed ledger.
"""

from __future__ import annotations

import datetime as dt
import gzip
import json

import pytest

from lab import drop as dropmod

SIM_START = dt.datetime(2026, 1, 1)
HORIZON = 300


def at(day: float, hours: float = 0.0) -> dt.datetime:
    return SIM_START + dt.timedelta(days=day, hours=hours)


def drain(state) -> None:
    while dropmod.due_boundaries(state):
        dropmod.run_once(state)


def payments_dir(lab):
    return lab.drop_dir / "payments"


def files_for(lab, feed: str, day: dt.date) -> list:
    spec = dropmod.FEEDS[feed]
    root = lab.drop_dir / spec.dirname
    if not root.exists():
        return []
    return sorted(root.glob(f"{spec.stem}_{day.isoformat()}*{spec.extension}"))


def read_csv(path) -> list[list[str]]:
    raw = path.read_bytes()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    text = raw.decode("utf-8", errors="replace")
    return [line.split(",") for line in text.splitlines()]


def params_of(lab, fault_id: str) -> dict:
    return {p.id: p for p in lab.plan}[fault_id].params


def build(labs, name, faults, times=None, horizon=HORIZON, scale=0.08):
    return labs.make(name, faults, times=times, horizon=horizon, scale=scale,
                     sim_start=SIM_START.date().isoformat())


# ---------------------------------------------------------------------------
# the channel itself
# ---------------------------------------------------------------------------


def test_files_appear_on_simulated_day_boundaries(clean_lab):
    st = clean_lab.state()
    st.clock.advance(9)
    dropmod.run_once(st)
    days = [SIM_START.date() + dt.timedelta(days=d) for d in range(9)]
    for d in days:
        assert files_for(clean_lab, "payments", d), d
        assert files_for(clean_lab, "catalog", d), d
    assert not files_for(clean_lab, "payments", SIM_START.date() + dt.timedelta(days=9))
    assert not list(payments_dir(clean_lab).glob("*.part"))


def test_payments_file_holds_what_was_learned_that_day(clean_lab):
    st = clean_lab.state()
    st.clock.advance(9)
    dropmod.run_once(st)
    day = SIM_START.date() + dt.timedelta(days=3)
    rows = read_csv(files_for(clean_lab, "payments", day)[0])
    ids = {r[0] for r in rows[1:]}
    expected = {
        r["payment_id"]
        for r in st.store.by_knowledge_day(
            "payments", day, (dt.datetime.combine(day, dt.time()) + dt.timedelta(days=1)).isoformat()
        )
    }
    assert ids == expected and ids


def test_no_latent_column_reaches_a_drop_file(clean_lab):
    from lab.world import allowed_keys

    st = clean_lab.state()
    st.clock.advance(9)
    dropmod.run_once(st)
    checked = 0
    for feed, spec in dropmod.FEEDS.items():
        allowed = {h for h, _ in spec.columns} | set(
            k for k in allowed_keys(spec.table)
        )
        for path in (clean_lab.drop_dir / spec.dirname).glob("*"):
            header = read_csv(path)[0]
            assert not [h for h in header if h.startswith("_")], (path, header)
            assert set(header) <= allowed, (path, set(header) - allowed)
            checked += 1
    assert checked > 10


# ---------------------------------------------------------------------------
# delivery
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def delivery_lab(labs):
    return build(
        labs, "ops_delivery",
        ["late_partition", "missing_delivery", "duplicate_delivery", "partial_write"],
        times={
            "late_partition": (at(40), at(41)),
            "missing_delivery": (at(80), at(81)),
            "duplicate_delivery": (at(120), at(121)),
            "partial_write": (at(160), at(161)),
        },
    )


def test_late_partition(delivery_lab):
    p = params_of(delivery_lab, "late_partition")
    day = dt.date.fromisoformat(p["day"])
    st = delivery_lab.state()
    st.clock.advance(41)          # boundary day+1: the file is due and absent
    drain(st)
    assert not files_for(delivery_lab, p["feed"], day)
    assert files_for(delivery_lab, p["feed"], day - dt.timedelta(days=1))
    st.clock.advance(p["delay_days"])
    drain(st)
    assert files_for(delivery_lab, p["feed"], day)


def test_missing_delivery(delivery_lab):
    p = params_of(delivery_lab, "missing_delivery")
    day = dt.date.fromisoformat(p["day"])
    st = delivery_lab.state()
    st.clock.advance(95)
    drain(st)
    assert not files_for(delivery_lab, p["feed"], day)
    assert files_for(delivery_lab, p["feed"], day - dt.timedelta(days=1))
    assert files_for(delivery_lab, p["feed"], day + dt.timedelta(days=1))


def test_duplicate_delivery(delivery_lab):
    p = params_of(delivery_lab, "duplicate_delivery")
    day = dt.date.fromisoformat(p["day"])
    st = delivery_lab.state()
    st.clock.advance(125)
    drain(st)
    got = files_for(delivery_lab, p["feed"], day)
    assert len(got) == 2, [f.name for f in got]
    assert {f.name for f in got} == {
        f"payments_{day.isoformat()}.csv.gz",
        f"payments_{day.isoformat()}{p['suffix']}.csv.gz",
    }
    assert read_csv(got[0]) == read_csv(got[1])


def test_partial_write(delivery_lab):
    p = params_of(delivery_lab, "partial_write")
    day = dt.date.fromisoformat(p["day"])
    st = delivery_lab.state()
    st.clock.advance(165)
    drain(st)
    f = files_for(delivery_lab, p["feed"], day)[0]
    raw = f.read_bytes()
    text = gzip.decompress(raw).decode("utf-8", "replace") if raw[:2] == b"\x1f\x8b" else raw.decode()
    assert not text.endswith("\n"), "a truncated file should stop mid-row"
    ncols = len(dropmod.FEEDS[p["feed"]].columns)
    assert len(text.splitlines()[-1].split(",")) < ncols


# ---------------------------------------------------------------------------
# schema and encoding
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def schema_lab(labs):
    return build(
        labs, "ops_schema",
        ["column_added", "column_reordered", "encoding_change", "compression_change"],
        times={
            "column_added": (at(60), at(HORIZON)),
            "column_reordered": (at(60), at(HORIZON)),
            "encoding_change": (at(100), at(130)),
            "compression_change": (at(150), at(180)),
        },
    )


@pytest.fixture(scope="module")
def schema_written(schema_lab):
    st = schema_lab.state()
    st.clock.advance(250)
    drain(st)
    return schema_lab


def _header(lab, feed, day):
    got = files_for(lab, feed, day)
    assert got, (feed, day)
    return read_csv(got[0])[0]


def test_column_added_at_a_date_boundary(schema_written):
    p = params_of(schema_written, "column_added")
    after = dt.date.fromisoformat(p["after"])
    before = _header(schema_written, p["feed"], after - dt.timedelta(days=2))
    now = _header(schema_written, p["feed"], after + dt.timedelta(days=2))
    assert p["column"] not in before
    assert p["column"] in now
    assert set(before) < set(now)


def test_column_reordered_at_a_date_boundary(schema_written):
    p = params_of(schema_written, "column_reordered")
    after = dt.date.fromisoformat(p["after"])
    before = _header(schema_written, p["feed"], after - dt.timedelta(days=2))
    now = _header(schema_written, p["feed"], after + dt.timedelta(days=2))
    assert set(before) <= set(now)
    assert [c for c in now if c in before] != before


def test_encoding_change(schema_written):
    p = params_of(schema_written, "encoding_change")
    lo, hi = dt.date.fromisoformat(p["from"]), dt.date.fromisoformat(p["to"])
    bad = 0
    day = lo
    while day < hi:
        for f in files_for(schema_written, p["feed"], day):
            raw = f.read_bytes()
            if raw[:2] == b"\x1f\x8b":
                raw = gzip.decompress(raw)
            try:
                raw.decode("utf-8")
            except UnicodeDecodeError:
                bad += 1
        day += dt.timedelta(days=1)
    assert bad, "nothing in the window stopped being UTF-8"
    outside = files_for(schema_written, p["feed"], hi + dt.timedelta(days=3))[0].read_bytes()
    if outside[:2] == b"\x1f\x8b":
        outside = gzip.decompress(outside)
    outside.decode("utf-8")


def test_compression_change_keeps_the_extension(schema_written):
    p = params_of(schema_written, "compression_change")
    lo, hi = dt.date.fromisoformat(p["from"]), dt.date.fromisoformat(p["to"])
    inside = files_for(schema_written, p["feed"], lo + dt.timedelta(days=2))[0]
    outside = files_for(schema_written, p["feed"], hi + dt.timedelta(days=3))[0]
    assert inside.suffix == outside.suffix
    assert (inside.read_bytes()[:2] == b"\x1f\x8b") is bool(p["gzip"])
    assert (outside.read_bytes()[:2] == b"\x1f\x8b") is dropmod.DEFAULT_GZIP[p["feed"]]


# ---------------------------------------------------------------------------
# serving
# ---------------------------------------------------------------------------


def test_sustained_outage(labs, client_factory):
    lab = build(labs, "ops_outage", ["sustained_outage"],
                times={"sustained_outage": (at(100), at(100, 6))})
    p = params_of(lab, "sustained_outage")
    st = lab.state()
    c = client_factory(st)
    hdr = {"Authorization": f"Bearer {st.cfg.api.token}"}
    st.clock.advance(99)
    assert c.get("/v1/orders", headers=hdr).status_code == 200
    st.clock.advance(1.1)
    r = c.get("/v1/orders", headers=hdr)
    assert r.status_code == 503 and r.json()["error"] == "service_unavailable"
    assert c.get("/v1/health").json()["status"] == "ok"
    st.clock.advance(1)
    assert c.get("/v1/orders", headers=hdr).status_code == 200


def test_clock_skew(labs, client_factory):
    lab = build(labs, "ops_skew", ["clock_skew"],
                times={"clock_skew": (at(100), at(105))})
    p = params_of(lab, "clock_skew")
    st = lab.state()
    c = client_factory(st)
    hdr = {"Authorization": f"Bearer {st.cfg.api.token}"}
    as_of = at(90).isoformat()

    st.clock.advance(95)
    clean = c.get("/v1/orders", params={"as_of": as_of, "limit": 20}, headers=hdr).json()["data"]
    st.clock.advance(7)
    skewed = c.get("/v1/orders", params={"as_of": as_of, "limit": 20}, headers=hdr).json()["data"]
    assert [x["order_id"] for x in clean] == [x["order_id"] for x in skewed]
    deltas = {
        (dt.datetime.fromisoformat(b["occurred_at"]) - dt.datetime.fromisoformat(a["occurred_at"]))
        for a, b in zip(clean, skewed)
    }
    assert deltas == {dt.timedelta(seconds=p["seconds"])}


# ---------------------------------------------------------------------------
# store-level
# ---------------------------------------------------------------------------


def test_silent_row_deletion(labs):
    lab = build(labs, "ops_delete", ["silent_row_deletion"],
                times={"silent_row_deletion": (at(200), at(HORIZON))})
    p = params_of(lab, "silent_row_deletion")
    st = lab.state()
    lo, hi = p["valid_window"]
    before = st.store.count("orders", at(199).isoformat(), lo, hi)
    after = st.store.count("orders", at(201).isoformat(), lo, hi)
    assert p["n_removed"] > 0
    assert after == before - p["n_removed"]
    # payments for the removed orders are still there, pointing at nothing
    orphan = st.store.con.execute(
        "SELECT COUNT(*) FROM payments WHERE order_id IN "
        "(SELECT order_id FROM orders WHERE _deleted=1)"
    ).fetchone()[0]
    assert orphan > 0


def test_id_reuse(labs):
    lab = build(labs, "ops_idreuse", ["id_reuse"],
                times={"id_reuse": (at(200), at(HORIZON))})
    p = params_of(lab, "id_reuse")
    st = lab.state()
    retired = dt.datetime.fromisoformat(p["retired_at"])
    reissued = dt.datetime.fromisoformat(p["reissued_at"])
    assert p["n_ids"] > 0
    changed = 0
    for cid in next(pf.affected_keys for pf in lab.plan if pf.id == "id_reuse"):
        old = st.store.one("customers", cid, (retired - dt.timedelta(days=1)).isoformat())
        gap = st.store.one("customers", cid, (retired + dt.timedelta(hours=1)).isoformat())
        new = st.store.one("customers", cid, (reissued + dt.timedelta(days=1)).isoformat())
        assert old is not None and gap is None and new is not None
        if new["signup_date"] != old["signup_date"]:
            changed += 1
    assert changed == p["n_ids"]


def test_backfill_restatement(labs):
    lab = build(labs, "ops_backfill", ["backfill_restatement"])
    p = params_of(lab, "backfill_restatement")
    assert p["always_on"] is True
    assert p["tables"]["orders"] > 0
    st = lab.state()
    row = st.store.con.execute(
        "SELECT order_id, knowledge_time FROM orders WHERE version > 1 LIMIT 1"
    ).fetchone()
    assert row is not None
    kt = dt.datetime.fromisoformat(row["knowledge_time"])
    before = st.store.one("orders", row["order_id"], (kt - dt.timedelta(seconds=1)).isoformat())
    after = st.store.one("orders", row["order_id"], kt.isoformat())
    assert before["version"] < after["version"]
    assert before["order_total_cents"] != after["order_total_cents"]


# ---------------------------------------------------------------------------
# the harness itself
# ---------------------------------------------------------------------------


def test_forcing_never_writes_a_ledger(labs):
    lab = build(labs, "ops_noledger", ["missing_delivery"])
    assert not (lab.state_dir / "ledger.sealed").exists()
