"""Phase 6: the seventeen data defects, re-plumbed through knowledge_time.

Each is force-fired on its own and checked for the signature the catalogue
promises.  The clean world is then checked for the absence of all of them.
"""

from __future__ import annotations

import datetime as dt
import gzip
import statistics

import pytest

from lab import drop as dropmod

SIM_START = dt.datetime(2026, 1, 1)
HORIZON = 400


def at(day: float) -> dt.datetime:
    return SIM_START + dt.timedelta(days=day)


def build(labs, name, faults, times=None, scale=0.12):
    return labs.make(name, faults, times=times, horizon=HORIZON, scale=scale,
                     sim_start=SIM_START.date().isoformat())


def params_of(lab, fault_id):
    return {p.id: p for p in lab.plan}[fault_id].params


def rows(store, sql, *args):
    return store.con.execute(sql, args).fetchall()


def api(lab, client_factory, hostility=0):
    st = lab.state(hostility=hostility)
    return st, client_factory(st)


def get(c, st, path, **params):
    r = c.get(path, params=params, headers={"Authorization": f"Bearer {st.cfg.api.token}"})
    assert r.status_code == 200, (path, r.status_code, r.text[:200])
    return r.json()


# ---------------------------------------------------------------------------
# ingestion
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def dup_lab(labs):
    return build(labs, "data_dups", ["dup_exact_payments", "dup_near_orders"],
                 times={"dup_exact_payments": (at(120), at(124)),
                        "dup_near_orders": (at(200), at(205))})


def test_dup_exact_payments(dup_lab):
    p = params_of(dup_lab, "dup_exact_payments")
    st = dup_lab.state()
    dups = rows(st.store, """
        SELECT payment_id, COUNT(*) n FROM payments
        GROUP BY payment_id, version HAVING n > 1""")
    assert len(dups) == p["n_duplicated_rows"] > 0
    # and they survive the visibility rule: a byte-identical retry is not a restatement
    pid = dups[0]["payment_id"]
    served = [r for r in st.store.snapshot("payments", at(HORIZON).isoformat())
              if r["payment_id"] == pid]
    assert len(served) == 2
    assert served[0]["amount_cents"] == served[1]["amount_cents"]


def test_dup_near_orders(dup_lab):
    p = params_of(dup_lab, "dup_near_orders")
    st = dup_lab.state()
    assert p["n_twin_orders"] > 0
    twins = rows(st.store, """
        SELECT a.order_id a_id, b.order_id b_id, a.order_total_cents t,
               a.occurred_at ao, b.occurred_at bo
        FROM orders a JOIN orders b
          ON a.customer_id = b.customer_id
         AND a.order_total_cents = b.order_total_cents
         AND a.order_id < b.order_id
        WHERE abs(julianday(b.occurred_at) - julianday(a.occurred_at)) * 86400 < 60""")
    assert len(twins) >= p["n_twin_orders"] * 0.9


@pytest.fixture(scope="module")
def cust_lab(labs):
    return build(labs, "data_cust1", ["mojibake_city", "category_rename"],
                 times={"mojibake_city": (at(150), at(HORIZON)),
                        "category_rename": (at(250), at(HORIZON))})


def test_mojibake_city(cust_lab):
    p = params_of(cust_lab, "mojibake_city")
    st = cust_lab.state()
    t0 = dt.datetime.fromisoformat(p["from"])
    before = {r["city"] for r in st.store.snapshot("customers", (t0 - dt.timedelta(days=1)).isoformat())}
    after = {r["city"] for r in st.store.snapshot("customers", (t0 + dt.timedelta(days=1)).isoformat())}
    assert p["n_customers"] > 0
    assert not any("Ã" in c or "Å" in c for c in before)
    assert any("Ã" in c or "Å" in c for c in after)


def test_category_rename(cust_lab):
    p = params_of(cust_lab, "category_rename")
    st = cust_lab.state()
    t0 = dt.datetime.fromisoformat(p["from"])
    before = {r["country"] for r in st.store.snapshot("customers", (t0 - dt.timedelta(days=1)).isoformat())}
    after = {r["country"] for r in st.store.snapshot("customers", (t0 + dt.timedelta(days=1)).isoformat())}
    assert "USA" not in before and "US" in before
    assert {"US", "USA"} <= after, "one class should have split in two"


@pytest.fixture(scope="module")
def sentinel_lab(labs):
    return build(labs, "data_cust2", ["sentinel_values", "survivorship_customers"],
                 times={"sentinel_values": (at(150), at(HORIZON)),
                        "survivorship_customers": (at(250), at(HORIZON))})


def test_sentinel_values(sentinel_lab):
    p = params_of(sentinel_lab, "sentinel_values")
    st = sentinel_lab.state()
    t0 = dt.datetime.fromisoformat(p["from"])
    before = st.store.snapshot("customers", (t0 - dt.timedelta(days=1)).isoformat())
    after = st.store.snapshot("customers", (t0 + dt.timedelta(days=1)).isoformat())
    assert not any(r["lifetime_value_cents"] == -999 for r in before)
    assert not any(r["signup_date"] == "1900-01-01" for r in before)
    hits = sum(
        r["lifetime_value_cents"] == -999
        or r["signup_date"] == "1900-01-01"
        or r["city"] in ("N/A", "", "null", "NULL", "-")
        for r in after
    )
    assert hits > 0 and sum(p["counts"].values()) > 0


def test_survivorship_customers(sentinel_lab):
    p = params_of(sentinel_lab, "survivorship_customers")
    st = sentinel_lab.state()
    t0 = dt.datetime.fromisoformat(p["from"])
    before = {r["customer_id"] for r in st.store.snapshot("customers", (t0 - dt.timedelta(days=1)).isoformat())}
    after = {r["customer_id"] for r in st.store.snapshot("customers", (t0 + dt.timedelta(days=1)).isoformat())}
    gone = before - after
    assert len(gone) == p["n_removed"] > 0
    orphaned = rows(st.store, """
        SELECT COUNT(*) n FROM orders WHERE customer_id IN (%s)"""
        % ",".join("?" * len(gone)), *sorted(gone))
    assert orphaned[0]["n"] > 0, "the facts should have outlived the dimension"


# ---------------------------------------------------------------------------
# render-time defects, seen through the API
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def render_lab(labs):
    return build(
        labs, "data_render",
        ["field_rename_midstream", "tz_mixed", "unit_switch_price", "leakage_column"],
        times={
            "field_rename_midstream": (at(100), at(HORIZON)),
            "tz_mixed": (at(150), at(HORIZON)),
            "unit_switch_price": (at(200), at(HORIZON)),
            "leakage_column": (at(250), at(HORIZON)),
        },
    )


def _sample_orders(lab, client_factory, day, n=30):
    st, c = api(lab, client_factory)
    lo = at(day).isoformat()
    hi = at(day + 6).isoformat()
    return get(c, st, "/v1/orders", since=lo, until=hi, as_of=at(HORIZON).isoformat(),
               limit=n)["data"]


def test_field_rename_midstream(render_lab, client_factory):
    p = params_of(render_lab, "field_rename_midstream")
    before = _sample_orders(render_lab, client_factory, 90)
    after = _sample_orders(render_lab, client_factory, 110)
    assert before and after
    assert all(p["from_name"] in o and p["to_name"] not in o for o in before)
    assert all(p["to_name"] in o and p["from_name"] not in o for o in after)


def test_tz_mixed(render_lab, client_factory):
    before = _sample_orders(render_lab, client_factory, 140)
    after = _sample_orders(render_lab, client_factory, 160)
    assert all(not o["occurred_at"].endswith("+00:00") for o in before)
    assert all(o["occurred_at"].endswith("+00:00") for o in after)


def test_unit_switch_price(render_lab, client_factory):
    before = _sample_orders(render_lab, client_factory, 190)
    after = _sample_orders(render_lab, client_factory, 210)
    assert all(isinstance(o["order_total_cents"], int) for o in before)
    assert all(isinstance(o["order_total_cents"], float) for o in after)
    assert statistics.mean(o["order_total_cents"] for o in after) < \
        statistics.mean(o["order_total_cents"] for o in before) / 50


def test_leakage_column(render_lab, client_factory):
    p = params_of(render_lab, "leakage_column")
    before = _sample_orders(render_lab, client_factory, 240)
    after = _sample_orders(render_lab, client_factory, 260)
    for col in p["columns"]:
        assert all(col not in o for o in before)
        assert all(col in o for o in after)
    assert any(o["support_ticket_count"] is not None for o in after)


def test_id_leading_zeros(labs):
    lab = build(labs, "data_intids", ["id_leading_zeros"],
                times={"id_leading_zeros": (at(100), at(HORIZON))})
    p = params_of(lab, "id_leading_zeros")
    st = lab.state()
    st.clock.advance(150)
    while dropmod.due_boundaries(st):
        dropmod.run_once(st)

    def cust_ids(day):
        f = lab.drop_dir / "payments" / f"payments_{at(day).date().isoformat()}.csv.gz"
        text = gzip.decompress(f.read_bytes()).decode()
        head, *body = text.splitlines()
        i = head.split(",").index("customer_id")
        return [r.split(",")[i] for r in body if r]

    before = cust_ids(90)
    after = cust_ids(110)
    assert before and after
    assert all(v.startswith("0") and len(v) == 7 for v in before)
    assert all(not v.startswith("0") for v in after)


# ---------------------------------------------------------------------------
# time
# ---------------------------------------------------------------------------


def test_late_arriving_events(labs):
    lab = build(labs, "data_lateev", ["late_arriving_events"],
                times={"late_arriving_events": (at(150), at(180))})
    p = params_of(lab, "late_arriving_events")
    st = lab.state()
    assert p["n_events_delayed"] > 0
    lag = rows(st.store, """
        SELECT COUNT(*) n FROM events
        WHERE occurred_at >= ? AND occurred_at < ?
          AND julianday(knowledge_time) - julianday(occurred_at) > 1.0""",
        at(150).isoformat(), at(180).isoformat())[0]["n"]
    assert lag == p["n_events_delayed"]
    outside = rows(st.store, """
        SELECT COUNT(*) n FROM events
        WHERE occurred_at < ? AND julianday(knowledge_time) - julianday(occurred_at) > 1.0""",
        at(150).isoformat())[0]["n"]
    assert outside == 0


def test_dst_duplicate_hour(labs):
    lab = build(labs, "data_dst", ["dst_duplicate_hour"])
    p = params_of(lab, "dst_duplicate_hour")
    st = lab.state()
    day = p["date"]
    hours = rows(st.store, """
        SELECT CAST(substr(occurred_at, 12, 2) AS INT) h, COUNT(*) n FROM orders
        WHERE substr(occurred_at, 1, 10) = ? GROUP BY h ORDER BY n DESC""", day)
    assert hours and hours[0]["h"] == 2
    other = rows(st.store, """
        SELECT CAST(substr(occurred_at, 12, 2) AS INT) h, COUNT(*) n FROM orders
        WHERE substr(occurred_at, 1, 10) = ? GROUP BY h ORDER BY n DESC""",
        (dt.date.fromisoformat(day) - dt.timedelta(days=7)).isoformat())
    assert other[0]["h"] != 2


# ---------------------------------------------------------------------------
# semantic / statistical
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def order_lab(labs):
    return build(labs, "data_orders",
                 ["covariate_shift", "new_category_late", "mnar_missing"],
                 times={"covariate_shift": (at(300), at(HORIZON)),
                        "new_category_late": (at(340), at(HORIZON)),
                        "mnar_missing": (at(120), at(HORIZON))})


def test_covariate_shift(order_lab):
    p = params_of(order_lab, "covariate_shift")
    st = order_lab.state()
    t0 = p["after"]
    lo = rows(st.store, "SELECT AVG(order_total_cents) a FROM orders WHERE occurred_at < ?", t0)[0]["a"]
    hi = rows(st.store, "SELECT AVG(order_total_cents) a FROM orders WHERE occurred_at > ?", t0)[0]["a"]
    assert hi > lo * 1.15


def test_new_category_late(order_lab):
    p = params_of(order_lab, "new_category_late")
    st = order_lab.state()
    before = rows(st.store, "SELECT COUNT(*) n FROM orders WHERE occurred_at < ? AND payment_method='bnpl'",
                  p["after"])[0]["n"]
    after = rows(st.store, "SELECT COUNT(*) n FROM orders WHERE occurred_at > ? AND payment_method='bnpl'",
                 p["after"])[0]["n"]
    assert before == 0 and after == p["n_rows"] > 0


def test_mnar_missing(order_lab):
    p = params_of(order_lab, "mnar_missing")
    st = order_lab.state()
    t0 = p["after"]
    def rate(refunded):
        r = rows(st.store, """
            SELECT AVG(CASE WHEN shipping_speed IS NULL THEN 1.0 ELSE 0.0 END) a
            FROM orders WHERE occurred_at > ? AND _true_refund = ?""", t0, refunded)
        return r[0]["a"]
    assert rate(1) > 4 * rate(0) > 0


# ---------------------------------------------------------------------------
# labels
# ---------------------------------------------------------------------------


def test_label_censoring_is_structural(clean_lab):
    """No fault needs to be planted: near the edge, absence is not a negative."""
    st = clean_lab.state()
    as_of = (clean_lab.cfg.clock.sim_start + dt.timedelta(days=430)).isoformat()
    edge = (clean_lab.cfg.clock.sim_start + dt.timedelta(days=410)).isoformat()
    old = (clean_lab.cfg.clock.sim_start + dt.timedelta(days=300)).isoformat()

    def labelled(lo, hi):
        n_orders = st.store.count("orders", as_of, lo, hi)
        seen = {r["order_id"] for r in st.store.snapshot("labels", as_of)}
        got = sum(1 for r in st.store.snapshot("orders", as_of)
                  if lo <= r["occurred_at"] < hi and r["order_id"] in seen)
        return got, n_orders

    near_got, near_n = labelled(edge, as_of)
    old_got, old_n = labelled(old, (clean_lab.cfg.clock.sim_start + dt.timedelta(days=330)).isoformat())
    assert near_n and old_n
    assert near_got / near_n < 0.35, "recent orders should mostly be undetermined"
    assert old_got / old_n > 0.95, "matured orders should almost all be resolved"


# ---------------------------------------------------------------------------
# forcing off
# ---------------------------------------------------------------------------


def test_clean_world_shows_no_signatures(clean_lab, client_factory):
    st, c = api(clean_lab, client_factory)
    as_of = clean_lab.cfg.sim_end.isoformat()
    data = get(c, st, "/v1/orders", as_of=as_of, limit=400)["data"]
    assert data
    assert all("channel" in o and "order_channel" not in o for o in data)
    assert all(isinstance(o["order_total_cents"], int) for o in data)
    assert all(not o["occurred_at"].endswith("+00:00") for o in data)
    assert all("refund_processed_at" not in o for o in data)
    assert all(o["payment_method"] != "bnpl" for o in data)

    custs = st.store.snapshot("customers", as_of)
    assert all("Ã" not in r["city"] for r in custs)
    assert all(r["country"] != "USA" for r in custs)
    assert all(r["lifetime_value_cents"] != -999 for r in custs)
    assert all(r["signup_date"] != "1900-01-01" for r in custs)

    assert rows(st.store, "SELECT COUNT(*) n FROM orders WHERE _deleted=1")[0]["n"] == 0
    assert rows(st.store, "SELECT COUNT(*) n FROM customers WHERE _deleted=1")[0]["n"] == 0
    assert rows(st.store, "SELECT COUNT(*) n FROM orders WHERE version>1")[0]["n"] == 0
    assert rows(st.store, """
        SELECT COUNT(*) n FROM (SELECT payment_id FROM payments
        GROUP BY payment_id, version HAVING COUNT(*) > 1)""")[0]["n"] == 0
    assert rows(st.store, """
        SELECT COUNT(*) n FROM events
        WHERE julianday(knowledge_time) - julianday(occurred_at) > 1.0""")[0]["n"] == 0
