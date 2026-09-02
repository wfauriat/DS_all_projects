"""Phase 3: the read API at hostility 0.  Phase 4: levels 1-3."""

from __future__ import annotations

import datetime as dt
import json

import pytest

from tests._client import extract, naive

ENDPOINTS = ("orders", "customers", "events", "labels")


@pytest.fixture(scope="module")
def as_of(request):
    return None


def _as_of(lab) -> str:
    return (lab.cfg.clock.sim_start + dt.timedelta(days=430)).isoformat()


def _state(lab, hostility):
    st = lab.state(hostility=hostility)
    st.latency_spike_s = 0.02
    st.hang_s = 0.02
    return st


# ---------------------------------------------------------------------------
# phase 3
# ---------------------------------------------------------------------------


def test_pagination_returns_every_row_exactly_once(clean_lab, client_factory):
    st = _state(clean_lab, 0)
    c = client_factory(st)
    ao = _as_of(clean_lab)
    for ep in ENDPOINTS:
        rows = naive(c, ep, st.cfg.api.token, ao, limit=137)
        expected = st.store.count(ep, ao, None, None)
        assert len(rows) == expected, ep
        keys = [json.loads(r) for r in rows]
        # customers are SCD2: one row per validity interval, so the identity of
        # a row is the key plus where the interval starts.
        fields = {"orders": ("order_id",), "customers": ("customer_id", "valid_from"),
                  "events": ("event_id",), "labels": ("order_id",)}[ep]
        ids = [tuple(k[f] for f in fields) for k in keys]
        assert len(ids) == len(set(ids)), f"{ep} paged a row twice"


def test_as_of_is_honoured(clean_lab, client_factory):
    st = _state(clean_lab, 0)
    c = client_factory(st)
    tok = st.cfg.api.token
    early = (clean_lab.cfg.clock.sim_start + dt.timedelta(days=90)).isoformat()
    late = (clean_lab.cfg.clock.sim_start + dt.timedelta(days=430)).isoformat()
    a = set(naive(c, "orders", tok, early))
    b = set(naive(c, "orders", tok, late))
    assert 0 < len(a) < len(b)
    ids_a = {json.loads(x)["order_id"] for x in a}
    ids_b = {json.loads(x)["order_id"] for x in b}
    assert ids_a < ids_b


def test_no_underscore_key_in_any_response(clean_lab, client_factory):
    st = _state(clean_lab, 0)
    c = client_factory(st)
    tok = st.cfg.api.token
    ao = _as_of(clean_lab)
    for ep in ENDPOINTS:
        r = c.get(f"/v1/{ep}", params={"limit": 50, "as_of": ao},
                  headers={"Authorization": f"Bearer {tok}"})
        assert r.status_code == 200
        blob = json.dumps(r.json())
        assert '"_' not in blob, ep
    oid = json.loads(naive(c, "orders", tok, ao, limit=5)[0])["order_id"]
    r = c.get(f"/v1/orders/{oid}", params={"as_of": ao},
              headers={"Authorization": f"Bearer {tok}"})
    assert r.status_code == 200 and '"_' not in json.dumps(r.json())


def test_auth_is_required(clean_lab, client_factory):
    st = _state(clean_lab, 0)
    c = client_factory(st)
    assert c.get("/v1/orders").status_code == 401
    assert c.get("/v1/orders", headers={"Authorization": "Bearer nope"}).status_code == 401


def test_single_order_defaults_to_simulated_now(clean_lab, client_factory):
    """A row that has not become knowable yet simply is not there."""
    st = _state(clean_lab, 0)
    c = client_factory(st)
    tok = st.cfg.api.token
    ao = _as_of(clean_lab)
    oid = json.loads(naive(c, "orders", tok, ao, limit=5)[-1])["order_id"]
    assert c.get(f"/v1/orders/{oid}", headers={"Authorization": f"Bearer {tok}"}).status_code == 404
    assert c.get(f"/v1/orders/{oid}", params={"as_of": ao},
                 headers={"Authorization": f"Bearer {tok}"}).status_code == 200


def test_health_answers(clean_lab, client_factory):
    c = client_factory(_state(clean_lab, 0))
    r = c.get("/v1/health")
    assert r.status_code == 200 and r.json()["status"] == "ok"


def test_since_until_filter_valid_time(clean_lab, client_factory):
    st = _state(clean_lab, 0)
    c = client_factory(st)
    lo = (clean_lab.cfg.clock.sim_start + dt.timedelta(days=100)).isoformat()
    hi = (clean_lab.cfg.clock.sim_start + dt.timedelta(days=140)).isoformat()
    r = c.get("/v1/orders", params={"since": lo, "until": hi, "as_of": _as_of(clean_lab),
                                    "limit": 500},
              headers={"Authorization": f"Bearer {st.cfg.api.token}"})
    rows = r.json()["data"]
    assert rows
    assert all(lo <= x["occurred_at"] < hi for x in rows)


# ---------------------------------------------------------------------------
# phase 4
# ---------------------------------------------------------------------------


def test_hostility_never_changes_the_data(clean_lab, client_factory):
    """The golden test.  Delivery gets worse; the payload does not move."""
    ao = _as_of(clean_lab)
    baseline = None
    for level in (0, 1, 2, 3):
        st = _state(clean_lab, level)
        c = client_factory(st)
        got = {ep: extract(c, ep, st.cfg.api.token, ao, limit=250) for ep in ENDPOINTS}
        if baseline is None:
            baseline = got
        else:
            for ep in ENDPOINTS:
                assert got[ep] == baseline[ep], f"level {level} changed {ep}"


def test_naive_client_survives_level_0_and_dies_at_level_2(clean_lab, client_factory):
    ao = _as_of(clean_lab)
    st0 = _state(clean_lab, 0)
    assert naive(client_factory(st0), "orders", st0.cfg.api.token, ao)

    st2 = _state(clean_lab, 2)
    with pytest.raises(Exception):
        naive(client_factory(st2), "orders", st2.cfg.api.token, ao)


def test_limit_is_silently_capped_above_level_0(clean_lab, client_factory):
    ao = _as_of(clean_lab)
    st = _state(clean_lab, 1)
    c = client_factory(st)
    for _ in range(40):
        r = c.get("/v1/events", params={"limit": 900, "as_of": ao},
                  headers={"Authorization": f"Bearer {st.cfg.api.token}"})
        if r.status_code == 200:
            assert len(r.json()["data"]) == 500
            return
    pytest.fail("never got a clean response")
