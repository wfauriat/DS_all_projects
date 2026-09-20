"""Integration tests against the FastAPI app + live dev DB.

These verify response shape and key invariants. They deliberately do NOT
assert on exact counts (article counts grow with each ingest run); when
counts matter, they assert lower bounds.
"""

from __future__ import annotations

# Re-export the skip guard from the conftest by importing it.
from tests.integration.conftest import pytestmark  # noqa: F401

# --- meta ----------------------------------------------------------------


async def test_healthz(aclient):
    r = await aclient.get("/healthz")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}


async def test_version(aclient):
    r = await aclient.get("/version")
    assert r.status_code == 200
    assert "api" in r.json()


async def test_stats_shape(aclient):
    r = await aclient.get("/stats")
    assert r.status_code == 200
    body = r.json()
    for key in ("articles", "mentions", "active_sources", "candidates", "ingest_runs"):
        assert key in body
        assert isinstance(body[key], int)
        assert body[key] >= 0


# --- candidates ----------------------------------------------------------


async def test_candidates_list_contains_macron_ineligible(aclient):
    r = await aclient.get("/candidates")
    assert r.status_code == 200
    items = r.json()
    assert len(items) >= 14
    by_slug = {c["slug"]: c for c in items}
    assert "macron" in by_slug
    assert by_slug["macron"]["eligible"] is False
    assert by_slug["macron"]["display_name"] == "Emmanuel Macron"


async def test_candidates_have_alias_counts(aclient):
    items = (await aclient.get("/candidates")).json()
    # Every candidate should have at least one alias defined.
    assert all(c["n_aliases"] >= 1 for c in items)


async def test_le_pen_marked_ineligible(aclient):
    items = (await aclient.get("/candidates")).json()
    by_slug = {c["slug"]: c for c in items}
    assert by_slug["le-pen-marine"]["eligible"] is False


# --- sources -------------------------------------------------------------


async def test_sources_active_only_by_default(aclient):
    r = await aclient.get("/sources")
    assert r.status_code == 200
    items = r.json()
    assert all(s["is_active"] is True for s in items)


async def test_sources_include_inactive_returns_more(aclient):
    active = (await aclient.get("/sources")).json()
    all_sources = (await aclient.get("/sources?include_inactive=true")).json()
    # Les Échos + La Croix were disabled in seeds/sources.yaml.
    assert len(all_sources) > len(active)
    assert any(s["slug"] == "lesechos-france" for s in all_sources)
    assert any(s["slug"] == "lacroix-une" for s in all_sources)


# --- timeseries ----------------------------------------------------------


async def test_timeseries_default_window_returns_chart_shape(aclient):
    r = await aclient.get("/timeseries")
    assert r.status_code == 200
    body = r.json()
    assert "points" in body
    assert "candidates" in body
    assert "sources" in body
    assert body["extractor"] == "keyword"
    assert body["extractor_version"] == "v1"
    if body["points"]:
        p = body["points"][0]
        for key in ("day", "candidate_id", "source_id", "n_mentions", "n_articles"):
            assert key in p


async def test_timeseries_filter_by_candidate(aclient):
    cands = (await aclient.get("/candidates")).json()
    macron = next(c for c in cands if c["slug"] == "macron")
    r = await aclient.get("/timeseries", params={"candidate_id": macron["id"]})
    assert r.status_code == 200
    body = r.json()
    # Every data point must be for Macron…
    assert all(p["candidate_id"] == macron["id"] for p in body["points"])
    # …but the embedded registry still lists every candidate (as a lookup table).
    assert any(c["id"] == macron["id"] for c in body["candidates"])
    assert len(body["candidates"]) >= 14


async def test_timeseries_no_data_window_is_well_formed(aclient):
    # A 1-day window in 1995 should return zero points but still include
    # the full candidate / active-source registries (the frontend needs them
    # as a stable lookup table regardless of the data window).
    r = await aclient.get("/timeseries", params={"from": "1995-01-01", "to": "1995-01-02"})
    assert r.status_code == 200
    body = r.json()
    assert body["points"] == []
    assert body["n_total_mentions"] == 0
    assert len(body["candidates"]) >= 14
    assert len(body["sources"]) >= 1


async def test_timeseries_tz_parameter_accepted(aclient):
    for tz in ("Europe/Paris", "UTC", "America/New_York"):
        r = await aclient.get("/timeseries", params={"tz": tz})
        assert r.status_code == 200, f"tz={tz} failed: {r.text}"
        assert r.json()["tz"] == tz


# --- articles ------------------------------------------------------------


async def test_articles_pagination_shape(aclient):
    r = await aclient.get("/articles", params={"limit": 5, "offset": 0})
    assert r.status_code == 200
    body = r.json()
    assert set(body) == {"items", "total", "limit", "offset"}
    assert body["limit"] == 5
    assert body["offset"] == 0
    assert len(body["items"]) <= 5
    assert body["total"] >= len(body["items"])
    if body["items"]:
        a = body["items"][0]
        for key in ("id", "title", "url", "outlet", "source_id", "published_at", "candidate_ids"):
            assert key in a


async def test_articles_filter_by_candidate(aclient):
    cands = (await aclient.get("/candidates")).json()
    bardella = next(c for c in cands if c["slug"] == "bardella")
    r = await aclient.get("/articles", params={"candidate_id": bardella["id"], "limit": 20})
    assert r.status_code == 200
    body = r.json()
    if body["items"]:
        assert all(bardella["id"] in a["candidate_ids"] for a in body["items"])


async def test_articles_invalid_limit_rejected(aclient):
    r = await aclient.get("/articles", params={"limit": 9999})
    assert r.status_code == 422  # FastAPI validation: ge=1 le=100


async def test_articles_has_mention_partitions_total(aclient):
    # has_mention=true ⊎ has_mention=false should sum to the unfiltered total.
    full = (await aclient.get("/articles", params={"limit": 1})).json()
    with_m = (await aclient.get("/articles", params={"has_mention": "true", "limit": 1})).json()
    no_m = (await aclient.get("/articles", params={"has_mention": "false", "limit": 1})).json()
    assert with_m["total"] + no_m["total"] == full["total"]


async def test_articles_has_mention_false_returns_unmatched_only(aclient):
    r = await aclient.get("/articles", params={"has_mention": "false", "limit": 50})
    assert r.status_code == 200
    body = r.json()
    assert all(a["candidate_ids"] == [] for a in body["items"])


async def test_articles_has_mention_true_returns_matched_only(aclient):
    r = await aclient.get("/articles", params={"has_mention": "true", "limit": 50})
    assert r.status_code == 200
    body = r.json()
    assert all(len(a["candidate_ids"]) >= 1 for a in body["items"])


async def test_articles_candidate_id_takes_precedence_over_has_mention(aclient):
    # When both are passed, candidate_id wins and has_mention is ignored.
    cands = (await aclient.get("/candidates")).json()
    bardella = next(c for c in cands if c["slug"] == "bardella")
    r = await aclient.get(
        "/articles",
        params={"candidate_id": bardella["id"], "has_mention": "false", "limit": 20},
    )
    assert r.status_code == 200
    body = r.json()
    if body["items"]:
        assert all(bardella["id"] in a["candidate_ids"] for a in body["items"])


async def test_openapi_schema_served(aclient):
    r = await aclient.get("/openapi.json")
    assert r.status_code == 200
    schema = r.json()
    paths = schema["paths"]
    assert "/healthz" in paths
    assert "/candidates" in paths
    assert "/sources" in paths
    assert "/timeseries" in paths
    assert "/articles" in paths
