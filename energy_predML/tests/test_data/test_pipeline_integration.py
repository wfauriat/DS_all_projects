"""Integration tests: fetch → validate → store → load pipeline.

These tests wire real components together against an isolated temp DuckDB,
with only the external HTTP layer mocked.
"""

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

from src.data.store import load_demand, load_predictions, log_prediction, upsert_demand
from src.data.validate import validate_records

# Minimal valid EIA-shaped records used across multiple tests
FAKE_RECORDS = [
    {"period": "2026-01-01T00", "respondent": "CISO", "type": "D", "value": 45000},
    {"period": "2026-01-01T01", "respondent": "CISO", "type": "D", "value": 43500},
    {"period": "2026-01-01T02", "respondent": "CISO", "type": "D", "value": 42000},
]


@patch("src.data.fetch.requests.get")
def test_fetch_validate_store_load_roundtrip(mock_get, tmp_duckdb):
    """Full pipeline: fetch → validate → store → load returns expected rows."""
    from src.data.fetch import fetch_demand

    mock_get.return_value.json.return_value = {
        "response": {"total": "3", "data": FAKE_RECORDS}
    }
    mock_get.return_value.raise_for_status = MagicMock()

    records = fetch_demand(region="CISO", start="2026-01-01T00", end="2026-01-01T02")
    validated = validate_records(records)
    n = upsert_demand(validated)
    loaded = load_demand(region="CISO")

    assert n == 3
    assert len(loaded) == 3
    assert set(loaded.columns) >= {"timestamp", "region", "demand_mwh"}
    assert (loaded["region"] == "CISO").all()
    assert (loaded["demand_mwh"] > 0).all()


def test_upsert_is_idempotent(tmp_duckdb):
    """Inserting the same rows twice must not duplicate data."""
    df = validate_records(FAKE_RECORDS)
    upsert_demand(df)
    n_second = upsert_demand(df)

    assert n_second == 0
    assert len(load_demand(region="CISO")) == 3


def test_load_demand_region_filter_isolates_correctly(tmp_duckdb):
    """load_demand with a region filter must exclude rows from other regions."""
    erco_records = [{**r, "respondent": "ERCO"} for r in FAKE_RECORDS]

    upsert_demand(validate_records(FAKE_RECORDS))
    upsert_demand(validate_records(erco_records))

    loaded = load_demand(region="CISO")
    assert len(loaded) == 3
    assert (loaded["region"] == "CISO").all()


def test_log_and_load_predictions_roundtrip(tmp_duckdb):
    """log_prediction → load_predictions must persist and return the value."""
    ts = datetime(2026, 1, 15, 14, 0, 0, tzinfo=UTC)
    log_prediction(timestamp=ts, region="CISO", demand_mwh=45000.0)

    preds = load_predictions(region="CISO")
    assert len(preds) == 1
    assert float(preds.iloc[0]["demand_mwh"]) == pytest.approx(45000.0)


def test_log_prediction_overwrites_on_duplicate_timestamp(tmp_duckdb):
    """Logging the same (timestamp, region) twice must update the row, not duplicate it."""
    ts = datetime(2026, 1, 15, 14, 0, 0, tzinfo=UTC)
    log_prediction(timestamp=ts, region="CISO", demand_mwh=44000.0)
    log_prediction(timestamp=ts, region="CISO", demand_mwh=45000.0)

    preds = load_predictions(region="CISO")
    assert len(preds) == 1
    assert float(preds.iloc[0]["demand_mwh"]) == pytest.approx(45000.0)
