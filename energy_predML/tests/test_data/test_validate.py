"""Tests for data validation."""

import pandas as pd
import pytest

from src.data.validate import validate_records


def test_valid_records():
    records = [
        {"period": "2026-01-01T00", "respondent": "CISO", "type": "D", "value": 25000},
        {"period": "2026-01-01T01", "respondent": "CISO", "type": "D", "value": 24000},
    ]
    df = validate_records(records)
    assert len(df) == 2
    assert list(df.columns) == ["timestamp", "region", "demand_mwh"]
    assert df["demand_mwh"].iloc[0] == 25000


def test_empty_records_raises():
    with pytest.raises(ValueError, match="No records"):
        validate_records([])


def test_missing_columns_raises():
    records = [{"period": "2026-01-01T00", "respondent": "CISO"}]
    with pytest.raises(ValueError, match="Missing required columns"):
        validate_records(records)


def test_null_values_dropped():
    records = [
        {"period": "2026-01-01T00", "respondent": "CISO", "type": "D", "value": 25000},
        {"period": "2026-01-01T01", "respondent": "CISO", "type": "D", "value": None},
        {"period": "2026-01-01T02", "respondent": "CISO", "type": "D", "value": "bad"},
    ]
    df = validate_records(records)
    assert len(df) == 1  # only the first record is valid


def test_output_sorted_by_timestamp():
    records = [
        {"period": "2026-01-01T02", "respondent": "CISO", "type": "D", "value": 26000},
        {"period": "2026-01-01T00", "respondent": "CISO", "type": "D", "value": 24000},
        {"period": "2026-01-01T01", "respondent": "CISO", "type": "D", "value": 25000},
    ]
    df = validate_records(records)
    assert df["demand_mwh"].tolist() == [24000, 25000, 26000]
