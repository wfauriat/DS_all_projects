"""Tests for temporal feature engineering."""

import numpy as np
import pandas as pd
import pytest

from src.features.temporal import add_temporal_features


@pytest.fixture
def sample_df():
    """Create a sample DataFrame spanning 48 hours."""
    timestamps = pd.date_range("2026-02-14 00:00", periods=48, freq="h", tz="UTC")
    return pd.DataFrame({
        "timestamp": timestamps,
        "region": "CISO",
        "demand_mwh": np.random.uniform(20000, 30000, 48),
    })


def test_adds_expected_columns(sample_df):
    result = add_temporal_features(sample_df)
    expected_cols = [
        "hour_of_day", "day_of_week", "day_of_month", "month",
        "week_of_year", "is_weekend", "is_holiday",
        "hour_sin", "hour_cos", "dow_sin", "dow_cos",
    ]
    for col in expected_cols:
        assert col in result.columns, f"Missing column: {col}"


def test_hour_of_day_range(sample_df):
    result = add_temporal_features(sample_df)
    assert result["hour_of_day"].min() >= 0
    assert result["hour_of_day"].max() <= 23


def test_is_weekend_binary(sample_df):
    result = add_temporal_features(sample_df)
    assert set(result["is_weekend"].unique()).issubset({0, 1})


def test_weekend_detection(sample_df):
    result = add_temporal_features(sample_df)
    # Feb 14 2026 is Saturday, Feb 15 is Sunday
    saturday_rows = result[result["day_of_week"] == 5]
    assert (saturday_rows["is_weekend"] == 1).all()


def test_cyclical_encoding_bounds(sample_df):
    result = add_temporal_features(sample_df)
    for col in ["hour_sin", "hour_cos", "dow_sin", "dow_cos"]:
        assert result[col].min() >= -1.0
        assert result[col].max() <= 1.0


def test_does_not_modify_original(sample_df):
    original_cols = list(sample_df.columns)
    add_temporal_features(sample_df)
    assert list(sample_df.columns) == original_cols
