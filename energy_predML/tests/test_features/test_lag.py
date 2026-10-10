"""Tests for lag feature engineering."""

import numpy as np
import pandas as pd
import pytest

from src.features.lag import add_lag_features


@pytest.fixture
def sample_df():
    """Create a sorted DataFrame with 200 hourly rows."""
    return pd.DataFrame({
        "timestamp": pd.date_range("2026-01-01", periods=200, freq="h", tz="UTC"),
        "region": "CISO",
        "demand_mwh": np.arange(1000, 1200, dtype=float),
    })


def test_adds_expected_columns(sample_df):
    result = add_lag_features(sample_df)
    expected = [
        "demand_lag_1h", "demand_lag_24h", "demand_lag_48h", "demand_lag_168h",
        "demand_roll_24h_mean", "demand_roll_24h_std",
        "demand_roll_7d_mean", "demand_roll_7d_std",
        "demand_diff_1h", "demand_diff_24h",
    ]
    for col in expected:
        assert col in result.columns, f"Missing column: {col}"


def test_lag_1h_correct(sample_df):
    result = add_lag_features(sample_df)
    # lag_1h at index 5 should equal demand at index 4
    assert result.iloc[5]["demand_lag_1h"] == sample_df.iloc[4]["demand_mwh"]


def test_lag_24h_correct(sample_df):
    result = add_lag_features(sample_df)
    assert result.iloc[30]["demand_lag_24h"] == sample_df.iloc[6]["demand_mwh"]


def test_lag_creates_nans_at_start(sample_df):
    result = add_lag_features(sample_df)
    # First row should have NaN for lag_1h
    assert pd.isna(result.iloc[0]["demand_lag_1h"])
    # First 168 rows should have NaN for lag_168h
    assert pd.isna(result.iloc[167]["demand_lag_168h"])
    assert not pd.isna(result.iloc[168]["demand_lag_168h"])


def test_no_data_leakage_in_diff(sample_df):
    """Diff features must not use the current target value."""
    result = add_lag_features(sample_df)
    # demand_diff_1h[t] = demand[t-1] - demand[t-2], NOT demand[t] - demand[t-1]
    # At index 3: diff = demand[2] - demand[1] = 1002 - 1001 = 1
    assert result.iloc[3]["demand_diff_1h"] == pytest.approx(1.0)


def test_rolling_mean_uses_shifted_data(sample_df):
    """Rolling mean should not include the current row's demand."""
    result = add_lag_features(sample_df)
    # At index 25: roll_24h_mean should be mean of demand[1..24] (shifted by 1)
    expected = sample_df.iloc[1:25]["demand_mwh"].mean()
    assert result.iloc[25]["demand_roll_24h_mean"] == pytest.approx(expected)


def test_does_not_modify_original(sample_df):
    original_cols = list(sample_df.columns)
    add_lag_features(sample_df)
    assert list(sample_df.columns) == original_cols
