"""Integration tests: DuckDB data → build_features pipeline.

Verifies that the full load_demand → temporal features → lag features
chain produces correct, leak-free output.
"""

import pytest

from src.features.pipeline import NON_FEATURE_COLS, build_features


def test_build_features_returns_no_nulls(seed_demand):
    """After the lag warm-up window is dropped, the result must have no NaN values."""
    df = build_features(region="CISO")
    assert not df.isna().any().any(), "Unexpected NaN values in feature DataFrame"


def test_build_features_raises_on_empty_db(tmp_duckdb):
    """build_features must raise ValueError when DuckDB contains no rows."""
    with pytest.raises(ValueError, match="No data found"):
        build_features(region="CISO")


def test_metadata_columns_are_preserved(seed_demand):
    """timestamp, region, and demand_mwh must be present alongside feature columns."""
    df = build_features(region="CISO")
    for col in NON_FEATURE_COLS:
        assert col in df.columns, f"Metadata column missing: {col}"


def test_feature_columns_are_non_empty(seed_demand):
    """There must be at least one column beyond the metadata columns."""
    df = build_features(region="CISO")
    feature_cols = [c for c in df.columns if c not in NON_FEATURE_COLS]
    assert len(feature_cols) > 0


def test_expected_temporal_features_present(seed_demand):
    """Core temporal features must all be present in the output."""
    df = build_features(region="CISO")
    for col in ["hour_of_day", "day_of_week", "month", "is_weekend"]:
        assert col in df.columns, f"Temporal feature missing: {col}"


def test_expected_lag_features_present(seed_demand):
    """Core lag features must all be present in the output."""
    df = build_features(region="CISO")
    for col in ["demand_lag_1h", "demand_lag_24h", "demand_lag_168h", "demand_roll_7d_mean"]:
        assert col in df.columns, f"Lag feature missing: {col}"


def test_lag_24h_matches_demand_24_rows_prior(seed_demand):
    """demand_lag_24h at row N must equal demand_mwh at row N-24 (no leakage)."""
    df = build_features(region="CISO").reset_index(drop=True)

    # Check a range of rows well after the 168h warm-up window
    for i in range(50, 70):
        expected = df.iloc[i - 24]["demand_mwh"]
        actual = df.iloc[i]["demand_lag_24h"]
        assert abs(actual - expected) < 0.01, (
            f"Lag-24h mismatch at row {i}: got {actual}, expected {expected}"
        )


def test_row_count_accounts_for_warmup(seed_demand):
    """After dropping the 168h warm-up NaN rows, most data should remain."""
    df = build_features(region="CISO")
    # 400 rows seeded; ~168 dropped for warm-up → at least 200 remaining
    assert len(df) >= 200, f"Unexpectedly few rows after warm-up: {len(df)}"
