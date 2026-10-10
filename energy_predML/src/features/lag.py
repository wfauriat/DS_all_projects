"""Lagged demand and rolling aggregate features."""

import logging

import pandas as pd

logger = logging.getLogger(__name__)


def add_lag_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add lag and rolling window features based on demand_mwh.

    The DataFrame must be sorted by timestamp before calling this function.

    Args:
        df: DataFrame with 'demand_mwh' column, sorted by timestamp.

    Returns:
        DataFrame with added lag and rolling feature columns.
    """
    df = df.copy()

    # Lag features (past demand values)
    df["demand_lag_1h"] = df["demand_mwh"].shift(1)
    df["demand_lag_24h"] = df["demand_mwh"].shift(24)
    df["demand_lag_48h"] = df["demand_mwh"].shift(48)
    df["demand_lag_168h"] = df["demand_mwh"].shift(168)  # 1 week

    # Rolling aggregates
    df["demand_roll_24h_mean"] = df["demand_mwh"].shift(1).rolling(24).mean()
    df["demand_roll_24h_std"] = df["demand_mwh"].shift(1).rolling(24).std()
    df["demand_roll_7d_mean"] = df["demand_mwh"].shift(1).rolling(168).mean()
    df["demand_roll_7d_std"] = df["demand_mwh"].shift(1).rolling(168).std()

    # Difference features (rate of change, shifted to avoid leakage)
    # diff at time t = demand[t-1] - demand[t-2] (how demand changed in the previous step)
    df["demand_diff_1h"] = df["demand_mwh"].shift(1).diff(1)
    df["demand_diff_24h"] = df["demand_mwh"].shift(1).diff(24)

    lag_cols = [c for c in df.columns if "lag_" in c or "roll_" in c or "diff_" in c]
    logger.info("Added %d lag/rolling features", len(lag_cols))
    return df
