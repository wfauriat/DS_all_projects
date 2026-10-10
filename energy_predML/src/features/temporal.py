"""Time-based feature extraction."""

import logging

import pandas as pd

logger = logging.getLogger(__name__)

# US federal holidays (static list, expandable)
US_HOLIDAYS = {
    "2025-01-01", "2025-01-20", "2025-02-17", "2025-05-26", "2025-07-04",
    "2025-09-01", "2025-10-13", "2025-11-11", "2025-11-27", "2025-12-25",
    "2026-01-01", "2026-01-19", "2026-02-16", "2026-05-25", "2026-07-03",
    "2026-09-07", "2026-10-12", "2026-11-11", "2026-11-26", "2026-12-25",
}


def add_temporal_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add time-based features derived from the timestamp column.

    Args:
        df: DataFrame with a 'timestamp' column.

    Returns:
        DataFrame with added temporal feature columns.
    """
    dt = df["timestamp"].dt

    df = df.copy()
    df["hour_of_day"] = dt.hour
    df["day_of_week"] = dt.dayofweek  # 0=Monday, 6=Sunday
    df["day_of_month"] = dt.day
    df["month"] = dt.month
    df["week_of_year"] = dt.isocalendar().week.astype(int)
    df["is_weekend"] = (df["day_of_week"] >= 5).astype(int)

    # Holiday indicator
    date_strings = dt.date.astype(str)
    df["is_holiday"] = date_strings.isin(US_HOLIDAYS).astype(int)

    # Cyclical encoding for hour and day_of_week
    import numpy as np

    df["hour_sin"] = np.sin(2 * np.pi * df["hour_of_day"] / 24)
    df["hour_cos"] = np.cos(2 * np.pi * df["hour_of_day"] / 24)
    df["dow_sin"] = np.sin(2 * np.pi * df["day_of_week"] / 7)
    df["dow_cos"] = np.cos(2 * np.pi * df["day_of_week"] / 7)

    logger.info("Added temporal features: %d columns", 11)
    return df
