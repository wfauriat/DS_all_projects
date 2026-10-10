"""Feature engineering pipeline: load raw data, transform, save to Parquet."""

import logging
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from src.config import PROCESSED_DATA_DIR
from src.data.store import load_demand
from src.features.lag import add_lag_features
from src.features.temporal import add_temporal_features

logger = logging.getLogger(__name__)

# Columns that are metadata/target, not model input features
NON_FEATURE_COLS = {"timestamp", "region", "demand_mwh"}


def build_features(
    region: str | None = None,
    start: str | None = None,
    end: str | None = None,
    drop_nulls: bool = True,
) -> pd.DataFrame:
    """Load raw demand data and apply all feature transformations.

    Args:
        region: Filter by region code.
        start: Filter start datetime.
        end: Filter end datetime.
        drop_nulls: Whether to drop rows with NaN from lag features.

    Returns:
        Feature-engineered DataFrame ready for model training.
    """
    df = load_demand(region=region, start=start, end=end)

    if df.empty:
        raise ValueError("No data found in DuckDB for the given filters")

    df = df.sort_values("timestamp").reset_index(drop=True)

    # Apply feature transformations
    df = add_temporal_features(df)
    df = add_lag_features(df)

    if drop_nulls:
        before = len(df)
        df = df.dropna().reset_index(drop=True)
        dropped = before - len(df)
        if dropped > 0:
            logger.info("Dropped %d rows with NaN (from lag warm-up)", dropped)

    # Validate: no nulls, no negative demand
    null_count = df.isna().sum().sum()
    if null_count > 0:
        raise ValueError(f"Feature DataFrame still contains {null_count} null values")

    logger.info(
        "Built features: %d rows, %d columns, range %s to %s",
        len(df),
        len(df.columns),
        df["timestamp"].min(),
        df["timestamp"].max(),
    )
    return df


def save_features(df: pd.DataFrame) -> Path:
    """Save feature DataFrame to a versioned Parquet file.

    Args:
        df: Feature-engineered DataFrame.

    Returns:
        Path to the saved Parquet file.
    """
    PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    filepath = PROCESSED_DATA_DIR / f"features_v{timestamp}.parquet"

    df.to_parquet(filepath, index=False)
    logger.info("Saved features to %s (%d rows)", filepath, len(df))
    return filepath


def get_latest_features() -> pd.DataFrame:
    """Load the most recent feature Parquet file.

    Returns:
        DataFrame from the latest versioned Parquet file.

    Raises:
        FileNotFoundError: If no feature files exist.
    """
    PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)
    files = sorted(PROCESSED_DATA_DIR.glob("features_v*.parquet"))

    if not files:
        raise FileNotFoundError("No feature files found in processed directory")

    latest = files[-1]
    df = pd.read_parquet(latest)
    logger.info("Loaded features from %s (%d rows)", latest, len(df))
    return df


if __name__ == "__main__":
    from src.config import settings

    df = build_features(region=settings.DEFAULT_REGION)
    path = save_features(df)
    print(f"Features saved to {path}")
    print(f"Shape: {df.shape}")
    print(f"Columns: {list(df.columns)}")
