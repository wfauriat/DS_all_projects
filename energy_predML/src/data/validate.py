"""Schema validation for EIA demand data."""

import logging
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

REQUIRED_COLUMNS = {"period", "respondent", "type", "value"}


def validate_records(records: list[dict[str, Any]]) -> pd.DataFrame:
    """Validate raw EIA records and return a cleaned DataFrame.

    Args:
        records: List of raw EIA API data records.

    Returns:
        Validated DataFrame with columns: timestamp, region, demand_mwh.

    Raises:
        ValueError: If records are empty or missing required columns.
    """
    if not records:
        raise ValueError("No records to validate")

    df = pd.DataFrame(records)

    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    # Parse and clean
    df["timestamp"] = pd.to_datetime(df["period"], utc=True)
    df["region"] = df["respondent"].astype(str)
    df["demand_mwh"] = pd.to_numeric(df["value"], errors="coerce")

    # Drop rows where demand is null
    null_count = df["demand_mwh"].isna().sum()
    if null_count > 0:
        logger.warning("Dropping %d rows with null demand values", null_count)
        df = df.dropna(subset=["demand_mwh"])

    # Select and sort
    df = df[["timestamp", "region", "demand_mwh"]].sort_values("timestamp").reset_index(drop=True)

    logger.info(
        "Validated %d records: %s to %s",
        len(df),
        df["timestamp"].min(),
        df["timestamp"].max(),
    )
    return df
