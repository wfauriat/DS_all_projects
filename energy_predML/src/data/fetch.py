"""EIA API client for fetching hourly electricity demand data."""

import json
import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Optional

import requests

from src.config import RAW_DATA_DIR, settings

logger = logging.getLogger(__name__)

EIA_BASE_URL = "https://api.eia.gov/v2/electricity/rto/region-data/data/"

# EIA API returns at most 5000 rows per request
MAX_ROWS_PER_REQUEST = 5000


def fetch_demand(
    region: Optional[str] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
) -> list[dict[str, Any]]:
    """Fetch hourly electricity demand data from the EIA API.

    Args:
        region: Grid region code (e.g. "CISO", "ERCO"). Defaults to config.
        start: Start datetime string, e.g. "2024-01-01T00". Defaults to 30 days ago.
        end: End datetime string. Defaults to now.

    Returns:
        List of data records from the EIA API.
    """
    region = region or settings.DEFAULT_REGION

    if start is None:
        start = (datetime.now(UTC) - timedelta(days=30)).strftime("%Y-%m-%dT%H")
    if end is None:
        end = datetime.now(UTC).strftime("%Y-%m-%dT%H")

    logger.info("Fetching demand for region=%s from %s to %s", region, start, end)

    all_records: list[dict[str, Any]] = []
    offset = 0

    while True:
        params = {
            "api_key": settings.EIA_API_KEY,
            "facets[respondent][]": region,
            "facets[type][]": "D",
            "data[]": "value",
            "frequency": "hourly",
            "start": start,
            "end": end,
            "sort[0][column]": "period",
            "sort[0][direction]": "asc",
            "offset": offset,
            "length": MAX_ROWS_PER_REQUEST,
        }

        response = requests.get(EIA_BASE_URL, params=params, timeout=30)
        response.raise_for_status()
        payload = response.json()

        data = payload.get("response", {}).get("data", [])
        total = int(payload.get("response", {}).get("total", 0))

        if not data:
            break

        all_records.extend(data)
        offset += len(data)
        logger.info("Fetched %d / %d records", len(all_records), total)

        if offset >= total:
            break

    logger.info("Total records fetched: %d", len(all_records))
    return all_records


def save_raw_response(records: list[dict[str, Any]], region: str) -> Path:
    """Save raw API records to a timestamped JSON file.

    Args:
        records: List of EIA API data records.
        region: Grid region code used for the filename.

    Returns:
        Path to the saved JSON file.
    """
    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    filepath = RAW_DATA_DIR / f"{region}_{timestamp}.json"

    with open(filepath, "w") as f:
        json.dump(records, f)

    logger.info("Saved %d records to %s", len(records), filepath)
    return filepath
