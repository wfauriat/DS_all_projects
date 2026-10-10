"""Prefect flow for data ingestion: fetch, validate, store."""

import logging
from typing import Optional

from prefect import flow, task
from prefect.tasks import task_input_hash

from src.data.fetch import fetch_demand, save_raw_response
from src.data.store import upsert_demand
from src.data.validate import validate_records

logger = logging.getLogger(__name__)


@task(retries=3, retry_delay_seconds=60, cache_key_fn=task_input_hash)
def fetch_data(region: str, start: Optional[str], end: Optional[str]) -> list:
    """Fetch demand data from EIA API with retries."""
    return fetch_demand(region=region, start=start, end=end)


@task
def save_raw(records: list, region: str) -> str:
    """Save raw API response to disk."""
    path = save_raw_response(records, region)
    return str(path)


@task
def validate_data(records: list):
    """Validate raw records and return cleaned DataFrame."""
    return validate_records(records)


@task
def store_data(df) -> int:
    """Store validated data into DuckDB."""
    return upsert_demand(df)


@flow(name="data-ingestion")
def ingest_flow(
    region: Optional[str] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
) -> int:
    """Run the full data ingestion pipeline.

    Args:
        region: Grid region code. Defaults to config DEFAULT_REGION.
        start: Start datetime string. Defaults to last 30 days.
        end: End datetime string. Defaults to now.

    Returns:
        Number of new rows inserted.
    """
    from src.config import settings

    region = region or settings.DEFAULT_REGION

    records = fetch_data(region, start, end)
    save_raw(records, region)
    df = validate_data(records)
    inserted = store_data(df)

    logger.info("Ingestion complete: %d new rows for %s", inserted, region)
    return inserted


if __name__ == "__main__":
    ingest_flow()
