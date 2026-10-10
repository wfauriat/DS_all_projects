"""DuckDB storage layer for demand data and logged predictions."""

import logging
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pandas as pd

from src.config import DATA_DIR

logger = logging.getLogger(__name__)

DB_PATH = DATA_DIR / "energy.duckdb"

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS demand (
    timestamp TIMESTAMPTZ NOT NULL,
    region VARCHAR NOT NULL,
    demand_mwh DOUBLE NOT NULL,
    PRIMARY KEY (timestamp, region)
)
"""

CREATE_PREDICTIONS_SQL = """
CREATE TABLE IF NOT EXISTS predictions (
    timestamp    TIMESTAMPTZ NOT NULL,
    region       VARCHAR     NOT NULL,
    demand_mwh   DOUBLE      NOT NULL,
    predicted_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (timestamp, region)
)
"""


def get_connection(read_only: bool = False) -> duckdb.DuckDBPyConnection:
    """Get a DuckDB connection, creating the database and tables if needed.

    Args:
        read_only: Open in read-only mode (allows concurrent readers).
                   Falls back to read-write if the database does not exist yet.
    """
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    # read_only requires the file to exist; fall back to read-write if it doesn't
    effective_read_only = read_only and DB_PATH.exists()
    conn = duckdb.connect(str(DB_PATH), read_only=effective_read_only)
    if not effective_read_only:
        conn.execute(CREATE_TABLE_SQL)
        conn.execute(CREATE_PREDICTIONS_SQL)
    return conn


def upsert_demand(df: pd.DataFrame) -> int:
    """Insert demand data, skipping rows that already exist (idempotent).

    Args:
        df: DataFrame with columns: timestamp, region, demand_mwh.

    Returns:
        Number of new rows inserted.
    """
    conn = get_connection()

    # Register the DataFrame as a temporary view
    conn.register("incoming", df)

    # Count existing rows before insert
    before = conn.execute("SELECT COUNT(*) FROM demand").fetchone()[0]

    # Insert only rows that don't already exist
    conn.execute("""
        INSERT INTO demand (timestamp, region, demand_mwh)
        SELECT i.timestamp, i.region, i.demand_mwh
        FROM incoming i
        WHERE NOT EXISTS (
            SELECT 1 FROM demand d
            WHERE d.timestamp = i.timestamp AND d.region = i.region
        )
    """)

    after = conn.execute("SELECT COUNT(*) FROM demand").fetchone()[0]
    inserted = after - before

    conn.unregister("incoming")
    conn.close()

    logger.info("Upserted: %d new rows (%d total)", inserted, after)
    return inserted


def load_demand(
    region: str | None = None,
    start: str | None = None,
    end: str | None = None,
) -> pd.DataFrame:
    """Load demand data from DuckDB with optional filters.

    Args:
        region: Filter by region code.
        start: Filter start datetime (inclusive).
        end: Filter end datetime (inclusive).

    Returns:
        DataFrame with columns: timestamp, region, demand_mwh.
    """
    conn = get_connection(read_only=True)

    query = "SELECT timestamp, region, demand_mwh FROM demand WHERE 1=1"
    params: list = []

    if region:
        query += " AND region = ?"
        params.append(region)
    if start:
        query += " AND timestamp >= ?"
        params.append(start)
    if end:
        query += " AND timestamp <= ?"
        params.append(end)

    query += " ORDER BY timestamp"

    df = conn.execute(query, params).fetchdf()
    conn.close()

    logger.info("Loaded %d rows from DuckDB", len(df))
    return df


def log_prediction(timestamp: datetime, region: str, demand_mwh: float) -> None:
    """Upsert a prediction into the predictions table.

    Overwrites an existing row if the same (timestamp, region) already exists,
    keeping only the most recent prediction for each slot.

    Args:
        timestamp: Target hour being predicted.
        region: Grid region code.
        demand_mwh: Predicted demand in MWh.
    """
    ts_utc = pd.Timestamp(timestamp, tz="UTC") if getattr(timestamp, "tzinfo", None) is None \
        else pd.Timestamp(timestamp).tz_convert("UTC")
    conn = get_connection()
    conn.execute(
        """
        INSERT INTO predictions (timestamp, region, demand_mwh, predicted_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT (timestamp, region) DO UPDATE SET
            demand_mwh   = excluded.demand_mwh,
            predicted_at = excluded.predicted_at
        """,
        [ts_utc, region, round(demand_mwh, 1), datetime.now(UTC)],
    )
    conn.close()
    logger.debug("Logged prediction: %s [%s] = %.1f MWh", ts_utc, region, demand_mwh)


def load_predictions(
    region: str | None = None,
    start: str | None = None,
    end: str | None = None,
) -> pd.DataFrame:
    """Load logged predictions from DuckDB with optional filters.

    Args:
        region: Filter by region code.
        start: Filter start datetime (inclusive).
        end: Filter end datetime (inclusive).

    Returns:
        DataFrame with columns: timestamp, region, demand_mwh, predicted_at.
    """
    conn = get_connection(read_only=True)

    query = "SELECT timestamp, region, demand_mwh, predicted_at FROM predictions WHERE 1=1"
    params: list = []

    if region:
        query += " AND region = ?"
        params.append(region)
    if start:
        query += " AND timestamp >= ?"
        params.append(start)
    if end:
        query += " AND timestamp <= ?"
        params.append(end)

    query += " ORDER BY timestamp"

    df = conn.execute(query, params).fetchdf()
    conn.close()

    logger.info("Loaded %d predictions from DuckDB", len(df))
    return df
