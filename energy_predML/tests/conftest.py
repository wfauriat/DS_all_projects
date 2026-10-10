"""Shared fixtures for integration tests."""

import numpy as np
import pandas as pd
import pytest


@pytest.fixture
def tmp_duckdb(tmp_path, monkeypatch):
    """Redirect DuckDB to an isolated temp file so tests never touch data/energy.duckdb."""
    db = tmp_path / "test.duckdb"
    monkeypatch.setattr("src.data.store.DB_PATH", db)
    return db


@pytest.fixture
def seed_demand(tmp_duckdb):
    """Insert 400 hours of sinusoidal demand data into the temp DB.

    Starts 2026-01-01 00:00 UTC, ends 2026-01-17 15:00 UTC.
    Provides enough history (>168h) for all lag/rolling features.
    """
    from src.data.store import upsert_demand

    rng = np.random.default_rng(42)
    timestamps = pd.date_range("2026-01-01", periods=400, freq="h", tz="UTC")
    demand = (
        40_000
        + 5_000 * np.sin(2 * np.pi * timestamps.hour / 24)
        + rng.normal(0, 200, 400)
    )
    df = pd.DataFrame(
        {
            "timestamp": timestamps,
            "region": "CISO",
            "demand_mwh": demand.round(1),
        }
    )
    upsert_demand(df)
    return df
