"""A fresh DuckDB warehouse with the raw schema, for each test."""

import pytest

from landing import warehouse


@pytest.fixture
def db_path(tmp_path):
    return str(tmp_path / "warehouse.duckdb")


@pytest.fixture
def db(db_path):
    conn = warehouse.connect(db_path)
    warehouse.ensure_raw_schema(conn)
    yield conn
    conn.close()
