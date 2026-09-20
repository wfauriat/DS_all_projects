"""Shared fixtures + skip-all guard for integration tests.

Integration tests run against the live dev DB (the same one `make psql` connects
to). They are auto-skipped when Postgres isn't reachable so `make test` stays
green on machines without the docker container up.

Uses `httpx.AsyncClient` + ASGITransport against the FastAPI app with a
session-scoped event loop, so the async DB engine's connection pool is
reused cleanly across all tests.
"""

from __future__ import annotations

import socket
from collections.abc import AsyncIterator

import httpx
import pytest
import pytest_asyncio

from app.api.main import app


def _postgres_reachable(host: str = "localhost", port: int = 5432, timeout: float = 0.5) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


pytestmark = pytest.mark.skipif(
    not _postgres_reachable(),
    reason="Postgres not reachable on localhost:5432 — integration tests skipped",
)


@pytest_asyncio.fixture(scope="session")
async def aclient() -> AsyncIterator[httpx.AsyncClient]:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client
