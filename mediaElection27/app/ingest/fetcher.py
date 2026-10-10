"""Async RSS feed fetcher. Bounded parallelism, per-source error capture."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass

import httpx

from app.config import settings


@dataclass
class FetchResult:
    source_id: int
    slug: str
    url: str
    status: int
    body: bytes | None
    etag: str | None
    last_modified: str | None
    elapsed_ms: int
    error: str | None = None


@dataclass
class FetchSpec:
    source_id: int
    slug: str
    url: str
    etag: str | None = None
    last_modified: str | None = None


async def fetch_one(client: httpx.AsyncClient, spec: FetchSpec) -> FetchResult:
    headers: dict[str, str] = {}
    if spec.etag:
        headers["If-None-Match"] = spec.etag
    if spec.last_modified:
        headers["If-Modified-Since"] = spec.last_modified

    started = time.monotonic()
    try:
        resp = await client.get(spec.url, headers=headers)
    except (httpx.RequestError, httpx.HTTPError) as exc:
        return FetchResult(
            source_id=spec.source_id,
            slug=spec.slug,
            url=spec.url,
            status=0,
            body=None,
            etag=None,
            last_modified=None,
            elapsed_ms=int((time.monotonic() - started) * 1000),
            error=f"{type(exc).__name__}: {exc}",
        )

    elapsed_ms = int((time.monotonic() - started) * 1000)
    body = resp.content if resp.status_code == 200 else None
    return FetchResult(
        source_id=spec.source_id,
        slug=spec.slug,
        url=spec.url,
        status=resp.status_code,
        body=body,
        etag=resp.headers.get("ETag"),
        last_modified=resp.headers.get("Last-Modified"),
        elapsed_ms=elapsed_ms,
    )


async def fetch_all(specs: list[FetchSpec]) -> list[FetchResult]:
    sem = asyncio.Semaphore(settings.ingest_max_parallel)
    headers = {
        "User-Agent": settings.ingest_user_agent,
        "Accept": "application/rss+xml, application/atom+xml, application/xml;q=0.9, */*;q=0.8",
    }

    async with httpx.AsyncClient(
        timeout=settings.ingest_timeout_seconds,
        headers=headers,
        follow_redirects=True,
    ) as client:

        async def bounded(spec: FetchSpec) -> FetchResult:
            async with sem:
                return await fetch_one(client, spec)

        return await asyncio.gather(*(bounded(s) for s in specs))
