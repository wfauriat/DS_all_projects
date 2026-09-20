"""AWS Lambda entrypoint for the ingest stage.

Triggered by EventBridge (4x/day). No DB access — reads the source list
from the bundled `seeds/sources.yaml`, fetches every active feed in
parallel, and writes each successful body to S3 at
`feeds/YYYY-MM-DD/{slug}.xml`.

Persistence (parse + dedup + insert + audit) lives in the loader Lambda
and fires on the S3 ObjectCreated event.
"""

from __future__ import annotations

import asyncio
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import boto3
import yaml

from app.ingest.fetcher import FetchSpec, fetch_all

# Resolves to <project_root>/seeds/sources.yaml; relies on the Lambda zip
# being rooted at the project (not at app/), so seeds/ rides along.
SOURCES_YAML = Path(__file__).resolve().parent.parent.parent / "seeds" / "sources.yaml"

_s3 = boto3.client("s3")


def _load_active_sources() -> list[FetchSpec]:
    doc = yaml.safe_load(SOURCES_YAML.read_text(encoding="utf-8"))
    return [
        FetchSpec(source_id=s["id"], slug=s["slug"], url=s["feed_url"])
        for s in doc["sources"]
        if s.get("is_active", True)
    ]


def _put_raw(bucket: str, slug: str, body: bytes) -> str:
    today = datetime.now(UTC).strftime("%Y-%m-%d")
    key = f"feeds/{today}/{slug}.xml"
    _s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/xml")
    return key


async def _run() -> dict[str, Any]:
    bucket = os.environ["RAW_BUCKET"]
    specs = _load_active_sources()
    results = await fetch_all(specs)

    feeds: list[dict[str, Any]] = []
    uploaded = failed = 0
    for r in results:
        entry: dict[str, Any] = {
            "slug": r.slug,
            "http_status": r.status,
            "elapsed_ms": r.elapsed_ms,
        }
        if r.error or r.body is None:
            entry["error"] = r.error or "empty body"
            failed += 1
        else:
            entry["s3_key"] = _put_raw(bucket, r.slug, r.body)
            uploaded += 1
        feeds.append(entry)

    return {
        "fetched": len(results),
        "uploaded": uploaded,
        "failed": failed,
        "feeds": feeds,
    }


def handler(event: dict[str, Any], context: object) -> dict[str, Any]:
    return asyncio.run(_run())
