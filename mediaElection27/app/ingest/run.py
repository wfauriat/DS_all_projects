"""Local ingest CLI: fetches all active feeds, parses, and writes to Postgres.

In production (Week 4+) the fetch step lives in a Lambda outside the VPC, raw
bytes go to S3, and an S3-triggered loader Lambda handles parse + write. This
script collapses both halves for local dev.

Usage:
    python -m app.ingest.run --once
    python -m app.ingest.run --once --source lemonde-politique
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import UTC, datetime
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from app.config import settings
from app.db.engine import get_sync_sessionmaker
from app.db.models import Article, IngestRun, Source
from app.ingest.dedup import canonical_url, content_hash
from app.ingest.fetcher import FetchResult, FetchSpec, fetch_all
from app.ingest.parser import parse_feed


def _save_raw(result: FetchResult, root: Path) -> Path | None:
    if result.body is None:
        return None
    today = datetime.now(UTC).strftime("%Y-%m-%d")
    out_dir = root / today
    out_dir.mkdir(parents=True, exist_ok=True)
    ext = ".xml"
    path = out_dir / f"{result.slug}{ext}"
    path.write_bytes(result.body)
    return path


def _persist_articles(session: Session, source_id: int, body: bytes) -> tuple[int, int, int]:
    """Returns (n_items_seen, n_inserted, n_skipped_dup)."""
    entries = parse_feed(body)
    inserted = 0
    skipped = 0
    for e in entries:
        url = canonical_url(e.url)
        h = content_hash(e.title, e.summary)
        stmt = (
            pg_insert(Article)
            .values(
                source_id=source_id,
                guid=e.guid,
                url=url,
                title=e.title,
                summary=e.summary,
                published_at=e.published_at,
                lang="fr",
                raw=e.raw,
                content_hash=h,
            )
            .on_conflict_do_nothing(index_elements=[Article.source_id, Article.guid])
            .returning(Article.id)
        )
        result = session.execute(stmt).scalar_one_or_none()
        if result is not None:
            inserted += 1
        else:
            skipped += 1
    return len(entries), inserted, skipped


def _record_run(
    session: Session,
    *,
    source_id: int,
    started_at: datetime,
    fetch: FetchResult,
    n_items: int,
    n_inserted: int,
    n_skipped: int,
    error: str | None,
) -> None:
    if error:
        status = "failed"
    elif n_inserted == 0 and n_items == 0:
        status = "partial"
    else:
        status = "ok"

    session.execute(
        pg_insert(IngestRun).values(
            source_id=source_id,
            started_at=started_at,
            finished_at=datetime.now(UTC),
            status=status,
            feed_http_status=fetch.status,
            n_items_seen=n_items,
            n_articles_inserted=n_inserted,
            n_articles_skipped_dup=n_skipped,
            n_mentions_inserted=0,
            error=error,
            meta={
                "etag": fetch.etag,
                "last_modified": fetch.last_modified,
                "elapsed_ms": fetch.elapsed_ms,
                "feed_url": fetch.url,
            },
        )
    )


async def run_once(slug_filter: str | None = None) -> int:
    sm = get_sync_sessionmaker()

    with sm() as session:
        stmt = select(Source).where(Source.is_active.is_(True))
        if slug_filter:
            stmt = stmt.where(Source.slug == slug_filter)
        sources = session.execute(stmt).scalars().all()

    if not sources:
        print("No active sources matched.", file=sys.stderr)
        return 0

    specs = [FetchSpec(source_id=s.id, slug=s.slug, url=s.feed_url) for s in sources]
    print(f"Fetching {len(specs)} feed(s)...", file=sys.stderr)
    results = await fetch_all(specs)

    total_inserted = 0
    settings.raw_feed_dir.mkdir(parents=True, exist_ok=True)

    for result in results:
        started = datetime.now(UTC)
        n_items = n_inserted = n_skipped = 0
        error: str | None = None

        if result.body is not None:
            _save_raw(result, settings.raw_feed_dir)

        # Transaction 1: persist articles. On failure, the txn rolls back cleanly
        # so it can't poison the ingest_runs write that follows.
        if result.error:
            error = result.error
        elif result.status != 200:
            error = f"HTTP {result.status}"
        elif result.body is None:
            error = "empty body"
        else:
            try:
                with sm() as session, session.begin():
                    n_items, n_inserted, n_skipped = _persist_articles(
                        session, result.source_id, result.body
                    )
            except Exception as exc:
                error = f"{type(exc).__name__}: {exc}"

        # Transaction 2: always record the run, independent of (1).
        with sm() as session, session.begin():
            _record_run(
                session,
                source_id=result.source_id,
                started_at=started,
                fetch=result,
                n_items=n_items,
                n_inserted=n_inserted,
                n_skipped=n_skipped,
                error=error,
            )

        total_inserted += n_inserted
        status_tag = "OK " if error is None else "ERR"
        print(
            f"{status_tag} {result.slug:<25} http={result.status:<3} "
            f"items={n_items:<3} new={n_inserted:<3} dup={n_skipped:<3} "
            f"{result.elapsed_ms}ms" + (f"  [{error}]" if error else ""),
            file=sys.stderr,
        )

    print(
        f"\nTotal new articles: {total_inserted} across {len(results)} feeds.",
        file=sys.stderr,
    )
    return total_inserted


def main() -> None:
    p = argparse.ArgumentParser(prog="ingest", description="Run one ingest pass")
    p.add_argument("--once", action="store_true", help="Run a single pass and exit")
    p.add_argument("--source", help="Only ingest the source with this slug")
    args = p.parse_args()

    if not args.once:
        p.error("--once is required (continuous mode is the EventBridge schedule in prod)")
    asyncio.run(run_once(args.source))


if __name__ == "__main__":
    main()
