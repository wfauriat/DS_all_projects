"""AWS Lambda entrypoint for the loader stage.

Triggered by S3 ObjectCreated on the raw bucket. For each record:
parse the XML body, dedup, insert into `articles`, and write an
`ingest_runs` audit row. Reuses `_persist_articles` and `_record_run`
from app.ingest.run so persistence logic lives in one place. After
all records are persisted, runs the keyword matcher inline so
mentions stay in step with articles.

DB credentials arrive as plaintext Lambda env vars (resolved from
Secrets Manager by CloudFormation at deploy time). The cold-start
hook stitches them into the sync DSN before any SQLAlchemy engine
is built. We avoid a runtime Secrets Manager call because the
loader runs in PRIVATE_ISOLATED subnets with no path to AWS public
APIs.

Also dispatches one-off admin actions when invoked with a non-S3
payload: `{"action": "migrate"}` runs `alembic upgrade head` against
the in-VPC RDS, `{"action": "seed"}` runs `app.sources.seed.main()`,
and `{"action": "extract", "reprocess_all": false}` runs the keyword
matcher over articles (equivalent of local `make extract`).
"""

from __future__ import annotations

import os
import urllib.parse
from datetime import UTC, datetime
from typing import Any

import boto3


def _configure_db_url_from_env() -> None:
    """Cold-start hook: build the sync DSN before the engine is created.

    Only runs inside the Lambda runtime (detected via AWS_LAMBDA_FUNCTION_NAME)
    so local imports of this module don't touch settings.
    """
    if "AWS_LAMBDA_FUNCTION_NAME" not in os.environ:
        return

    from app.config import settings

    user = urllib.parse.quote(os.environ["DB_USERNAME"], safe="")
    pwd = urllib.parse.quote(os.environ["DB_PASSWORD"], safe="")
    host = os.environ["DB_HOST"]
    port = os.environ.get("DB_PORT", "5432")
    name = os.environ["DB_NAME"]
    settings.database_url_sync = (
        f"postgresql+psycopg://{user}:{pwd}@{host}:{port}/{name}"
    )


_configure_db_url_from_env()

# Imports below trigger the engine module — must be after the DSN is fixed.
from sqlalchemy import select  # noqa: E402

from app.db.engine import get_sync_sessionmaker  # noqa: E402
from app.db.models import Source  # noqa: E402
from app.ingest.fetcher import FetchResult  # noqa: E402
from app.ingest.run import _persist_articles, _record_run  # noqa: E402

_s3 = boto3.client("s3")


def _process_record(bucket: str, key: str) -> dict[str, Any]:
    # Key format: feeds/YYYY-MM-DD/{slug}.xml
    slug = key.rsplit("/", 1)[-1].removesuffix(".xml")

    sm = get_sync_sessionmaker()

    with sm() as session:
        source = session.execute(
            select(Source).where(Source.slug == slug)
        ).scalar_one_or_none()

    if source is None:
        return {"slug": slug, "error": f"unknown source slug: {slug}"}

    body = _s3.get_object(Bucket=bucket, Key=key)["Body"].read()

    fetch = FetchResult(
        source_id=source.id,
        slug=source.slug,
        url=source.feed_url,
        status=200,
        body=body,
        etag=None,
        last_modified=None,
        elapsed_ms=0,
        error=None,
    )

    started = datetime.now(UTC)
    n_items = n_inserted = n_skipped = 0
    error: str | None = None
    try:
        with sm() as session, session.begin():
            n_items, n_inserted, n_skipped = _persist_articles(
                session, source.id, body
            )
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"

    with sm() as session, session.begin():
        _record_run(
            session,
            source_id=source.id,
            started_at=started,
            fetch=fetch,
            n_items=n_items,
            n_inserted=n_inserted,
            n_skipped=n_skipped,
            error=error,
        )

    return {
        "slug": slug,
        "n_items": n_items,
        "n_inserted": n_inserted,
        "n_skipped": n_skipped,
        "error": error,
    }


def _run_migrate() -> dict[str, Any]:
    from pathlib import Path

    from alembic import command
    from alembic.config import Config

    # `app.ingest.loader_handler` -> .../app/ingest/loader_handler.py
    # parents[2] is the project root, where alembic.ini is bundled.
    project_root = Path(__file__).resolve().parents[2]
    cfg = Config(str(project_root / "alembic.ini"))
    cfg.set_main_option("script_location", str(project_root / "app" / "db" / "migrations"))

    from app.config import settings

    cfg.set_main_option("sqlalchemy.url", settings.database_url_sync)
    command.upgrade(cfg, "head")
    return {"action": "migrate", "status": "ok"}


def _run_seed() -> dict[str, Any]:
    from app.sources.seed import main as seed_main

    seed_main()
    return {"action": "seed", "status": "ok"}


def _run_extract(reprocess_all: bool) -> dict[str, Any]:
    from app.extract.run import run_extract

    inserted = run_extract(reprocess_all=reprocess_all)
    return {
        "action": "extract",
        "status": "ok",
        "inserted": inserted,
        "reprocess_all": reprocess_all,
    }


def handler(event: dict[str, Any], context: object) -> dict[str, Any]:
    action = event.get("action") if isinstance(event, dict) else None
    if action == "migrate":
        return _run_migrate()
    if action == "seed":
        return _run_seed()
    if action == "extract":
        reprocess_all = bool(event.get("reprocess_all", False))
        return _run_extract(reprocess_all)

    results: list[dict[str, Any]] = []
    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])
        results.append(_process_record(bucket, key))

    # Run the keyword matcher inline after persistence so mentions stay
    # in step with articles. `run_extract(reprocess_all=False)` skips any
    # article that already has a mention from this extractor+version, so
    # repeated invocations are cheap. Failures are recorded but never
    # propagate — persistence is the loader's primary job; matching can
    # be backfilled by a manual `{"action": "extract"}` invoke.
    n_inserted = sum(r.get("n_inserted", 0) for r in results if r.get("n_inserted"))
    extract: dict[str, Any] | None = None
    if n_inserted > 0:
        try:
            extract = _run_extract(reprocess_all=False)
        except Exception as exc:
            extract = {"error": f"{type(exc).__name__}: {exc}"}

    return {"processed": len(results), "items": results, "extract": extract}
