"""CLI entry point for running the keyword extractor against the article store.

Idempotent: skips articles that already have at least one mention for the
current (extractor, extractor_version). Re-runs after seeding new aliases or
adjusting `requires_context` rules are safe — they only insert new rows.

Usage:
    python -m app.extract.run                # process pending articles
    python -m app.extract.run --all          # reprocess everything (still
                                             # idempotent via the UNIQUE
                                             # constraint on mentions)
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime

from sqlalchemy import exists, not_, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from app.db.engine import get_sync_sessionmaker
from app.db.models import Article, CandidateAlias, Mention
from app.extract.base import AliasSpec, MentionDraft
from app.extract.keyword import KeywordExtractor


def _load_aliases(session: Session) -> list[AliasSpec]:
    rows = (
        session.execute(select(CandidateAlias).where(CandidateAlias.is_active.is_(True)))
        .scalars()
        .all()
    )
    return [
        AliasSpec(
            candidate_id=r.candidate_id,
            alias=r.alias,
            match_kind=r.match_kind,
            requires_context=r.requires_context,
        )
        for r in rows
    ]


def _insert_drafts(session: Session, drafts: list[MentionDraft]) -> int:
    """UPSERT mentions, returning the count actually inserted (excluding dup-noops)."""
    n = 0
    for d in drafts:
        stmt = (
            pg_insert(Mention)
            .values(
                article_id=d.article_id,
                candidate_id=d.candidate_id,
                field=d.field,
                match_text=d.match_text,
                start_offset=d.start_offset,
                end_offset=d.end_offset,
                extractor=d.extractor,
                extractor_version=d.extractor_version,
                confidence=d.confidence,
                attributes=d.attributes,
            )
            .on_conflict_do_nothing(
                index_elements=[
                    Mention.article_id,
                    Mention.candidate_id,
                    Mention.field,
                    Mention.start_offset,
                    Mention.extractor,
                    Mention.extractor_version,
                ]
            )
            .returning(Mention.id)
        )
        if session.execute(stmt).scalar_one_or_none() is not None:
            n += 1
    return n


def run_extract(*, reprocess_all: bool, batch_size: int = 500) -> int:
    sm = get_sync_sessionmaker()
    started = datetime.now().astimezone()

    with sm() as session:
        aliases = _load_aliases(session)
        if not aliases:
            print("No active aliases found — run `make seed` first.", file=sys.stderr)
            return 0
        extractor = KeywordExtractor(aliases)

        # Select articles to process. When not reprocessing, skip those that
        # already have any mention from the current (extractor, extractor_version)
        # via a correlated NOT EXISTS.
        stmt = select(Article).order_by(Article.id)
        if not reprocess_all:
            stmt = stmt.where(
                not_(
                    exists().where(
                        Mention.article_id == Article.id,
                        Mention.extractor == extractor.extractor_id,
                        Mention.extractor_version == extractor.version,
                    )
                )
            )

        articles = session.execute(stmt).scalars().all()

    print(
        f"Loaded {len(aliases)} active aliases. "
        f"Processing {len(articles)} article(s) "
        f"({'reprocess all' if reprocess_all else 'pending only'})...",
        file=sys.stderr,
    )

    total_inserted = 0
    processed = 0
    batch: list[MentionDraft] = []

    with sm() as session:
        for article in articles:
            drafts = extractor.extract(
                article_id=article.id,
                title=article.title,
                summary=article.summary,
            )
            batch.extend(drafts)
            processed += 1
            if len(batch) >= batch_size:
                with session.begin():
                    total_inserted += _insert_drafts(session, batch)
                batch = []
        if batch:
            with session.begin():
                total_inserted += _insert_drafts(session, batch)

    elapsed = (datetime.now().astimezone() - started).total_seconds()
    print(
        f"Done in {elapsed:.1f}s. "
        f"Inserted {total_inserted} new mentions across {processed} articles "
        f"(extractor={extractor.extractor_id} {extractor.version}).",
        file=sys.stderr,
    )
    return total_inserted


def main() -> None:
    p = argparse.ArgumentParser(prog="extract", description="Run the keyword mention extractor")
    p.add_argument(
        "--all",
        dest="reprocess_all",
        action="store_true",
        help="Reprocess every article (useful after editing aliases)",
    )
    args = p.parse_args()
    run_extract(reprocess_all=args.reprocess_all)


if __name__ == "__main__":
    main()
