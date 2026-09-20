"""Paginated article browser with candidate / source / date filters."""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Annotated

from fastapi import APIRouter, Query
from sqlalchemy import func, select

from app.api.deps import SessionDep
from app.db.models import Article, Mention, Source
from app.models.article import ArticleListOut, ArticleOut

router = APIRouter(prefix="/articles", tags=["articles"])

DEFAULT_LIMIT = 20
MAX_LIMIT = 100


@router.get("", response_model=ArticleListOut)
async def list_articles(
    session: SessionDep,
    candidate_id: Annotated[
        list[int] | None,
        Query(description="Only articles mentioning at least one of these candidates"),
    ] = None,
    has_mention: Annotated[
        bool | None,
        Query(
            description=(
                "true → articles with ≥1 mention (any candidate); "
                "false → articles with zero mentions. Ignored if candidate_id is set."
            ),
        ),
    ] = None,
    source_id: Annotated[list[int] | None, Query()] = None,
    from_: Annotated[date | None, Query(alias="from")] = None,
    to: Annotated[date | None, Query()] = None,
    limit: Annotated[int, Query(ge=1, le=MAX_LIMIT)] = DEFAULT_LIMIT,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> ArticleListOut:
    base = select(Article, Source.outlet).join(Source, Source.id == Article.source_id)

    if candidate_id:
        # Only articles that have at least one mention of one of the requested candidates.
        sub = select(Mention.article_id).where(Mention.candidate_id.in_(candidate_id))
        base = base.where(Article.id.in_(sub))
    elif has_mention is not None:
        any_mention = select(Mention.article_id)
        base = base.where(
            Article.id.in_(any_mention) if has_mention else Article.id.not_in(any_mention)
        )
    if source_id:
        base = base.where(Article.source_id.in_(source_id))
    if from_:
        base = base.where(
            Article.published_at >= datetime.combine(from_, datetime.min.time(), tzinfo=UTC)
        )
    if to:
        base = base.where(
            Article.published_at < datetime.combine(to, datetime.min.time(), tzinfo=UTC)
        )

    total_stmt = select(func.count()).select_from(base.subquery())
    total = (await session.execute(total_stmt)).scalar_one()

    rows = (
        await session.execute(
            base.order_by(Article.published_at.desc(), Article.id.desc())
            .limit(limit)
            .offset(offset)
        )
    ).all()

    if not rows:
        return ArticleListOut(items=[], total=int(total), limit=limit, offset=offset)

    article_ids = [a.id for a, _ in rows]
    mentions = (
        await session.execute(
            select(Mention.article_id, Mention.candidate_id)
            .where(Mention.article_id.in_(article_ids))
            .distinct()
            .order_by(Mention.article_id, Mention.candidate_id)
        )
    ).all()
    by_article: dict[int, list[int]] = {}
    for art_id, cand_id in mentions:
        by_article.setdefault(art_id, []).append(int(cand_id))

    items = []
    for article, outlet in rows:
        out = ArticleOut.model_validate(article).model_copy(
            update={
                "outlet": outlet,
                "candidate_ids": by_article.get(article.id, []),
            }
        )
        items.append(out)

    return ArticleListOut(items=items, total=int(total), limit=limit, offset=offset)
