"""Liveness, version, and lightweight aggregate stats."""

from __future__ import annotations

from fastapi import APIRouter
from sqlalchemy import func, select

from app.api.deps import SessionDep
from app.db.models import Article, Candidate, IngestRun, Mention, Source

router = APIRouter(tags=["meta"])

API_VERSION = "0.1.0"


@router.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/version")
async def version() -> dict[str, str]:
    return {"api": API_VERSION}


@router.get("/stats")
async def stats(session: SessionDep) -> dict[str, int]:
    n_articles = (await session.execute(select(func.count()).select_from(Article))).scalar_one()
    n_mentions = (await session.execute(select(func.count()).select_from(Mention))).scalar_one()
    n_sources = (
        await session.execute(
            select(func.count()).select_from(Source).where(Source.is_active.is_(True))
        )
    ).scalar_one()
    n_candidates = (
        await session.execute(select(func.count()).select_from(Candidate))
    ).scalar_one()
    n_ingest_runs = (
        await session.execute(select(func.count()).select_from(IngestRun))
    ).scalar_one()
    return {
        "articles": n_articles,
        "mentions": n_mentions,
        "active_sources": n_sources,
        "candidates": n_candidates,
        "ingest_runs": n_ingest_runs,
    }
