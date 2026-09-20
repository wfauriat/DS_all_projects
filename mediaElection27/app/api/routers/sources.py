"""Source registry endpoint."""

from __future__ import annotations

from fastapi import APIRouter, Query
from sqlalchemy import select

from app.api.deps import SessionDep
from app.db.models import Source
from app.models.source import SourceOut

router = APIRouter(prefix="/sources", tags=["sources"])


@router.get("", response_model=list[SourceOut])
async def list_sources(
    session: SessionDep,
    include_inactive: bool = Query(default=False, description="Include disabled sources"),
) -> list[Source]:
    stmt = select(Source).order_by(Source.outlet, Source.section)
    if not include_inactive:
        stmt = stmt.where(Source.is_active.is_(True))
    return list((await session.execute(stmt)).scalars().all())
