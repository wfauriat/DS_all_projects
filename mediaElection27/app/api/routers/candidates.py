"""Candidate registry endpoint."""

from __future__ import annotations

from fastapi import APIRouter
from sqlalchemy import func, select

from app.api.deps import SessionDep
from app.db.models import Candidate, CandidateAlias
from app.models.candidate import CandidateOut

router = APIRouter(prefix="/candidates", tags=["candidates"])


@router.get("", response_model=list[CandidateOut])
async def list_candidates(session: SessionDep) -> list[CandidateOut]:
    """All candidates, with alias counts. Sorted by display_name."""
    stmt = (
        select(Candidate, func.count(CandidateAlias.id).label("n_aliases"))
        .outerjoin(
            CandidateAlias,
            (CandidateAlias.candidate_id == Candidate.id)
            & (CandidateAlias.is_active.is_(True)),
        )
        .group_by(Candidate.id)
        .order_by(Candidate.display_name)
    )
    rows = (await session.execute(stmt)).all()
    return [
        CandidateOut.model_validate(c).model_copy(update={"n_aliases": int(n)})
        for c, n in rows
    ]
