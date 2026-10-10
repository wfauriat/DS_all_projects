"""Mention time-series — the chart-feeding endpoint.

Returns one row per (day, candidate, source) bucket within the requested
window, plus the candidate and source registries needed to label the chart.

The day boundary uses `AT TIME ZONE :tz` so a "day" matches the editorial
day in the requested timezone (default Europe/Paris) rather than UTC.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import Annotated

from fastapi import APIRouter, Query
from sqlalchemy import bindparam, select, text
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.types import Integer

from app.api.deps import SessionDep
from app.db.models import Candidate, Source
from app.models.candidate import CandidateOut
from app.models.source import SourceOut
from app.models.timeseries import TimeseriesPoint, TimeseriesResponse

router = APIRouter(tags=["timeseries"])

DEFAULT_EXTRACTOR = "keyword"
DEFAULT_VERSION = "v1"
DEFAULT_TZ = "Europe/Paris"
DEFAULT_WINDOW_DAYS = 30

_TIMESERIES_SQL = text(
    """
    SELECT
        date_trunc('day', a.published_at AT TIME ZONE :tz)::date AS day,
        m.candidate_id,
        a.source_id,
        COUNT(*)                          AS n_mentions,
        COUNT(DISTINCT m.article_id)      AS n_articles
    FROM mentions m
    JOIN articles a ON a.id = m.article_id
    WHERE a.published_at >= :from_dt
      AND a.published_at <  :to_dt
      AND m.extractor          = :extractor
      AND m.extractor_version  = :extractor_version
      AND (CARDINALITY(:candidate_ids) = 0 OR m.candidate_id = ANY(:candidate_ids))
      AND (CARDINALITY(:source_ids)    = 0 OR a.source_id    = ANY(:source_ids))
    GROUP BY day, m.candidate_id, a.source_id
    ORDER BY day, m.candidate_id, a.source_id
    """
).bindparams(
    bindparam("candidate_ids", type_=ARRAY(Integer)),
    bindparam("source_ids", type_=ARRAY(Integer)),
)


@router.get("/timeseries", response_model=TimeseriesResponse, response_model_by_alias=True)
async def get_timeseries(
    session: SessionDep,
    candidate_id: Annotated[
        list[int] | None,
        Query(description="Filter by candidate id(s); omit for all"),
    ] = None,
    source_id: Annotated[
        list[int] | None,
        Query(description="Filter by source id(s); omit for all"),
    ] = None,
    from_: Annotated[date | None, Query(alias="from")] = None,
    to: Annotated[date | None, Query()] = None,
    tz: Annotated[str, Query(description="IANA timezone name for day bucketing")] = DEFAULT_TZ,
    extractor: str = DEFAULT_EXTRACTOR,
    extractor_version: str = DEFAULT_VERSION,
) -> TimeseriesResponse:
    today = date.today()
    if from_ is None:
        from_ = today - timedelta(days=DEFAULT_WINDOW_DAYS)
    if to is None:
        to = today + timedelta(days=1)  # inclusive of today

    from_dt = datetime.combine(from_, datetime.min.time(), tzinfo=UTC)
    to_dt = datetime.combine(to, datetime.min.time(), tzinfo=UTC)

    params = {
        "tz": tz,
        "from_dt": from_dt,
        "to_dt": to_dt,
        "extractor": extractor,
        "extractor_version": extractor_version,
        "candidate_ids": list(candidate_id or []),
        "source_ids": list(source_id or []),
    }
    rows = (await session.execute(_TIMESERIES_SQL, params)).mappings().all()
    points = [
        TimeseriesPoint(
            day=r["day"],
            candidate_id=r["candidate_id"],
            source_id=r["source_id"],
            n_mentions=int(r["n_mentions"]),
            n_articles=int(r["n_articles"]),
        )
        for r in rows
    ]

    # Always return the full candidate + active-source registries so the
    # frontend can render labels/colors as a stable lookup, regardless of
    # whether the requested window happened to be empty or filtered.
    candidates = list(
        (
            await session.execute(select(Candidate).order_by(Candidate.display_name))
        ).scalars().all()
    )
    sources = list(
        (
            await session.execute(
                select(Source)
                .where(Source.is_active.is_(True))
                .order_by(Source.outlet, Source.section)
            )
        ).scalars().all()
    )

    n_total = sum(p.n_mentions for p in points)
    return TimeseriesResponse(
        points=points,
        candidates=[CandidateOut.model_validate(c) for c in candidates],
        sources=[SourceOut.model_validate(s) for s in sources],
        **{"from": from_},
        to=to,
        tz=tz,
        extractor=extractor,
        extractor_version=extractor_version,
        n_total_mentions=n_total,
    )
