from __future__ import annotations

from datetime import date

from pydantic import BaseModel, ConfigDict, Field

from app.models.candidate import CandidateOut
from app.models.source import SourceOut


class TimeseriesPoint(BaseModel):
    """One bucket in the time series — (day, candidate, source) → counts."""

    day: date
    candidate_id: int
    source_id: int
    n_mentions: int
    n_articles: int


class TimeseriesResponse(BaseModel):
    """Chart-ready response with embedded candidate + source registries.

    The frontend reads `points` as the data series and uses `candidates` /
    `sources` to render labels and colors without making extra requests.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    points: list[TimeseriesPoint]
    candidates: list[CandidateOut]
    sources: list[SourceOut]
    from_: date = Field(alias="from")
    to: date
    tz: str
    extractor: str
    extractor_version: str
    n_total_mentions: int
