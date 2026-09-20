from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict


class ArticleOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    source_id: int
    outlet: str | None = None  # filled by joined query
    title: str
    summary: str | None = None
    url: str
    published_at: datetime
    fetched_at: datetime
    candidate_ids: list[int] = []  # filled by joined query when present


class ArticleListOut(BaseModel):
    items: list[ArticleOut]
    total: int
    limit: int
    offset: int
