from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class SourceOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    slug: str
    outlet: str
    section: str | None = None
    feed_url: str
    lean: str | None = None
    is_active: bool
