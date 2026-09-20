from __future__ import annotations

from datetime import date

from pydantic import BaseModel, ConfigDict, Field


class CandidateOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    slug: str
    display_name: str
    party: str | None = None
    lean: str | None = None
    declared_at: date | None = None
    eligible: bool
    notes: str | None = None
    n_aliases: int | None = Field(
        default=None,
        description="Set when listing endpoints want to show alias counts inline.",
    )
