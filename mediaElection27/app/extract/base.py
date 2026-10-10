"""Extractor protocol + shared dataclasses.

The mention extraction layer produces `MentionDraft` objects that map 1:1 to
rows in the `mentions` table. Multiple extractors (keyword v1 today, NER later)
can write into the same table side-by-side; the `(extractor, extractor_version)`
columns plus the UNIQUE constraint on `mentions` keep them from clobbering each
other.
"""

from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field as dc_field
from typing import Any, Protocol


@dataclass(slots=True)
class AliasSpec:
    """Pure-python view of a candidate_aliases row, decoupled from the ORM
    so the extractor can be unit-tested without a DB."""

    candidate_id: int
    alias: str
    match_kind: str = "wholeword"  # exact | wholeword | regex
    requires_context: str | None = None


@dataclass(slots=True)
class MentionDraft:
    """One pending row for the `mentions` table.

    `attributes` is the open-ended JSONB payload — keyword extractors fill in
    which alias hit, NER extractors will add entity type / model confidence.
    """

    article_id: int
    candidate_id: int
    field: str  # title | summary
    match_text: str
    start_offset: int
    end_offset: int
    extractor: str
    extractor_version: str
    confidence: float = 1.0
    attributes: dict[str, Any] = dc_field(default_factory=dict)


class Extractor(Protocol):
    """Anything that turns (article text) into a list of MentionDraft.

    Both the keyword matcher (v1) and any future NER pipeline implement this.
    """

    extractor_id: str
    version: str

    def extract(
        self, *, article_id: int, title: str, summary: str | None
    ) -> list[MentionDraft]: ...
