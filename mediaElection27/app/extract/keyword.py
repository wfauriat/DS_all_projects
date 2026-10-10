"""Keyword-based mention extractor (v1).

Walks the candidate_aliases table over the title + summary of each article and
emits one MentionDraft per match. Supports three match kinds and per-alias
context disambiguation (e.g. "Le Pen" requires "Marine" co-occurring in the
same field, otherwise it would match Marion-Maréchal references too).

Design choices worth keeping in mind:
- Case-insensitive everywhere. French outlets vary on capitalisation in
  headlines ("Mélenchon" vs "MELENCHON" vs "mélenchon").
- Accent-stripped fallbacks (e.g. "Melenchon") are encoded as separate aliases
  in candidates.yaml rather than via Unicode normalisation here. Keeps the
  matcher simple and the data declarative.
- requires_context scope is "the same field" — if the alias matches in `title`,
  the context token must appear in `title` too (likewise for summary). This
  is conservative; the alternative (look in both fields) would over-match.
- Within a single (candidate, field), overlapping matches are collapsed by
  keeping the longest span. Without this, "Jean-Luc Mélenchon" would emit two
  mentions for the same candidate at overlapping positions (one for the full
  name, one for the surname).
"""

from __future__ import annotations

import re
from collections import defaultdict
from collections.abc import Iterable

from app.extract.base import AliasSpec, MentionDraft


class KeywordExtractor:
    extractor_id = "keyword"
    version = "v1"

    def __init__(self, aliases: Iterable[AliasSpec]):
        self.aliases: list[AliasSpec] = list(aliases)
        # Pre-compile regexes once at construction.
        self._compiled: dict[int, re.Pattern[str]] = {}
        for i, a in enumerate(self.aliases):
            self._compiled[i] = _compile(a)

    def extract(
        self, *, article_id: int, title: str, summary: str | None
    ) -> list[MentionDraft]:
        out: list[MentionDraft] = []
        for field_name, text in (("title", title), ("summary", summary)):
            if not text:
                continue
            out.extend(self._extract_field(article_id, field_name, text))
        return out

    def _extract_field(
        self, article_id: int, field_name: str, text: str
    ) -> list[MentionDraft]:
        # Group aliases by candidate so we can collapse overlapping matches per candidate.
        by_candidate: dict[int, list[tuple[int, AliasSpec]]] = defaultdict(list)
        for i, a in enumerate(self.aliases):
            by_candidate[a.candidate_id].append((i, a))

        results: list[MentionDraft] = []
        for cand_id, indexed_aliases in by_candidate.items():
            # Longer aliases first → they get to claim the span before shorter ones do.
            indexed_aliases.sort(key=lambda ia: -len(ia[1].alias))
            spans_taken: list[tuple[int, int]] = []

            for idx, alias in indexed_aliases:
                pattern = self._compiled[idx]
                for m in pattern.finditer(text):
                    s, e = m.start(), m.end()
                    # Skip if entirely subsumed by an earlier (longer) match for this candidate.
                    if any(es <= s and e <= ee for es, ee in spans_taken):
                        continue
                    # Context disambiguation: required token must appear (anywhere) in same field.
                    if alias.requires_context and not _has_context(
                        alias.requires_context, text
                    ):
                        continue
                    results.append(
                        MentionDraft(
                            article_id=article_id,
                            candidate_id=cand_id,
                            field=field_name,
                            match_text=text[s:e],
                            start_offset=s,
                            end_offset=e,
                            extractor=self.extractor_id,
                            extractor_version=self.version,
                            confidence=1.0,
                            attributes={
                                "alias": alias.alias,
                                "match_kind": alias.match_kind,
                            },
                        )
                    )
                    spans_taken.append((s, e))
        return results


def _compile(alias: AliasSpec) -> re.Pattern[str]:
    flags = re.IGNORECASE | re.UNICODE
    if alias.match_kind == "exact":
        return re.compile(re.escape(alias.alias), flags)
    if alias.match_kind == "wholeword":
        return re.compile(r"\b" + re.escape(alias.alias) + r"\b", flags)
    if alias.match_kind == "regex":
        return re.compile(alias.alias, flags)
    raise ValueError(f"unknown match_kind: {alias.match_kind!r}")


def _has_context(token: str, text: str) -> bool:
    """Whole-word check that `token` appears in `text`."""
    return (
        re.search(r"\b" + re.escape(token) + r"\b", text, re.IGNORECASE | re.UNICODE)
        is not None
    )
