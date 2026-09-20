"""Unit tests for KeywordExtractor — pure-Python, no DB."""

from __future__ import annotations

import pytest

from app.extract.base import AliasSpec
from app.extract.keyword import KeywordExtractor

# Candidate IDs used across tests (mirror seeds/candidates.yaml conventions
# but tests don't actually need the real registry).
MELENCHON = 1
BARDELLA = 2
LE_PEN = 3
MACRON = 99  # not in seeds — used purely to test multi-candidate behaviour


def _extract(aliases: list[AliasSpec], title: str, summary: str | None = None):
    return KeywordExtractor(aliases).extract(article_id=1, title=title, summary=summary)


def test_wholeword_match_basic():
    drafts = _extract(
        [AliasSpec(MELENCHON, "Mélenchon", "wholeword")],
        title="Mélenchon prend la parole",
    )
    assert len(drafts) == 1
    assert drafts[0].candidate_id == MELENCHON
    assert drafts[0].match_text == "Mélenchon"
    assert drafts[0].start_offset == 0
    assert drafts[0].end_offset == 9
    assert drafts[0].field == "title"
    assert drafts[0].attributes == {"alias": "Mélenchon", "match_kind": "wholeword"}


def test_wholeword_does_not_match_inside_larger_word():
    drafts = _extract(
        [AliasSpec(MELENCHON, "Mélenchon", "wholeword")],
        title="Le mélenchonisme à l'épreuve",
    )
    assert drafts == []


def test_exact_match_finds_substrings():
    # `exact` is a literal substring search — boundaries don't matter.
    drafts = _extract(
        [AliasSpec(MELENCHON, "lench", "exact")],
        title="Le mélenchonisme à l'épreuve",
    )
    assert len(drafts) == 1
    assert drafts[0].match_text.lower() == "lench"


def test_case_insensitive():
    drafts = _extract(
        [AliasSpec(MELENCHON, "Mélenchon", "wholeword")],
        title="MÉLENCHON sort du silence",
    )
    assert len(drafts) == 1
    assert drafts[0].match_text == "MÉLENCHON"


def test_accent_stripped_fallback_works_as_separate_alias():
    # The matcher itself doesn't do Unicode normalisation; the data does
    # via a second alias entry. This documents and locks that contract.
    drafts = _extract(
        [
            AliasSpec(MELENCHON, "Mélenchon", "wholeword"),
            AliasSpec(MELENCHON, "Melenchon", "wholeword"),
        ],
        title="Melenchon poursuit sa tournee",
    )
    assert len(drafts) == 1
    assert drafts[0].match_text == "Melenchon"


def test_requires_context_positive():
    drafts = _extract(
        [AliasSpec(LE_PEN, "Le Pen", "wholeword", requires_context="Marine")],
        title="Marine Le Pen attaque la décision",
    )
    assert len(drafts) == 1
    assert drafts[0].candidate_id == LE_PEN
    assert drafts[0].match_text == "Le Pen"


def test_requires_context_negative_when_context_absent():
    # Without "Marine" in the title, the Le Pen alias should not fire — this
    # is what protects Marine Le Pen's count from Marion-Maréchal references.
    drafts = _extract(
        [AliasSpec(LE_PEN, "Le Pen", "wholeword", requires_context="Marine")],
        title="Le Pen jugée inéligible",
    )
    assert drafts == []


def test_requires_context_marion_marechal_does_not_match_marine():
    # The classic disambiguation case from the plan.
    drafts = _extract(
        [AliasSpec(LE_PEN, "Le Pen", "wholeword", requires_context="Marine")],
        title="Marion Maréchal-Le Pen prend ses distances",
    )
    assert drafts == []


def test_multiple_matches_in_one_text():
    drafts = _extract(
        [AliasSpec(MELENCHON, "Mélenchon", "wholeword")],
        title="Mélenchon répond à Mélenchon",
    )
    assert len(drafts) == 2
    assert {d.start_offset for d in drafts} == {0, 19}


def test_span_subsumption_keeps_longest_alias_per_candidate():
    # Two aliases for the same candidate cover overlapping spans; only the
    # widest should be kept.
    drafts = _extract(
        [
            AliasSpec(MELENCHON, "Jean-Luc Mélenchon", "exact"),
            AliasSpec(MELENCHON, "Mélenchon", "wholeword"),
        ],
        title="Jean-Luc Mélenchon à l'Élysée",
    )
    assert len(drafts) == 1
    assert drafts[0].match_text == "Jean-Luc Mélenchon"
    assert drafts[0].attributes["alias"] == "Jean-Luc Mélenchon"


def test_different_candidates_in_same_text():
    drafts = _extract(
        [
            AliasSpec(MELENCHON, "Mélenchon", "wholeword"),
            AliasSpec(BARDELLA, "Bardella", "wholeword"),
        ],
        title="Mélenchon rencontre Bardella",
    )
    assert len(drafts) == 2
    assert {d.candidate_id for d in drafts} == {MELENCHON, BARDELLA}


def test_title_and_summary_both_scanned():
    drafts = _extract(
        [AliasSpec(MACRON, "Macron", "wholeword")],
        title="Macron en visite",
        summary="Le président Macron a rencontré...",
    )
    assert len(drafts) == 2
    assert {d.field for d in drafts} == {"title", "summary"}


def test_empty_title_no_summary_yields_nothing():
    drafts = _extract(
        [AliasSpec(MACRON, "Macron", "wholeword")],
        title="",
        summary=None,
    )
    assert drafts == []


def test_unknown_match_kind_raises_at_construction():
    with pytest.raises(ValueError, match="unknown match_kind"):
        KeywordExtractor([AliasSpec(MACRON, "Macron", "nonsense")])


def test_regex_match_kind():
    # Regex alias covers multiple accent variants (è, é, ê, e) for the same name.
    drafts = _extract(
        [AliasSpec(MELENCHON, r"M[éeèê]lenchon", "regex")],
        title="Mèlenchon orthographié exotiquement",
    )
    assert len(drafts) == 1
    assert drafts[0].match_text == "Mèlenchon"
    assert drafts[0].attributes["match_kind"] == "regex"
