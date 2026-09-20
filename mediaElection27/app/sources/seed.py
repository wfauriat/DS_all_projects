"""Idempotent seeder for sources, candidates, and aliases.

Reads `seeds/sources.yaml` and `seeds/candidates.yaml` and UPSERTs into Postgres.
Aliases are reset per candidate (delete-all + reinsert from YAML) so the file
stays the source of truth — removed aliases disappear from the DB.

Usage:
    python -m app.sources.seed
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import yaml
from sqlalchemy import delete
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from app.config import settings
from app.db.engine import get_sync_sessionmaker
from app.db.models import Candidate, CandidateAlias, Source

SOURCE_FIELDS = ("id", "slug", "outlet", "section", "feed_url", "lean", "is_active")
CANDIDATE_FIELDS = (
    "id",
    "slug",
    "display_name",
    "party",
    "lean",
    "declared_at",
    "eligible",
    "notes",
)


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Seed file not found: {path}")
    data: dict[str, Any] = yaml.safe_load(path.read_text(encoding="utf-8"))
    return data


def upsert_sources(session: Session, rows: list[dict[str, Any]]) -> int:
    n = 0
    for row in rows:
        values = {k: row.get(k) for k in SOURCE_FIELDS if k in row}
        values.setdefault("is_active", True)
        stmt = pg_insert(Source).values(**values)
        update_cols = {k: stmt.excluded[k] for k in values if k != "id"}
        stmt = stmt.on_conflict_do_update(index_elements=[Source.id], set_=update_cols)
        session.execute(stmt)
        n += 1
    return n


def upsert_candidates(session: Session, rows: list[dict[str, Any]]) -> tuple[int, int]:
    n_candidates = 0
    n_aliases = 0
    for row in rows:
        values = {k: row.get(k) for k in CANDIDATE_FIELDS if k in row}
        values.setdefault("eligible", True)
        stmt = pg_insert(Candidate).values(**values)
        update_cols = {k: stmt.excluded[k] for k in values if k != "id"}
        stmt = stmt.on_conflict_do_update(index_elements=[Candidate.id], set_=update_cols)
        session.execute(stmt)
        n_candidates += 1

        cand_id = values["id"]
        session.execute(delete(CandidateAlias).where(CandidateAlias.candidate_id == cand_id))
        for a in row.get("aliases", []):
            session.execute(
                pg_insert(CandidateAlias).values(
                    candidate_id=cand_id,
                    alias=a["alias"],
                    match_kind=a.get("match_kind", "wholeword"),
                    requires_context=a.get("requires_context"),
                    is_active=a.get("is_active", True),
                )
            )
            n_aliases += 1
    return n_candidates, n_aliases


def main(seeds_dir: Path | None = None) -> None:
    sd = seeds_dir or settings.seeds_dir
    sources_doc = _load_yaml(sd / "sources.yaml")
    candidates_doc = _load_yaml(sd / "candidates.yaml")

    sm = get_sync_sessionmaker()
    with sm() as session, session.begin():
        n_src = upsert_sources(session, sources_doc["sources"])
        n_cand, n_alias = upsert_candidates(session, candidates_doc["candidates"])

    print(
        f"Seeded {n_src} sources, {n_cand} candidates, {n_alias} aliases from {sd.resolve()}",
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()
