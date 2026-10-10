"""drop articles content_hash unique constraint

Revision ID: 98d3055b30f7
Revises: d66ca171650a
Create Date: 2026-05-15 17:53:48.267966

The initial schema enforced UNIQUE (content_hash) across all articles, which
collapsed cross-source/cross-feed republications (e.g. France Info politique
and France Info élections carrying the same article, or AFP wires running in
multiple outlets) into a single row. The analysis treats each feed's pick-up
as a distinct data point, so this drops the unique constraint and replaces it
with a plain index for fast cross-outlet wire lookups.
"""

from collections.abc import Sequence

from alembic import op

revision: str = "98d3055b30f7"
down_revision: str | Sequence[str] | None = "d66ca171650a"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.drop_constraint("uq_articles_content_hash", "articles", type_="unique")
    op.create_index("ix_articles_content_hash", "articles", ["content_hash"])


def downgrade() -> None:
    op.drop_index("ix_articles_content_hash", table_name="articles")
    op.create_unique_constraint(
        "uq_articles_content_hash", "articles", ["content_hash"]
    )
