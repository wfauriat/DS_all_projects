"""initial schema

Revision ID: d66ca171650a
Revises:
Create Date: 2026-05-15 14:30:22.587457

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "d66ca171650a"
down_revision: str | Sequence[str] | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "sources",
        sa.Column("id", sa.SmallInteger(), autoincrement=False, nullable=False),
        sa.Column("slug", sa.Text(), nullable=False),
        sa.Column("outlet", sa.Text(), nullable=False),
        sa.Column("section", sa.Text(), nullable=True),
        sa.Column("feed_url", sa.Text(), nullable=False),
        sa.Column("lean", sa.Text(), nullable=True),
        sa.Column("is_active", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("slug"),
    )

    op.create_table(
        "candidates",
        sa.Column("id", sa.SmallInteger(), autoincrement=False, nullable=False),
        sa.Column("slug", sa.Text(), nullable=False),
        sa.Column("display_name", sa.Text(), nullable=False),
        sa.Column("party", sa.Text(), nullable=True),
        sa.Column("lean", sa.Text(), nullable=True),
        sa.Column("declared_at", sa.Date(), nullable=True),
        sa.Column("eligible", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("slug"),
    )

    op.create_table(
        "candidate_aliases",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("candidate_id", sa.SmallInteger(), nullable=False),
        sa.Column("alias", sa.Text(), nullable=False),
        sa.Column(
            "match_kind",
            sa.Text(),
            server_default=sa.text("'wholeword'"),
            nullable=False,
        ),
        sa.Column("requires_context", sa.Text(), nullable=True),
        sa.Column("is_active", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.ForeignKeyConstraint(["candidate_id"], ["candidates.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("candidate_id", "alias", name="uq_aliases_candidate_alias"),
    )

    op.create_table(
        "articles",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("source_id", sa.SmallInteger(), nullable=False),
        sa.Column("guid", sa.Text(), nullable=False),
        sa.Column("url", sa.Text(), nullable=False),
        sa.Column("title", sa.Text(), nullable=False),
        sa.Column("summary", sa.Text(), nullable=True),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "fetched_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("lang", sa.Text(), server_default=sa.text("'fr'"), nullable=False),
        sa.Column("raw", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("content_hash", sa.LargeBinary(), nullable=False),
        sa.ForeignKeyConstraint(["source_id"], ["sources.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("source_id", "guid", name="uq_articles_source_guid"),
        sa.UniqueConstraint("content_hash", name="uq_articles_content_hash"),
    )
    op.create_index("ix_articles_published_at", "articles", ["published_at"])
    op.create_index("ix_articles_source_pub", "articles", ["source_id", "published_at"])

    op.create_table(
        "mentions",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("article_id", sa.BigInteger(), nullable=False),
        sa.Column("candidate_id", sa.SmallInteger(), nullable=False),
        sa.Column("field", sa.Text(), nullable=False),
        sa.Column("match_text", sa.Text(), nullable=False),
        sa.Column("start_offset", sa.Integer(), nullable=False),
        sa.Column("end_offset", sa.Integer(), nullable=False),
        sa.Column("extractor", sa.Text(), nullable=False),
        sa.Column("extractor_version", sa.Text(), nullable=False),
        sa.Column("confidence", sa.Numeric(asdecimal=False), nullable=True),
        sa.Column(
            "attributes",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["article_id"], ["articles.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["candidate_id"], ["candidates.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "article_id",
            "candidate_id",
            "field",
            "start_offset",
            "extractor",
            "extractor_version",
            name="uq_mentions_position_extractor",
        ),
    )
    op.create_index("ix_mentions_candidate", "mentions", ["candidate_id"])
    op.create_index("ix_mentions_article", "mentions", ["article_id"])
    op.create_index(
        "ix_mentions_attributes_gin",
        "mentions",
        ["attributes"],
        postgresql_using="gin",
    )

    op.create_table(
        "ingest_runs",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column(
            "started_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("source_id", sa.SmallInteger(), nullable=True),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("feed_http_status", sa.SmallInteger(), nullable=True),
        sa.Column("n_items_seen", sa.Integer(), nullable=True),
        sa.Column("n_articles_inserted", sa.Integer(), nullable=True),
        sa.Column("n_articles_skipped_dup", sa.Integer(), nullable=True),
        sa.Column("n_mentions_inserted", sa.Integer(), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column(
            "meta",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["source_id"], ["sources.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_ingest_runs_started", "ingest_runs", ["started_at"])


def downgrade() -> None:
    op.drop_index("ix_ingest_runs_started", table_name="ingest_runs")
    op.drop_table("ingest_runs")

    op.drop_index("ix_mentions_attributes_gin", table_name="mentions")
    op.drop_index("ix_mentions_article", table_name="mentions")
    op.drop_index("ix_mentions_candidate", table_name="mentions")
    op.drop_table("mentions")

    op.drop_index("ix_articles_source_pub", table_name="articles")
    op.drop_index("ix_articles_published_at", table_name="articles")
    op.drop_table("articles")

    op.drop_table("candidate_aliases")
    op.drop_table("candidates")
    op.drop_table("sources")
