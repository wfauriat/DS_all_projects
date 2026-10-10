from __future__ import annotations

from datetime import date, datetime
from typing import Any, ClassVar

from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    LargeBinary,
    Numeric,
    SmallInteger,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    type_annotation_map: ClassVar[dict[Any, Any]] = {dict[str, Any]: JSONB}


class Source(Base):
    __tablename__ = "sources"

    id: Mapped[int] = mapped_column(SmallInteger, primary_key=True, autoincrement=False)
    slug: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    outlet: Mapped[str] = mapped_column(Text, nullable=False)
    section: Mapped[str | None] = mapped_column(Text)
    feed_url: Mapped[str] = mapped_column(Text, nullable=False)
    lean: Mapped[str | None] = mapped_column(Text)
    is_active: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=True, server_default="true"
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    articles: Mapped[list[Article]] = relationship(back_populates="source")


class Article(Base):
    __tablename__ = "articles"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    source_id: Mapped[int] = mapped_column(SmallInteger, ForeignKey("sources.id"), nullable=False)
    guid: Mapped[str] = mapped_column(Text, nullable=False)
    url: Mapped[str] = mapped_column(Text, nullable=False)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    summary: Mapped[str | None] = mapped_column(Text)
    published_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    fetched_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    lang: Mapped[str] = mapped_column(Text, nullable=False, default="fr", server_default="fr")
    raw: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    content_hash: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)

    source: Mapped[Source] = relationship(back_populates="articles")
    mentions: Mapped[list[Mention]] = relationship(
        back_populates="article", cascade="all, delete-orphan"
    )

    __table_args__ = (
        UniqueConstraint("source_id", "guid", name="uq_articles_source_guid"),
        Index("ix_articles_published_at", "published_at"),
        Index("ix_articles_source_pub", "source_id", "published_at"),
        Index("ix_articles_content_hash", "content_hash"),
    )


class Candidate(Base):
    __tablename__ = "candidates"

    id: Mapped[int] = mapped_column(SmallInteger, primary_key=True, autoincrement=False)
    slug: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    display_name: Mapped[str] = mapped_column(Text, nullable=False)
    party: Mapped[str | None] = mapped_column(Text)
    lean: Mapped[str | None] = mapped_column(Text)
    declared_at: Mapped[date | None] = mapped_column()
    eligible: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=True, server_default="true"
    )
    notes: Mapped[str | None] = mapped_column(Text)

    aliases: Mapped[list[CandidateAlias]] = relationship(
        back_populates="candidate", cascade="all, delete-orphan"
    )
    mentions: Mapped[list[Mention]] = relationship(back_populates="candidate")


class CandidateAlias(Base):
    __tablename__ = "candidate_aliases"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    candidate_id: Mapped[int] = mapped_column(
        SmallInteger, ForeignKey("candidates.id", ondelete="CASCADE"), nullable=False
    )
    alias: Mapped[str] = mapped_column(Text, nullable=False)
    match_kind: Mapped[str] = mapped_column(
        Text, nullable=False, default="wholeword", server_default="wholeword"
    )  # exact | wholeword | regex
    requires_context: Mapped[str | None] = mapped_column(Text)
    is_active: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=True, server_default="true"
    )

    candidate: Mapped[Candidate] = relationship(back_populates="aliases")

    __table_args__ = (UniqueConstraint("candidate_id", "alias", name="uq_aliases_candidate_alias"),)


class Mention(Base):
    __tablename__ = "mentions"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    article_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("articles.id", ondelete="CASCADE"), nullable=False
    )
    candidate_id: Mapped[int] = mapped_column(
        SmallInteger, ForeignKey("candidates.id"), nullable=False
    )
    field: Mapped[str] = mapped_column(Text, nullable=False)  # title | summary
    match_text: Mapped[str] = mapped_column(Text, nullable=False)
    start_offset: Mapped[int] = mapped_column(Integer, nullable=False)
    end_offset: Mapped[int] = mapped_column(Integer, nullable=False)
    extractor: Mapped[str] = mapped_column(Text, nullable=False)
    extractor_version: Mapped[str] = mapped_column(Text, nullable=False)
    confidence: Mapped[float | None] = mapped_column(Numeric(asdecimal=False))
    attributes: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, default=dict, server_default="{}"
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    article: Mapped[Article] = relationship(back_populates="mentions")
    candidate: Mapped[Candidate] = relationship(back_populates="mentions")

    __table_args__ = (
        UniqueConstraint(
            "article_id",
            "candidate_id",
            "field",
            "start_offset",
            "extractor",
            "extractor_version",
            name="uq_mentions_position_extractor",
        ),
        Index("ix_mentions_candidate", "candidate_id"),
        Index("ix_mentions_article", "article_id"),
        Index(
            "ix_mentions_attributes_gin",
            "attributes",
            postgresql_using="gin",
        ),
    )


class IngestRun(Base):
    __tablename__ = "ingest_runs"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    source_id: Mapped[int | None] = mapped_column(SmallInteger, ForeignKey("sources.id"))
    status: Mapped[str] = mapped_column(Text, nullable=False)  # running | ok | partial | failed
    feed_http_status: Mapped[int | None] = mapped_column(SmallInteger)
    n_items_seen: Mapped[int | None] = mapped_column(Integer)
    n_articles_inserted: Mapped[int | None] = mapped_column(Integer)
    n_articles_skipped_dup: Mapped[int | None] = mapped_column(Integer)
    n_mentions_inserted: Mapped[int | None] = mapped_column(Integer)
    error: Mapped[str | None] = mapped_column(Text)
    meta: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, default=dict, server_default="{}"
    )

    __table_args__ = (Index("ix_ingest_runs_started", "started_at"),)
