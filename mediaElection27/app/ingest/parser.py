"""Parse raw RSS/Atom bytes into normalised entries with UTC timestamps."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from time import struct_time
from typing import Any
from zoneinfo import ZoneInfo

import feedparser
from dateutil import parser as dateparser

PARIS_TZ = ZoneInfo("Europe/Paris")
_MIN_DATE = datetime(1990, 1, 1, tzinfo=UTC)


@dataclass
class ParsedEntry:
    guid: str
    url: str
    title: str
    summary: str | None
    published_at: datetime
    raw: dict[str, Any] = field(default_factory=dict)


def normalise_pubdate(
    raw_value: str | struct_time | None,
    *,
    now: datetime | None = None,
) -> datetime | None:
    """Normalise a feed pubdate to a UTC-aware datetime, or None if unparseable/sane-rejected.

    Naive datetimes are assumed to be Europe/Paris.
    Rejects dates more than 7 days in the future or older than 1990-01-01.
    """
    if raw_value is None:
        return None

    dt: datetime | None = None
    if isinstance(raw_value, struct_time):
        # feedparser already converted to UTC; struct_time is naive but UTC by convention.
        try:
            dt = datetime(*raw_value[:6], tzinfo=UTC)
        except (ValueError, TypeError):
            return None
    else:
        try:
            dt = dateparser.parse(raw_value)
        except (ValueError, TypeError, OverflowError):
            return None

    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=PARIS_TZ)
    dt = dt.astimezone(UTC)

    now_utc = now or datetime.now(UTC)
    if dt < _MIN_DATE or dt > now_utc + timedelta(days=7):
        return None

    return dt


def parse_feed(body: bytes, *, source_slug: str | None = None) -> list[ParsedEntry]:
    """Parse RSS/Atom bytes into ParsedEntry list. Skips entries without a usable date or URL.

    Some feeds (e.g. Le Parisien) omit per-item dates. When that happens we fall back
    to the channel-level lastBuildDate / updated so the items still flow into the DB.
    All entries from such a feed share the same `published_at`, which is acceptable —
    they were all surfaced by the outlet at the same time anyway.
    """
    feed = feedparser.parse(body)
    channel = feed.get("feed", {}) or {}
    channel_date = normalise_pubdate(
        channel.get("updated_parsed") or channel.get("published_parsed")
    ) or normalise_pubdate(channel.get("updated") or channel.get("published"))

    out: list[ParsedEntry] = []
    for raw in feed.entries:
        url = (raw.get("link") or "").strip()
        title = (raw.get("title") or "").strip()
        if not url or not title:
            continue

        published = normalise_pubdate(raw.get("published_parsed") or raw.get("updated_parsed"))
        if published is None:
            published = normalise_pubdate(raw.get("published") or raw.get("updated"))
        if published is None:
            published = channel_date
        if published is None:
            continue

        guid = (raw.get("id") or raw.get("guid") or url).strip()
        summary = raw.get("summary") or raw.get("description")
        if summary:
            summary = summary.strip()

        out.append(
            ParsedEntry(
                guid=guid,
                url=url,
                title=title,
                summary=summary,
                published_at=published,
                raw=dict(raw),
            )
        )

    return out
