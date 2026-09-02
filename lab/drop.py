"""The file drop channel.

A directory that gets written to on simulated-day boundaries.  At the start of
simulated day D the extract for day D-1 shows up:

    drop/payments/payments_YYYY-MM-DD.csv.gz
    drop/catalog/product_catalog_YYYY-MM-DD.csv

The payments extract is "what settlement told us about on that day", not "what
happened on that day".  There is no manifest, no checksum and no notification.
Files are simply there, or not.
"""

from __future__ import annotations

import asyncio
import csv
import datetime as dt
import gzip
import io
import json
import os
import random
from dataclasses import dataclass
from pathlib import Path

from lab import config as cfgmod

TICK_SECONDS = 2.0
MAX_BOUNDARIES_PER_TICK = 60
CURSOR_NAME = "drop_cursor.json"


@dataclass(frozen=True)
class FeedSpec:
    name: str
    table: str
    dirname: str
    stem: str
    extension: str
    gzip: bool
    columns: tuple[tuple[str, str], ...]   # (header name, source key)
    snapshot: bool = False


FEEDS: dict[str, FeedSpec] = {
    "payments": FeedSpec(
        name="payments", table="payments", dirname="payments", stem="payments",
        extension=".csv.gz", gzip=True,
        columns=(
            ("payment_id", "payment_id"),
            ("order_id", "order_id"),
            ("customer_id", "customer_id"),
            ("paid_ts", "occurred_at"),
            ("amount_cents", "amount_cents"),
            ("status", "status"),
            ("processor", "processor"),
            ("billing_city", "billing_city"),
        ),
    ),
    "catalog": FeedSpec(
        name="catalog", table="products", dirname="catalog", stem="product_catalog",
        extension=".csv", gzip=False,
        columns=(
            ("sku", "sku"),
            ("product_name", "product_name"),
            ("category", "category"),
            ("list_price_cents", "list_price_cents"),
            ("active_from", "active_from"),
        ),
        snapshot=True,
    ),
}

FEED_NAMES = tuple(FEEDS)
DEFAULT_GZIP = {k: v.gzip for k, v in FEEDS.items()}

_TIMESTAMP_KEYS = {"occurred_at", "paid_ts", "active_from"}


# ---------------------------------------------------------------------------
# cursor
# ---------------------------------------------------------------------------


def _cursor_path() -> Path:
    return cfgmod.STATE_DIR / CURSOR_NAME


def _read_cursor(default: dt.date) -> dt.date:
    p = _cursor_path()
    if p.exists():
        try:
            return dt.date.fromisoformat(json.loads(p.read_text())["last_boundary"])
        except Exception:
            pass
    return default


def _write_cursor(day: dt.date) -> None:
    p = _cursor_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(".json.tmp")
    tmp.write_text(json.dumps({"last_boundary": day.isoformat()}) + "\n", encoding="utf-8")
    os.replace(tmp, p)


# ---------------------------------------------------------------------------
# rendering
# ---------------------------------------------------------------------------


def _columns_for(feed: FeedSpec, spec) -> list[tuple[str, str]]:
    cols = list(feed.columns)
    if spec.extra_column is not None:
        name, pos = spec.extra_column
        cols.insert(min(max(pos, 1), len(cols)), (name, name))
    if spec.permute_seed is not None:
        random.Random(spec.permute_seed).shuffle(cols)
    return cols


def _extra_value(name: str, row: dict, day: dt.date) -> str:
    if name == "settlement_batch_id":
        return f"B{day.strftime('%Y%m%d')}-{abs(hash(row.get('processor', ''))) % 7:02d}"
    if name == "catalog_revision":
        return str(day.toordinal() % 997)
    return ""


def _render_rows(state, feed: FeedSpec, spec, as_of: str) -> list[dict]:
    store = state.store
    if feed.snapshot:
        raw = store.snapshot(feed.table, as_of)
    else:
        raw = store.by_knowledge_day(feed.table, spec.content_day, as_of)
    policy = state.effects.policy
    skew = policy.skew(dt.datetime.combine(spec.content_day, dt.time()))
    numeric_ids = (
        policy.numeric_customer_id_from is not None
        and spec.content_day >= policy.numeric_customer_id_from
    )
    out = []
    for row in raw:
        payload = store.serialize(feed.table, row, as_of)
        if skew:
            for k in ("occurred_at", "active_from"):
                v = payload.get(k)
                if isinstance(v, str) and "T" in v:
                    payload[k] = (dt.datetime.fromisoformat(v) + skew).isoformat()
        if numeric_ids and "customer_id" in payload and payload["customer_id"] is not None:
            try:
                payload["customer_id"] = int(payload["customer_id"])
            except (TypeError, ValueError):
                pass
        out.append(payload)
    return out


def render_file(state, spec) -> bytes:
    feed = FEEDS[spec.feed]
    as_of = dt.datetime.combine(
        spec.content_day + dt.timedelta(days=1), dt.time()
    ).isoformat()
    rows = _render_rows(state, feed, spec, as_of)
    cols = _columns_for(feed, spec)

    buf = io.StringIO(newline="")
    w = csv.writer(buf, lineterminator="\n")
    w.writerow([h for h, _ in cols])
    for row in rows:
        line = []
        for header, key in cols:
            if key in row:
                v = row[key]
            else:
                v = _extra_value(header, row, spec.content_day)
            line.append("" if v is None else v)
        w.writerow(line)
    text = buf.getvalue()

    if spec.truncate_fraction is not None:
        text = text[: max(1, int(len(text) * spec.truncate_fraction))]
    data = text.encode(spec.encoding, errors="replace")
    if spec.gzip:
        data = gzip.compress(data)
    return data


def filename(spec) -> str:
    feed = FEEDS[spec.feed]
    return f"{feed.stem}_{spec.name_day.isoformat()}{spec.suffix}{feed.extension}"


def write_file(state, spec) -> Path:
    feed = FEEDS[spec.feed]
    root = Path(state.cfg.drop.path) / feed.dirname
    root.mkdir(parents=True, exist_ok=True)
    dest = root / filename(spec)
    data = render_file(state, spec)
    if spec.atomic:
        tmp = dest.with_name(dest.name + ".part")
        tmp.write_bytes(data)
        os.replace(tmp, dest)
    else:
        dest.write_bytes(data)
    return dest


# ---------------------------------------------------------------------------
# the loop
# ---------------------------------------------------------------------------


def due_boundaries(state) -> list[dt.date]:
    today = state.clock.now().date()
    last = _read_cursor(state.clock.sim_start.date())
    out = []
    day = last + dt.timedelta(days=1)
    while day <= today and len(out) < MAX_BOUNDARIES_PER_TICK:
        out.append(day)
        day += dt.timedelta(days=1)
    return out


def run_once(state) -> int:
    written = 0
    for boundary in due_boundaries(state):
        for spec in state.effects.deliveries_for(boundary, FEED_NAMES, DEFAULT_GZIP):
            write_file(state, spec)
            written += 1
        _write_cursor(boundary)
    return written


async def _loop(state) -> None:
    while True:
        try:
            await asyncio.to_thread(run_once, state)
        except asyncio.CancelledError:
            raise
        except Exception:
            pass
        await asyncio.sleep(TICK_SECONDS)


async def start(state):
    return asyncio.create_task(_loop(state))


async def stop(task) -> None:
    task.cancel()
    try:
        await task
    except (asyncio.CancelledError, Exception):
        pass
