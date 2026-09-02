"""Turning whatever the vendor sent into rows we can store.

Three principles:

1. **Never drop a field.**  Anything outside the expected set goes to
   ``extra_json`` and is reported as schema drift.  A field we throw away is a
   fault we cannot see.
2. **Repair, but record.**  If a join key comes back stripped of its leading
   zeros we re-pad it so the warehouse still joins, *and* we count it, because
   the repair is the evidence.
3. **Never raise.**  A row we cannot parse is written with nulls and counted.
"""
from __future__ import annotations

import difflib
import json
import re

from . import config, util

ID_WIDTH = {"customer_id": 7}
_NUM_ID = re.compile(r"^\d+$")


class NormStats:
    """What had to be repaired while normalising one batch."""

    def __init__(self):
        self.unexpected_fields = {}     # field -> count
        self.missing_fields = {}        # field -> count
        self.renamed = {}               # missing -> chosen alias
        self.padded_ids = 0
        self.id_width_violations = {}   # field -> count
        self.tz_offsets = 0
        self.tz_naive = 0
        self.sentinels = {}             # "field=value" -> count
        self.mojibake = {}              # field -> count
        self.unparseable_ts = 0
        self.rows = 0

    def merge(self, other):
        for attr in ("unexpected_fields", "missing_fields", "id_width_violations",
                     "sentinels", "mojibake", "renamed"):
            d, o = getattr(self, attr), getattr(other, attr)
            for k, v in o.items():
                if isinstance(v, int):
                    d[k] = d.get(k, 0) + v
                else:
                    d[k] = v
        self.padded_ids += other.padded_ids
        self.tz_offsets += other.tz_offsets
        self.tz_naive += other.tz_naive
        self.unparseable_ts += other.unparseable_ts
        self.rows += other.rows


# Mojibake signature.  A UTF-8 two-byte sequence decoded as latin-1/cp1252
# always leaves an accented capital followed by another high character ("Gijón" ->
# "GijÃ³n").  Requiring the *pair* matters: a lone high letter can occur in
# genuine European text; the pair cannot.
_MOJI = re.compile("[\u00c3\u00c2\u00c5\u00d0\u00d1][\u0080\u0081\u0082\u0083\u0084\u0085\u0086\u0087\u0088\u0089\u008a\u008b\u008c\u008d\u008e\u008f\u0090\u0091\u0092\u0093\u0094\u0095\u0096\u0097\u0098\u0099\u009a\u009b\u009c\u009d\u009e\u009f\u00a0\u00a1\u00a2\u00a3\u00a4\u00a5\u00a6\u00a7\u00a8\u00a9\u00aa\u00ab\u00ac\u00ad\u00ae\u00af\u00b0\u00b1\u00b2\u00b3\u00b4\u00b5\u00b6\u00b7\u00b8\u00b9\u00ba\u00bb\u00bc\u00bd\u00be\u00bf\u0152\u0153\u0160\u0161\u0178\u017d\u017e\u0192\u02c6\u02dc\u2013\u2014\u2018\u2019\u201a\u201c\u201d\u201e\u2020\u2021\u2022\u2026\u2030\u2039\u203a\u20ac\u2122\ufffd]|\ufffd")


def looks_mojibake(s: str) -> bool:
    return bool(s) and bool(_MOJI.search(s))


# Deliberately excludes the bare spellings of null ("", "none", "null"): those
# are how a source says "nothing", and at higher hostility levels they are also
# how the *transport* says it.  A sentinel is a value pretending to be data.
SENTINEL_TOKENS = {"n/a", "na", "missing", "unknown", "tbd", "undefined", "?"}


def sentinel_of(field, value):
    """Return a printable sentinel token if `value` is a magic null, else None."""
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        if int(value) in util.SENTINEL_NUMBERS and ("cents" in field or "amount" in field):
            return f"{field}={int(value)}"
        return None
    s = str(value).strip()
    low = s.lower()
    if low in SENTINEL_TOKENS:
        return f"{field}={s}"
    if s[:10] in util.SENTINEL_DATES:
        return f"{field}={s[:10]}"
    if _NUM_ID.match(s.lstrip("-")):
        try:
            if int(s) in util.SENTINEL_NUMBERS and ("cents" in field or "amount" in field):
                return f"{field}={s}"
        except ValueError:
            pass
    return None


def resolve_fields(rec: dict, expected: set, stats: NormStats):
    """Map a record's keys onto the expected schema, following renames.

    A field that changed name mid-feed shows up as one expected key missing and
    one unknown key present.  We match them by name similarity so downstream
    models keep working, and record the mapping so the monitor can report it.
    """
    keys = set(rec.keys())
    unknown = keys - expected
    missing = expected - keys
    out = dict(rec)
    if unknown:
        for f in unknown:
            stats.unexpected_fields[f] = stats.unexpected_fields.get(f, 0) + 1
    if missing:
        for f in missing:
            stats.missing_fields[f] = stats.missing_fields.get(f, 0) + 1
    if unknown and missing:
        pool = list(unknown)
        for want in sorted(missing):
            if not pool:
                break
            best, score = None, 0.0
            for cand in pool:
                r = difflib.SequenceMatcher(None, want, cand).ratio()
                # a shared token ("total", "city", "amount") is strong evidence
                wt, ct = set(want.split("_")), set(cand.split("_"))
                if wt & ct:
                    r += 0.35
                if r > score:
                    best, score = cand, r
            if best is not None and score >= 0.55:
                out[want] = rec.get(best)
                pool.remove(best)
                stats.renamed[want] = best
    return out, unknown


def norm_id(value, field, stats: NormStats):
    """Normalise an identifier, re-padding numeric ids that lost their zeros."""
    if value is None:
        return None
    s = str(value).strip()
    if not s or s.lower() in util.NULLISH:
        return None
    width = ID_WIDTH.get(field)
    if width and _NUM_ID.match(s):
        if len(s) != width:
            stats.id_width_violations[field] = stats.id_width_violations.get(field, 0) + 1
            if len(s) < width:
                s = s.zfill(width)
                stats.padded_ids += 1
    return s


def norm_text(value, field, stats: NormStats):
    sent = sentinel_of(field, value)
    if sent:
        stats.sentinels[sent] = stats.sentinels.get(sent, 0) + 1
        return None                      # a sentinel is a null once it is recognised
    s, _isnull = util.clean_str(value)
    if s is None:
        return None
    if looks_mojibake(s):
        stats.mojibake[field] = stats.mojibake.get(field, 0) + 1
    return s


def norm_ts(value, field, stats: NormStats):
    d, had_off = util.parse_ts(value)
    if value is not None and str(value).strip().lower() not in util.NULLISH and d is None:
        stats.unparseable_ts += 1
    if had_off:
        stats.tz_offsets += 1
    elif d is not None:
        stats.tz_naive += 1
    sent = sentinel_of(field, value)
    if sent:
        stats.sentinels[sent] = stats.sentinels.get(sent, 0) + 1
    return util.iso(d)


def norm_num(value, field, stats: NormStats):
    sent = sentinel_of(field, value)
    if sent:
        stats.sentinels[sent] = stats.sentinels.get(sent, 0) + 1
        return None
    return util.to_int(value)


# --------------------------------------------------------------- per entity
def normalize(entity, rec: dict, stats: NormStats, sim_now: str, cycle_id: int):
    exp = config.EXPECTED_FIELDS[entity]
    r, unknown = resolve_fields(rec, exp, stats)
    stats.rows += 1
    extra = {k: rec[k] for k in unknown} or None
    extra_json = json.dumps(extra, ensure_ascii=False, default=str) if extra else None
    ver = util.to_int(r.get("version")) or 1
    kt = norm_ts(r.get("knowledge_time"), "knowledge_time", stats)
    tzoff = 1 if util.parse_ts(r.get("knowledge_time"))[1] else 0

    if entity == "orders":
        items = r.get("items")
        if isinstance(items, str):
            try:
                items = json.loads(items)
            except (json.JSONDecodeError, TypeError):
                items = None
        items = items if isinstance(items, list) else []
        occ = norm_ts(r.get("occurred_at"), "occurred_at", stats)
        if util.parse_ts(r.get("occurred_at"))[1]:
            tzoff = 1
        for it in items:
            if isinstance(it, dict):
                norm_text(it.get("category"), "category", stats)
        return ("orders", (
            norm_id(r.get("order_id"), "order_id", stats), ver, kt, occ,
            norm_id(r.get("customer_id"), "customer_id", stats),
            norm_text(r.get("channel"), "channel", stats),
            norm_text(r.get("payment_method"), "payment_method", stats),
            norm_text(r.get("shipping_speed"), "shipping_speed", stats),
            norm_num(r.get("order_total_cents"), "order_total_cents", stats),
            json.dumps(items, ensure_ascii=False, default=str),
            len(items), sum(util.to_int(i.get("qty")) or 0 for i in items if isinstance(i, dict)),
            extra_json, tzoff, sim_now, cycle_id,
            util.row_hash(rec)))

    if entity == "customers":
        vf = norm_ts(r.get("valid_from"), "valid_from", stats)
        if util.parse_ts(r.get("valid_from"))[1]:
            tzoff = 1
        return ("customers", (
            norm_id(r.get("customer_id"), "customer_id", stats), vf, ver,
            norm_ts(r.get("valid_to"), "valid_to", stats),
            norm_text(r.get("signup_date"), "signup_date", stats),
            norm_text(r.get("country"), "country", stats),
            norm_text(r.get("city"), "city", stats),
            norm_text(r.get("segment"), "segment", stats),
            norm_num(r.get("lifetime_value_cents"), "lifetime_value_cents", stats),
            util.to_int(r.get("is_active")), kt, extra_json, tzoff,
            sim_now, cycle_id, util.row_hash(rec)))

    if entity == "events":
        occ = norm_ts(r.get("occurred_at"), "occurred_at", stats)
        if util.parse_ts(r.get("occurred_at"))[1]:
            tzoff = 1
        return ("events", (
            norm_id(r.get("event_id"), "event_id", stats), ver,
            norm_id(r.get("customer_id"), "customer_id", stats),
            norm_text(r.get("session_id"), "session_id", stats), occ,
            norm_text(r.get("event_type"), "event_type", stats),
            norm_text(r.get("sku"), "sku", stats), kt, extra_json, tzoff,
            sim_now, cycle_id, util.row_hash(rec)))

    if entity == "labels":
        res = norm_ts(r.get("resolved_at"), "resolved_at", stats)
        if util.parse_ts(r.get("resolved_at"))[1]:
            tzoff = 1
        ref = r.get("refunded_within_30d")
        return ("labels", (
            norm_id(r.get("order_id"), "order_id", stats), ver, res,
            norm_ts(r.get("refund_at"), "refund_at", stats),
            util.to_int(ref), kt, extra_json, tzoff,
            sim_now, cycle_id, util.row_hash(rec)))

    raise ValueError(entity)
