"""Small shared helpers: time, hashing, logging, tolerant coercion."""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import logging
import re
import sys

from . import config

# Values the vendor might use to mean "nothing".  The first three are ordinary
# null spellings; the rest are the magic-value family and are recorded, not
# silently swallowed, because a sentinel is a data fault and a null is not.
NULLISH = {"", "none", "null", "nan", "n/a", "na", "-", "unknown"}
SENTINEL_NUMBERS = {-999, -9999, -1, 999999999, -999999}
SENTINEL_DATES = {"1900-01-01", "1970-01-01", "9999-12-31", "0001-01-01"}
SENTINEL_STRINGS = {"n/a", "na", "null", "none", "unknown", "-999", "missing", "tbd"}

_TS_RE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2}):(\d{2})")
_OFFSET_RE = re.compile(r"(?:Z|[+-]\d{2}:?\d{2})$")


# ------------------------------------------------------------------ logging
def get_logger(name: str = "pipeline") -> logging.Logger:
    log = logging.getLogger(name)
    if log.handlers:
        return log
    log.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s %(levelname)-7s %(name)s %(message)s")
    fh = logging.FileHandler(config.LOGS / "pipeline.log", encoding="utf-8")
    fh.setFormatter(fmt)
    log.addHandler(fh)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    log.addHandler(sh)
    return log


# --------------------------------------------------------------------- time
def parse_ts(value):
    """Parse a vendor timestamp into a naive datetime, tolerating offsets.

    Returns ``(datetime | None, had_offset: bool)``.  We deliberately do not
    raise: a timestamp we cannot read is a finding, not a crash.
    """
    if value is None:
        return None, False
    s = str(value).strip()
    if s.lower() in NULLISH:
        return None, False
    had_off = bool(_OFFSET_RE.search(s))
    try:
        d = dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
        if d.tzinfo is not None:
            d = d.astimezone(dt.timezone.utc).replace(tzinfo=None)
        return d, had_off
    except ValueError:
        pass
    m = _TS_RE.match(s)
    if m:
        y, mo, da, h, mi, se = (int(x) for x in m.groups())
        try:
            return dt.datetime(y, mo, da, h, mi, se), had_off
        except ValueError:
            return None, had_off
    try:
        return dt.datetime.strptime(s[:10], "%Y-%m-%d"), had_off
    except ValueError:
        return None, had_off


def parse_date(value):
    d, _ = parse_ts(value)
    return d.date() if d else None


def iso(d) -> str:
    if d is None:
        return None
    if isinstance(d, dt.date) and not isinstance(d, dt.datetime):
        return d.isoformat()
    return d.replace(microsecond=0).isoformat()


def day_str(d) -> str:
    if isinstance(d, str):
        return d[:10]
    return d.strftime("%Y-%m-%d")


def to_day(s: str) -> dt.date:
    return dt.date.fromisoformat(s[:10])


def days_between(a, b) -> int:
    """Whole days from a to b, accepting dates, datetimes or ISO strings."""
    if isinstance(a, str):
        a = to_day(a)
    if isinstance(b, str):
        b = to_day(b)
    if isinstance(a, dt.datetime):
        a = a.date()
    if isinstance(b, dt.datetime):
        b = b.date()
    return (b - a).days


# ------------------------------------------------------------------ hashing
def json_dumps(obj) -> str:
    return json.dumps(obj, ensure_ascii=False, default=str)


def json_loads(s, default=None):
    if not s:
        return default
    try:
        return json.loads(s)
    except (json.JSONDecodeError, TypeError):
        return default


def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def row_hash(obj) -> str:
    return hashlib.sha1(
        json.dumps(obj, sort_keys=True, default=str, ensure_ascii=False).encode("utf-8")
    ).hexdigest()


# ------------------------------------------------------------- coercion
def clean_str(v):
    """Normalise a vendor string, returning ``(value, was_nullish)``."""
    if v is None:
        return None, True
    s = str(v).strip()
    if s.lower() in NULLISH:
        return None, True
    return s, False


def to_int(v):
    if v is None:
        return None
    if isinstance(v, bool):
        return int(v)
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        return int(v)
    s = str(v).strip()
    if s.lower() in NULLISH:
        return None
    try:
        return int(float(s))
    except ValueError:
        return None


def pct(a, b) -> float:
    return (a / b) if b else 0.0


def median(xs):
    xs = sorted(x for x in xs if x is not None)
    if not xs:
        return None
    n = len(xs)
    return xs[n // 2] if n % 2 else (xs[n // 2 - 1] + xs[n // 2]) / 2.0


def quantile(xs, q):
    xs = sorted(x for x in xs if x is not None)
    if not xs:
        return None
    i = min(len(xs) - 1, max(0, int(round(q * (len(xs) - 1)))))
    return xs[i]
