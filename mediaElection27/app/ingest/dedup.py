"""URL canonicalisation and content hashing for article deduplication."""

from __future__ import annotations

import hashlib
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

# Tracking-style query params we strip before hashing/storing the URL.
_STRIP_PARAM_PREFIXES = ("utm_",)
_STRIP_PARAM_EXACT = frozenset(
    {"fbclid", "gclid", "mc_cid", "mc_eid", "ref", "ref_src", "from", "_ga"}
)


def canonical_url(url: str) -> str:
    """Strip tracking params and fragment; keep scheme/host/path/remaining-query."""
    parts = urlparse(url)
    keep = [
        (k, v)
        for k, v in parse_qsl(parts.query, keep_blank_values=True)
        if not k.lower().startswith(_STRIP_PARAM_PREFIXES) and k.lower() not in _STRIP_PARAM_EXACT
    ]
    new_query = urlencode(keep, doseq=True)
    return urlunparse(parts._replace(query=new_query, fragment=""))


def content_hash(title: str, summary: str | None) -> bytes:
    """Stable cross-source hash for dedup. Lowercases and strips both fields."""
    norm_title = (title or "").strip().lower()
    norm_summary = (summary or "").strip().lower()
    payload = f"{norm_title}\n{norm_summary}".encode()
    return hashlib.sha256(payload).digest()
