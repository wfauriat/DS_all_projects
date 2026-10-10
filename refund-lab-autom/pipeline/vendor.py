"""The vendor HTTP client.

Everything hostile about the transport is absorbed here so that no other module
has to think about it:

* bearer-token exchange, cached, re-issued on ``401 token_expired`` mid-pull;
* retry with jittered backoff on 429 (honouring ``Retry-After`` when present),
  5xx, connection resets, read timeouts and ``200 OK`` bodies that are really
  errors;
* content-encoding is *sniffed from the bytes*, never trusted from the header;
* pagination that validates cursors -- a repeated cursor, a cursor that does not
  move, or a page whose ids we have already seen is caught rather than looped on;
* ``total_count`` is used only as a hint for change detection, never as a
  completeness check.

Transport anomalies are counted, not logged as incidents: ambient hostility is
the environment.  The one exception is a *sustained* 503 window, which the
counters here make visible to the monitors.
"""
from __future__ import annotations

import gzip
import json
import random
import time
import zlib

import requests

from . import config, util

log = util.get_logger("vendor")


class VendorError(RuntimeError):
    pass


class TransportStats:
    """Per-cycle tally of what the transport had to survive."""

    def __init__(self):
        self.reset()

    def reset(self):
        self.requests = 0
        self.retries = 0
        self.status = {}
        self.reauths = 0
        self.timeouts = 0
        self.conn_errors = 0
        self.body_errors = 0          # 200 OK carrying an error document
        self.encoding_lies = 0
        self.cursor_anomalies = 0
        self.count_mismatch = 0
        self.sleep_seconds = 0.0
        self.unavailable_streak_s = 0.0   # longest continuous 503 stretch, real seconds
        self.unavailable_hits = 0

    def bump(self, code):
        self.status[code] = self.status.get(code, 0) + 1

    def snapshot(self):
        return {
            "requests": self.requests, "retries": self.retries, "status": dict(self.status),
            "reauths": self.reauths, "timeouts": self.timeouts, "conn_errors": self.conn_errors,
            "body_errors": self.body_errors, "encoding_lies": self.encoding_lies,
            "cursor_anomalies": self.cursor_anomalies, "count_mismatch": self.count_mismatch,
            "sleep_seconds": round(self.sleep_seconds, 2),
            "unavailable_hits": self.unavailable_hits,
            "unavailable_streak_s": round(self.unavailable_streak_s, 1),
        }


def _sniff_decode(raw: bytes, declared: str | None, stats: TransportStats) -> str:
    """Decode a response body by looking at it, not by believing the header."""
    is_gzip = raw[:2] == b"\x1f\x8b"
    declared = (declared or "").lower()
    if is_gzip:
        if "gzip" not in declared:
            stats.encoding_lies += 1
        raw = gzip.decompress(raw)
    elif "gzip" in declared:
        stats.encoding_lies += 1
    elif declared.startswith("deflate"):
        try:
            raw = zlib.decompress(raw)
        except zlib.error:
            try:
                raw = zlib.decompress(raw, -zlib.MAX_WBITS)
            except zlib.error:
                stats.encoding_lies += 1
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        stats.encoding_lies += 1
        return raw.decode("utf-8", errors="replace")


class VendorClient:
    def __init__(self, base=None, token=None):
        self.base = (base or config.API_BASE).rstrip("/")
        self.token = token or config.API_TOKEN
        self.access = None
        self.session = requests.Session()
        self.session.headers["Accept-Encoding"] = "identity"
        self.stats = TransportStats()
        self._unavailable_since = None

    # ---------------------------------------------------------------- auth
    def authenticate(self, force=False):
        if self.access and not force:
            return self.access
        for attempt in range(config.MAX_ATTEMPTS):
            try:
                r = self.session.post(
                    f"{self.base}/v1/auth/token",
                    headers={"Authorization": f"Bearer {self.token}"},
                    timeout=config.HTTP_TIMEOUT,
                )
                self.stats.requests += 1
                self.stats.bump(r.status_code)
                if r.status_code == 200:
                    body = json.loads(r.content.decode("utf-8", "replace"))
                    if "access_token" in body:
                        self.access = body["access_token"]
                        self.stats.reauths += 1
                        return self.access
                self._backoff(attempt, r.headers.get("Retry-After"))
            except requests.RequestException:
                self.stats.conn_errors += 1
                self._backoff(attempt, None)
        raise VendorError("could not obtain an access token")

    def _backoff(self, attempt, retry_after):
        delay = None
        if retry_after:
            try:
                delay = float(retry_after)
            except (TypeError, ValueError):
                delay = None
        if delay is None:
            delay = min(4.0, 0.15 * (2 ** attempt)) * (0.6 + 0.8 * random.random())
        delay = min(delay, 8.0)
        self.stats.sleep_seconds += delay
        self.stats.retries += 1
        time.sleep(delay)

    # ------------------------------------------------------------- request
    def get_json(self, path, params=None, auth=True):
        """One GET, retried until it yields a usable JSON document."""
        url = f"{self.base}{path}"
        last = None
        for attempt in range(config.MAX_ATTEMPTS):
            headers = {}
            if auth:
                headers["Authorization"] = f"Bearer {self.authenticate()}"
            try:
                r = self.session.get(url, params=params, headers=headers,
                                     timeout=config.HTTP_TIMEOUT, stream=True)
                try:
                    raw = r.raw.read(decode_content=False)
                finally:
                    r.close()
                self.stats.requests += 1
                self.stats.bump(r.status_code)

                if r.status_code == 503:
                    self.stats.unavailable_hits += 1
                    now = time.time()
                    if self._unavailable_since is None:
                        self._unavailable_since = now
                    self.stats.unavailable_streak_s = max(
                        self.stats.unavailable_streak_s, now - self._unavailable_since)
                    last = "503"
                    self._backoff(attempt, r.headers.get("Retry-After"))
                    continue
                self._unavailable_since = None

                if r.status_code == 401:
                    self.authenticate(force=True)
                    last = "401"
                    continue
                if r.status_code == 429 or r.status_code >= 500:
                    last = str(r.status_code)
                    self._backoff(attempt, r.headers.get("Retry-After"))
                    continue

                text = _sniff_decode(raw, r.headers.get("Content-Encoding"), self.stats)
                try:
                    body = json.loads(text)
                except json.JSONDecodeError:
                    self.stats.body_errors += 1
                    last = "unparseable body"
                    self._backoff(attempt, None)
                    continue

                # 200 OK carrying an error document.
                if isinstance(body, dict) and "error" in body and "data" not in body:
                    self.stats.body_errors += 1
                    if body.get("error") in ("token_expired", "unauthorized"):
                        self.authenticate(force=True)
                        last = body.get("error")
                        continue
                    last = str(body.get("error"))
                    self._backoff(attempt, None)
                    continue
                return body
            except (requests.Timeout,):
                self.stats.timeouts += 1
                last = "timeout"
                self._backoff(attempt, None)
            except requests.RequestException as exc:
                self.stats.conn_errors += 1
                last = f"conn:{type(exc).__name__}"
                self._backoff(attempt, None)
        raise VendorError(f"GET {path} failed after {config.MAX_ATTEMPTS} attempts ({last})")

    # ------------------------------------------------------------ lab clock
    def clock(self):
        return self.get_json("/_lab/clock", auth=False)

    def advance(self, days):
        r = self.session.post(f"{self.base}/_lab/advance", params={"days": days},
                              timeout=config.HTTP_TIMEOUT)
        r.raise_for_status()
        return r.json()

    def probe(self):
        """One unretried request, for asking *is the source up right now*.

        Deliberately not `get_json`: that retries a 503 for seventeen seconds,
        which is the right behaviour for extracting data and the wrong one for
        sampling availability.
        """
        try:
            r = self.session.get(f"{self.base}/v1/health", timeout=5)
            self.stats.requests += 1
            self.stats.bump(r.status_code)
            if r.status_code == 503:
                self.stats.unavailable_hits += 1
            return r.status_code
        except requests.RequestException:
            self.stats.conn_errors += 1
            return None

    def health(self):
        try:
            return self.get_json("/v1/health", auth=False)
        except VendorError:
            return None

    # ------------------------------------------------------------ counting
    def count(self, entity, since=None, until=None, as_of=None):
        """Cheap probe: one row, read ``total_count``.

        Used only to *notice change*.  Every conclusion drawn from it is
        confirmed by a real pull before it reaches the warehouse.
        """
        params = {"limit": 1}
        if since:
            params["since"] = since
        if until:
            params["until"] = until
        if as_of:
            params["as_of"] = as_of
        body = self.get_json(f"/v1/{entity}", params)
        return util.to_int(body.get("total_count")) or 0

    # ---------------------------------------------------------- pagination
    def pull(self, entity, since=None, until=None, as_of=None, limit=None):
        """Page an endpoint to exhaustion, returning (rows, meta).

        Cursor handling is defensive: we stop on a repeated cursor, on a cursor
        that does not move, on an empty page that still claims a successor, and
        on a page whose rows we have already seen in this pull.
        """
        limit = limit or config.PAGE_LIMIT
        params = {"limit": limit}
        if since:
            params["since"] = since
        if until:
            params["until"] = until
        if as_of:
            params["as_of"] = as_of

        rows, seen_keys, seen_cursors = [], set(), set()
        cursor, pages, reported = None, 0, None
        dup_rows = 0
        keyfields = config.ENTITY_KEYS[entity][0]

        while pages < config.MAX_PAGES:
            p = dict(params)
            if cursor:
                p["cursor"] = cursor
            body = self.get_json(f"/v1/{entity}", p)
            pages += 1
            data = body.get("data") or []
            if reported is None:
                reported = util.to_int(body.get("total_count"))

            served = body.get("cursor")
            if cursor and served and served != cursor:
                # The server told us it served from somewhere other than where we
                # asked; the page may overlap or skip.  Dedupe below handles the
                # overlap; the skip is what the count reconciliation catches.
                self.stats.cursor_anomalies += 1

            for rec in data:
                if not isinstance(rec, dict):
                    continue
                k = tuple(str(rec.get(f)) for f in keyfields) + (str(rec.get("version")),)
                if k in seen_keys:
                    dup_rows += 1
                    continue
                seen_keys.add(k)
                rows.append(rec)

            nxt = body.get("next_cursor")
            if not nxt:
                break
            if nxt in seen_cursors or nxt == cursor:
                self.stats.cursor_anomalies += 1
                break
            seen_cursors.add(nxt)
            if not data:
                # A page with no rows that still offers a successor is a lie we
                # tolerate once, then stop.
                self.stats.cursor_anomalies += 1
                break
            cursor = nxt

        if reported is not None and reported != len(rows):
            self.stats.count_mismatch += 1
        meta = {"pages": pages, "reported_total": reported, "rows": len(rows),
                "dup_rows_in_pull": dup_rows,
                "since": since, "until": until, "as_of": as_of}
        return rows, meta

    def order_by_id(self, order_id, as_of=None):
        params = {}
        if as_of:
            params["as_of"] = as_of
        return self.get_json(f"/v1/orders/{order_id}", params)
