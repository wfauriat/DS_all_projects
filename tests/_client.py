"""A throwaway extraction client used only to prove properties of the lab.

OPERATOR: STOP.  Writing this is your exercise.  This one is deliberately a
single ugly function with no structure, no incrementality, no state and no
error taxonomy -- it exists to show that a full extraction is *possible* at
every hostility level, not to show you how to build one.  Copying it would
give you something that cannot do the job you actually have.
"""

from __future__ import annotations

import gzip
import json
import time

MAX_TRIES = 400


def _auth(client, config_token: str) -> str:
    r = client.post("/v1/auth/token", headers={"Authorization": f"Bearer {config_token}"})
    r.raise_for_status()
    return r.json()["access_token"]


def _unnull(obj):
    if isinstance(obj, dict):
        return {k: _unnull(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_unnull(v) for v in obj]
    return None if obj in ("", "None") else obj


def extract(client, endpoint: str, config_token: str, as_of: str, limit: int = 250) -> list[str]:
    token = _auth(client, config_token)
    seen: set[str] = set()
    cursor = None
    tries = 0
    while True:
        tries += 1
        if tries > MAX_TRIES:
            raise RuntimeError(f"gave up on /v1/{endpoint}")
        params = {"limit": limit}
        if cursor is None:
            params["as_of"] = as_of
        else:
            params["cursor"] = cursor
        try:
            r = client.get(f"/v1/{endpoint}", params=params,
                           headers={"Authorization": f"Bearer {token}"})
            raw = r.content
        except Exception:
            continue
        if r.status_code == 401:
            token = _auth(client, config_token)
            continue
        if r.status_code == 429:
            time.sleep(min(0.01, float(r.headers.get("Retry-After", 0) or 0)))
            continue
        if r.status_code >= 500:
            continue
        if r.status_code != 200:
            raise RuntimeError(f"{r.status_code} {raw[:200]!r}")
        try:
            body = json.loads(raw)
        except Exception:
            try:
                body = json.loads(gzip.decompress(raw))
            except Exception:
                continue
        if "data" not in body:
            continue
        if body.get("cursor") != cursor:
            continue                      # not the page we asked for
        for rec in body["data"]:
            seen.add(json.dumps(_unnull(rec), sort_keys=True, ensure_ascii=False))
        cursor = body.get("next_cursor")
        if not cursor:
            return sorted(seen)


def naive(client, endpoint: str, config_token: str, as_of: str, limit: int = 250) -> list[str]:
    """No retries, no re-auth, no cursor checking, long-lived token."""
    out: list[str] = []
    params = {"limit": limit, "as_of": as_of}
    headers = {"Authorization": f"Bearer {config_token}"}
    while True:
        r = client.get(f"/v1/{endpoint}", params=params, headers=headers)
        if r.status_code != 200:
            raise RuntimeError(f"status {r.status_code}")
        body = r.json()
        if "data" not in body:
            raise RuntimeError("no data in body")
        out.extend(json.dumps(x, sort_keys=True, ensure_ascii=False) for x in body["data"])
        nxt = body.get("next_cursor")
        if not nxt:
            return sorted(set(out))
        params = {"cursor": nxt, "limit": limit}
