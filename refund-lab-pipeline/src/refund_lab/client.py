
import httpx
import time
import json

from .config import API_URL, DEV_TOKEN
from .utils import compute_backoff, b64decode_relaxed

class NonRetryable(Exception):
    """Raised for a response we should not retry (4xx other than 401,
    malformed body, etc.)"""
    pass

class RetryableAuth(Exception):
    """401 - needs a fresh token, not a wait"""
    pass

class RetryableBackoff(Exception):
    """429/5xx - needs a wait, possibly server-specified"""
    def __init__(self, retry_after=None):
        self.retry_after = retry_after

def auth_to_API(client: httpx.Client):
    resp = client.post(API_URL + "/v1/auth/token",
                   headers={"Authorization": f"Bearer {DEV_TOKEN}"})
    live_token = resp.json()["access_token"]
    client.headers["Authorization"] = f"Bearer {live_token}"

def classify_response(resp: httpx.Response):
    """
    Look at a response and decide what family of outcome it is.
    Raises the appropriate exception, or returns normally if it's a success.
    """
    if resp.status_code == 200:
        return
    if resp.status_code == 401:
        raise RetryableAuth()
    if resp.status_code in (429, 500, 502, 503, 504):
        retry_after = resp.headers.get("retry-after")
        raise RetryableBackoff(
            retry_after=float(retry_after) if retry_after else None)
    raise NonRetryable(f"status {resp.status_code}: {resp.text[:200]}")


def fetch_page_with_retry(client: httpx.Client, entity: str,
               cursor: str | None,
               as_of: str | None = None,
               since: str | None = None,
               until: str | None = None,
               limit=5, max_retries=5) -> dict:
    tries = 0
    while tries < max_retries:
            try:
                resp = client.get("v1/" + entity, 
                params={"limit":limit,
                        **({"cursor": cursor} if cursor else {}),
                        **({"as_of": as_of} if as_of else {}),
                        **({"since": since} if since else {}),
                        **({"until": until} if until else {})})
                classify_response(resp)
                payload = resp.json()
                return payload
            except RetryableAuth:
                auth_to_API(client)
            except RetryableBackoff as e:
                time.sleep(compute_backoff(tries, retry_after=e.retry_after))
            except NonRetryable as e:
                raise NonRetryable(f"page failed at cursor {cursor}") from e
            tries+=1
    else:
        raise RuntimeError(f"exhausted retries")

def decode_cursor(cursor: str | None) -> dict | None:
    if cursor is None:
        return None
    decoded = json.loads(b64decode_relaxed(cursor))
    dict_cursor = {
        "entity": decoded["t"],
        "as_of": decoded.get("a"),
        "total_count": decoded["n"],
        "rows_served": decoded["r"],
        "since": decoded.get("s"),
        "until": decoded.get("u"),
        "last_seen_value": decoded["v"],
        "prev_rows_served": decoded.get("pr"),
        "prev_last_seen_value": decoded.get("pv")
    }
    return dict_cursor
