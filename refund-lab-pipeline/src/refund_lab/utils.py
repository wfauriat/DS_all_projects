import json
import hashlib
import random
import base64


def content_hash(obj: dict) -> str:
    canonical = json.dumps(obj, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

def compute_backoff(tries: int, retry_after: float | None = None,
                     base: float = 0.2, max_delay: float = 10.0) -> float:
    if retry_after is not None:
        return retry_after
    delay = min(base * (2 ** tries), max_delay)
    jitter = random.uniform(0, delay * 0.1) 
    return delay + jitter

def b64decode_relaxed(s):
    s = s.strip()
    s += "=" * (-len(s) % 4)
    return base64.b64decode(s)