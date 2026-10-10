"""Event log: the vendor's outbox. It numbers events, keeps the most recent ones,
and wakes up SSE subscribers.

    FaultLayer output ──► EventLog.publish() ──► ring buffer of the last N events, as JSON
                                                   ├──► GET /v1/stream  (SSE: live, plus resume)
                                                   └──► GET /v1/events  (JSON replay by seq)

`seq` is the SSE `id:`. A consumer that reconnects with `Last-Event-ID: <seq>` gets
everything after that seq, as long as it is still in the buffer. Otherwise it gets
a `gap` notice saying where the buffer now starts. That is the source's delivery
guarantee: at-least-once from the consumer's resume point, bounded by the buffer size.
"""

import asyncio
import json
from collections import deque
from datetime import datetime
from itertools import islice


def to_json(obj: dict) -> str:
    return json.dumps(obj, default=_json_default, separators=(",", ":"))


def _json_default(value: object) -> str:
    if isinstance(value, datetime):
        # ISO 8601 in UTC with millisecond precision: 2026-03-02T08:14:22.123Z
        return value.isoformat(timespec="milliseconds").replace("+00:00", "Z")
    raise TypeError(f"not JSON serializable: {type(value).__name__}")


class EventLog:
    def __init__(self, maxlen: int) -> None:
        self._buffer: deque[tuple[int, str, str]] = deque(maxlen=maxlen)  # (seq, type, json)
        self.last_seq = 0
        self.closed = False
        self._new_data = asyncio.Event()

    def close(self) -> None:
        """Server shutdown: wake every subscriber so its SSE stream can end cleanly.
        Clients reconnect later with Last-Event-ID and lose nothing."""
        self.closed = True
        self._new_data.set()

    def publish(self, events: list[dict], emitted_at: datetime) -> None:
        """Number the events, stamp their emission time, serialize them and wake subscribers."""
        if not events:
            return
        for e in events:
            self.last_seq += 1
            record = {  # canonical key order of the envelope (see schemas.py)
                "event_id": e["event_id"],
                "event_type": e["event_type"],
                "schema_version": e["schema_version"],
                "seq": self.last_seq,
                "event_time": e["event_time"],
                "emitted_at": emitted_at,
                "payload": e["payload"],
            }
            self._buffer.append((self.last_seq, e["event_type"], to_json(record)))
        # Wake everyone waiting on the current Event, then arm a fresh one.
        self._new_data.set()
        self._new_data = asyncio.Event()

    @property
    def first_seq(self) -> int:
        """Oldest seq still in the buffer (last_seq + 1 when the buffer is empty)."""
        return self.last_seq - len(self._buffer) + 1

    def read_after(
        self, after_seq: int, limit: int
    ) -> tuple[list[tuple[int, str, str]], dict | None]:
        """Return up to `limit` records with seq > after_seq, plus a gap notice if some
        of them have already fallen out of the buffer."""
        first = self.first_seq
        gap = None
        if after_seq < first - 1:
            gap = {"requested_after_seq": after_seq, "oldest_available_seq": first}
            after_seq = first - 1
        start = after_seq - first + 1  # buffer position of seq after_seq + 1
        return list(islice(self._buffer, start, start + limit)), gap

    async def wait_for_new(self, timeout: float) -> bool:
        """Block until the next publish(). Returns False on timeout."""
        try:
            await asyncio.wait_for(self._new_data.wait(), timeout)
            return True
        except TimeoutError:
            return False

    def buffered(self) -> int:
        return len(self._buffer)

    # Persistence (pickle): the buffer and seq are saved, so consumers can resume
    # across a source restart. The asyncio.Event is not picklable, so it is recreated.
    def __getstate__(self) -> dict:
        return {"buffer": self._buffer, "last_seq": self.last_seq}

    def __setstate__(self, state: dict) -> None:
        self._buffer = state["buffer"]
        self.last_seq = state["last_seq"]
        self.closed = False
        self._new_data = asyncio.Event()
