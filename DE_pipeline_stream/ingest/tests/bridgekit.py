"""Test helpers: a fake Kafka producer and a scripted fake source, so the bridge
can be tested end to end with no network, no Kafka and no source process.

(A plain module rather than conftest.py, so tests can import it. pytest runs in
importlib mode, and pyproject.toml puts this folder on `pythonpath`.)
"""

import json
from collections.abc import Iterable
from dataclasses import dataclass, field
from types import SimpleNamespace

import httpx

from bikeshare_bridge.config import Topics

TOPICS = Topics()


def sample(event_type: str, seq: int = 1, **payload) -> dict:
    """A contract-valid event; keyword arguments override payload fields."""
    payloads = {
        "trip_started": {"trip_id": "TR-00000001", "bike_id": "BK-0001", "bike_type": "electric",
                         "station_id": "ST-001", "rider_type": "member"},
        "trip_ended": {"trip_id": "TR-00000001", "bike_id": "BK-0001", "bike_type": "electric",
                       "station_id": "ST-002", "start_station_id": "ST-001", "duration_s": 754},
        "station_status": {"station_id": "ST-001", "bikes_available": 12, "ebikes_available": 4,
                           "docks_available": 8, "is_renting": True},
    }  # fmt: skip
    return {
        "event_id": f"00000000-0000-4000-8000-{seq:012d}",
        "event_type": event_type,
        "schema_version": 1,
        "seq": seq,
        "event_time": "2026-03-02T08:00:00.000Z",
        "emitted_at": "2026-03-02T08:00:10.000Z",
        "payload": {**payloads[event_type], **payload},
    }


def sse_body(*frames: dict | tuple[str, dict]) -> bytes:
    """SSE wire text. A dict is an event frame; a ("gap", {...}) tuple is a notice without an id."""
    out = []
    for frame in frames:
        if isinstance(frame, tuple):
            name, data = frame
            out.append(f"event: {name}\ndata: {json.dumps(data)}\n\n")
        else:
            out.append(
                f"id: {frame['seq']}\nevent: {frame['event_type']}\ndata: {json.dumps(frame)}\n\n"
            )
        out.append(": keepalive\n\n")
    return "".join(out).encode()


@dataclass
class FakeSource:
    """An httpx transport playing the source: /health, then one scripted SSE body per connection."""

    streams: list[bytes]
    last_seq: int = 1_000
    requests: list[httpx.Request] = field(default_factory=list)

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if request.url.path == "/health":
            return httpx.Response(200, json={"status": "ok", "last_seq": self.last_seq})
        return httpx.Response(
            200, headers={"content-type": "text/event-stream"}, content=self.streams.pop(0)
        )

    def client(self) -> httpx.Client:
        return httpx.Client(transport=httpx.MockTransport(self), base_url="http://source")

    @property
    def resume_headers(self) -> list[str | None]:
        return [r.headers.get("last-event-id") for r in self.requests if r.url.path == "/v1/stream"]


class FakeProducer:
    """Just enough of confluent_kafka.Producer: messages are 'acknowledged' on poll/flush."""

    def __init__(self, config: dict | None = None, fail_topics: Iterable[str] = ()) -> None:
        self.config = config
        self.fail_topics = set(fail_topics)
        self.messages: list[SimpleNamespace] = []
        self._pending: list[tuple] = []

    def produce(self, topic, key=None, value=None, headers=None, on_delivery=None) -> None:
        self._pending.append((topic, key, value, headers, on_delivery))

    def poll(self, timeout: float = 0) -> int:
        return self._deliver()

    def flush(self, timeout: float | None = None) -> int:
        self._deliver()
        return 0

    def list_topics(self, timeout: float | None = None) -> SimpleNamespace:
        partitions = {0: None, 1: None, 2: None}
        return SimpleNamespace(
            topics={t: SimpleNamespace(partitions=partitions) for t in TOPICS.all()}
        )

    def _deliver(self) -> int:
        for topic, key, value, headers, callback in self._pending:
            error = "broker unavailable" if topic in self.fail_topics else None
            if error is None:
                self.messages.append(
                    SimpleNamespace(topic=topic, key=key, value=value, headers=dict(headers))
                )
            callback(error, SimpleNamespace(topic=lambda t=topic: t))
        count, self._pending = len(self._pending), []
        return count

    def on(self, topic: str) -> list[SimpleNamespace]:
        return [m for m in self.messages if m.topic == topic]
