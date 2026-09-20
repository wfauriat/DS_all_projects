"""The bridge: source SSE stream → contract check → Kafka. At-least-once, resumable.

    source-api   GET /v1/stream   (Last-Event-ID: <checkpoint>)
        │ SSE frames: id = seq, event = event_type, data = the event as JSON
        ▼
    contract.route()   valid   → bikeshare.trip-events.v1 / bikeshare.station-status.v1
        │              invalid → bikeshare.dlq.v1, with the validation errors
        ▼
    KafkaSink.send()   idempotent producer, batches in the background
        │  every CHECKPOINT_EVERY_S: flush() → everything acknowledged → checkpoint.save(seq)
        ▼
    data/bridge/checkpoint.json   where to resume after a restart or a dropped connection

What goes wrong, and what the bridge does about it:
    source unreachable, restarting    reconnect with backoff, resume after the checkpoint
    `gap` event from the source       events were lost upstream (the buffer moved on): logged, counted
    resume point ahead of the source  the source was reset (new world): start again from its oldest event
    Kafka does not acknowledge        no checkpoint; reconnect from the last checkpoint and re-send
    SIGTERM (docker stop)             flush, checkpoint, exit (see close())
"""

import json
import logging
import time
from collections import Counter
from collections.abc import Callable
from datetime import UTC, datetime

import httpx

from . import contract, sse
from .checkpoint import Checkpoint
from .config import Settings
from .logs import event
from .sink import DeliveryFailed, KafkaSink

log = logging.getLogger("bridge")

BACKOFF_S = (1, 2, 4, 8, 15, 30)  # waits between reconnection attempts; the last one repeats


class Bridge:
    def __init__(
        self,
        settings: Settings,
        sink: KafkaSink,
        checkpoint: Checkpoint,
        http: httpx.Client,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.settings = settings
        self.sink = sink
        self.checkpoint = checkpoint
        self.http = http
        self.clock = clock

        self.last_seq: int | None = checkpoint.load()  # last seq handed to the producer
        self.saved_seq: int | None = self.last_seq  # last seq known to be in Kafka
        self._last_checkpoint = self._last_report = clock()

        # Counters. The window ones reset at every throughput report.
        self.window: Counter[str] = Counter()
        self.totals: Counter[str] = Counter()
        self.last_event_time: str | None = None

    # ── main loop ───────────────────────────────────────────────────────────
    def run(self) -> None:
        """Consume forever: one connection at a time, reconnecting with backoff."""
        failures = 0
        while True:
            try:
                received = self.consume_once()
                failures = 0 if received else failures + 1
                event(
                    log,
                    "stream closed by the source",
                    level=logging.WARNING,
                    last_seq=self.last_seq,
                )
            except (httpx.TransportError, httpx.HTTPStatusError) as exc:
                failures += 1
                event(log, "source unavailable", level=logging.WARNING, error=repr(exc))
            except DeliveryFailed as exc:
                failures += 1
                event(log, "kafka delivery failed: rewinding to the checkpoint",
                      level=logging.ERROR, error=str(exc), rewind_to=self.saved_seq)  # fmt: skip
                self.last_seq = self.saved_seq
            self.totals["reconnects"] += 1
            time.sleep(BACKOFF_S[min(failures, len(BACKOFF_S) - 1)])

    def consume_once(self) -> int:
        """Open one SSE connection and handle frames until it ends. Returns frames received."""
        after_seq = self._resume_point()
        event(log, "connecting", source=self.settings.source_url, after_seq=after_seq)
        received = 0
        with sse.open_stream(
            self.http, f"{self.settings.source_url}/v1/stream", after_seq
        ) as frames:
            event(log, "connected", after_seq=after_seq)
            for frame in frames:
                if frame.event != sse.KEEPALIVE:
                    self.handle(frame)
                    received += 1
                self.maybe_checkpoint()
                self.maybe_report()
        return received

    def handle(self, frame: sse.SseFrame) -> None:
        if frame.event == "gap":
            gap = json.loads(frame.data)
            lost = gap["oldest_available_seq"] - gap["requested_after_seq"] - 1
            self.totals["lost_upstream"] += lost
            event(log, "gap: events lost upstream (the source buffer moved on)",
                  level=logging.ERROR, lost=lost, **gap)  # fmt: skip
            self.last_seq = gap["oldest_available_seq"] - 1  # the next frame continues from there
            return

        seq = int(frame.id or 0)
        if self.last_seq is not None and seq != self.last_seq + 1:
            event(
                log,
                "unexpected seq jump",
                level=logging.WARNING,
                expected=self.last_seq + 1,
                got=seq,
            )

        routed = contract.route(frame.data, seq, self.settings.topics, ingested_at=_now_iso())
        self.sink.send(routed)
        self.last_seq = seq

        kind = routed.event_type or f"dlq:{routed.error_type}"
        self.window[kind] += 1
        self.totals[kind] += 1
        if routed.event_time:
            self.last_event_time = routed.event_time

    # ── periodic work ───────────────────────────────────────────────────────
    def maybe_checkpoint(self, force: bool = False) -> None:
        due = self.clock() - self._last_checkpoint >= self.settings.checkpoint_every_s
        if not (due or force) or self.last_seq in (None, self.saved_seq):
            return
        self.sink.flush(timeout=30)  # raises DeliveryFailed: the checkpoint then stays put
        self.checkpoint.save(self.last_seq)
        self.saved_seq = self.last_seq
        self._last_checkpoint = self.clock()

    def maybe_report(self) -> None:
        elapsed = self.clock() - self._last_report
        if elapsed < self.settings.report_every_s:
            return
        received = sum(self.window.values())
        source_seq = self._source_last_seq()
        event(
            log,
            "throughput",
            window_s=round(elapsed, 1),
            received=received,
            per_s=round(received / elapsed, 1),
            by_type=dict(self.window),
            last_seq=self.last_seq,
            checkpoint=self.saved_seq,
            source_lag=None if source_seq is None else source_seq - (self.last_seq or 0),
            last_event_time=self.last_event_time,
            totals=dict(self.totals),
            delivered=dict(self.sink.delivered),
        )
        self.window.clear()
        self._last_report = self.clock()

    def close(self) -> None:
        """Shutdown: make sure everything read is in Kafka, and remember where we are."""
        try:
            self.maybe_checkpoint(force=True)
            event(log, "stopped cleanly", checkpoint=self.saved_seq)
        except DeliveryFailed as exc:
            event(log, "stopped with undelivered messages: they will be re-sent",
                  level=logging.ERROR, error=str(exc), checkpoint=self.saved_seq)  # fmt: skip

    # ── helpers ─────────────────────────────────────────────────────────────
    def _resume_point(self) -> int | None:
        if self.last_seq is None:  # first start ever
            return 0 if self.settings.start_from == "oldest" else None
        source_seq = self._source_last_seq()
        if source_seq is not None and self.last_seq > source_seq:
            event(log, "resume point is ahead of the source: was it reset? starting from its oldest event",
                  level=logging.ERROR, checkpoint=self.last_seq, source_last_seq=source_seq)  # fmt: skip
            self.last_seq = self.saved_seq = None
            self.checkpoint.clear()
            return 0
        return self.last_seq

    def _source_last_seq(self) -> int | None:
        try:
            return int(self.http.get(f"{self.settings.source_url}/health").json()["last_seq"])
        except (httpx.HTTPError, ValueError, KeyError):
            return None


def _now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")
