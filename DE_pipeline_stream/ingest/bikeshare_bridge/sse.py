"""A minimal Server-Sent Events client on top of httpx. The wire format is simple:

    id: 42
    event: trip_started
    data: {"event_id": "...", ...}
                                   ← a blank line ends a frame
    : keepalive                    ← a comment line (the source sends one when idle)

parse() turns lines into SseFrame objects, and open_stream() opens one streaming
HTTP request that resumes after a given seq (standard Last-Event-ID header).
"""

from collections.abc import Generator, Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass

import httpx

# Event name given to comment lines, so that an idle stream still wakes the bridge's loop.
KEEPALIVE = "__keepalive__"


@dataclass(frozen=True)
class SseFrame:
    event: str
    data: str
    id: str | None = None


def parse(lines: Iterable[str]) -> Iterator[SseFrame]:
    event, data, frame_id = "message", [], None
    for line in lines:
        if line == "":  # end of frame
            if data:
                yield SseFrame(event, "\n".join(data), frame_id)
            event, data, frame_id = "message", [], None
        elif line.startswith(":"):
            yield SseFrame(KEEPALIVE, line[1:].strip())
        else:
            name, _, value = line.partition(":")
            value = value.removeprefix(" ")
            if name == "event":
                event = value
            elif name == "data":
                data.append(value)
            elif name == "id":
                frame_id = value
            # "retry" and unknown fields are ignored, as the SSE spec says


@contextmanager
def open_stream(
    client: httpx.Client, url: str, after_seq: int | None
) -> Generator[Iterator[SseFrame], None, None]:
    """One connection. after_seq=None means "live, from now"; 0 means "from the oldest buffered"."""
    headers = {"Accept": "text/event-stream"}
    if after_seq is not None:
        headers["Last-Event-ID"] = str(after_seq)
    with client.stream("GET", url, headers=headers) as response:
        response.raise_for_status()
        yield parse(response.iter_lines())
