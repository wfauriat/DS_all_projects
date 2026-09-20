"""The bridge's copy of the source contract, and the routing that follows from it.

Producer and consumer share a contract, not code. This file restates what the
source promises (source/bikeshare_sim/schemas.py documents the same shapes).
If the vendor changes a payload without notice (the schema_drift fault),
validation fails here, and the event goes to the dead-letter topic instead of
poisoning every downstream consumer.

    SSE data ──► route() ──► Routed(topic, key, value, headers)     valid event
                          └► Routed(dlq, key, error record, …)      anything else

What is checked here: shape and types, the "syntax". A negative bike count is a
contract violation (bikes_available ≥ 0). Checks that need other data are
semantic, like "more bikes than the station has docks" (which needs capacity),
and belong downstream (Spark, dbt).
"""

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, ValidationError

from .config import Topics

BikeType = Literal["mechanical", "electric"]


class _Strict(BaseModel):
    # strict: "754s" is not an int, "true" is not a bool. extra=forbid: renamed fields fail too.
    model_config = ConfigDict(extra="forbid", strict=True)


class TripStarted(_Strict):
    trip_id: str
    bike_id: str
    bike_type: BikeType
    station_id: str
    rider_type: Literal["member", "casual"]


class TripEnded(_Strict):
    trip_id: str
    bike_id: str
    bike_type: BikeType
    station_id: str
    start_station_id: str
    duration_s: int = Field(ge=0)


class StationStatus(_Strict):
    station_id: str
    bikes_available: int = Field(ge=0)
    ebikes_available: int = Field(ge=0)
    docks_available: int = Field(ge=0)
    is_renting: bool


class _Envelope(BaseModel):
    event_id: UUID
    schema_version: Literal[1]
    seq: int
    event_time: datetime
    emitted_at: datetime


class TripStartedEvent(_Envelope):
    event_type: Literal["trip_started"]
    payload: TripStarted


class TripEndedEvent(_Envelope):
    event_type: Literal["trip_ended"]
    payload: TripEnded


class StationStatusEvent(_Envelope):
    event_type: Literal["station_status"]
    payload: StationStatus


Event = Annotated[
    TripStartedEvent | TripEndedEvent | StationStatusEvent, Field(discriminator="event_type")
]
EVENT_ADAPTER = TypeAdapter(Event)

# event_type → (topic, payload field used as the message key). The key picks
# the partition, and so decides which events keep their relative order.
ROUTES = {
    "trip_started": ("trip_events", "bike_id"),
    "trip_ended": ("trip_events", "bike_id"),
    "station_status": ("station_status", "station_id"),
}


@dataclass(frozen=True)
class Routed:
    """One Kafka message, ready to produce."""

    topic: str
    key: str | None
    value: bytes
    headers: list[tuple[str, str]]
    event_type: str | None = None  # None for dead letters
    event_time: str | None = None
    error_type: str | None = None  # set for dead letters


def route(data: str, seq: int, topics: Topics, ingested_at: str) -> Routed:
    try:
        event = EVENT_ADAPTER.validate_json(data)
    except ValidationError as exc:
        return _dead_letter(data, seq, topics, ingested_at, exc)

    topic_attr, key_field = ROUTES[event.event_type]
    return Routed(
        topic=getattr(topics, topic_attr),
        key=getattr(event.payload, key_field),
        value=data.encode(),  # the vendor's bytes, untouched: exact lineage
        headers=[
            ("event_type", event.event_type),
            ("schema_version", str(event.schema_version)),
            ("source_seq", str(seq)),
            ("ingested_at", ingested_at),  # the pipeline's clock (wall time)
        ],
        event_type=event.event_type,
        event_time=event.event_time.isoformat(),
    )


def _dead_letter(
    data: str, seq: int, topics: Topics, ingested_at: str, exc: ValidationError
) -> Routed:
    kinds = {e["type"] for e in exc.errors()}
    if "json_invalid" in kinds:
        error_type = "unparseable"
    elif "union_tag_invalid" in kinds or "union_tag_not_found" in kinds:
        error_type = "unknown_event_type"
    else:
        error_type = "contract_violation"

    errors = [f"{'.'.join(map(str, e['loc']))}: {e['msg']}" for e in exc.errors()]
    record = {
        "error_type": error_type,
        "errors": errors,
        "source_seq": seq,
        "ingested_at": ingested_at,
        "raw": data,  # the original text, so the event can be replayed once fixed
    }
    return Routed(
        topic=topics.dlq,
        key=_event_id_or_none(data),
        value=json.dumps(record).encode(),
        headers=[
            ("error_type", error_type),
            ("source_seq", str(seq)),
            ("ingested_at", ingested_at),
        ],
        error_type=error_type,
    )


def _event_id_or_none(data: str) -> str | None:
    try:
        event_id = json.loads(data).get("event_id")
    except (ValueError, AttributeError):
        return None
    return event_id if isinstance(event_id, str) else None
