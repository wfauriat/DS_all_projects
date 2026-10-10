"""The source's published contract: what consumers may rely on.

- The API uses these models for its responses and for /docs (OpenAPI).
- The tests check that the truthful stream (all faults off) conforms to them.
- The engine builds plain dicts instead, for speed and so that faults can corrupt
  them. A schema_drift fault produces exactly the payloads these models reject.
- The bridge (layer 2) keeps its own copy of the event models: producer and
  consumer share a contract, not code.

Envelope (every event, in this key order):
    event_id        UUID, unique per logical event (a duplicate reuses it)
    event_type      trip_started | trip_ended | station_status
    schema_version  1
    seq             position in the vendor's outbox (the SSE id)
    event_time      simulated time when it happened
    emitted_at      simulated time when the vendor sent it (≥ event_time)
    payload         depends on event_type
"""

from datetime import datetime
from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter

BikeType = Literal["mechanical", "electric"]


# ── Event payloads ──────────────────────────────────────────────────────────
class _Strict(BaseModel):
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
    station_id: str  # where the bike was docked
    start_station_id: str
    duration_s: int = Field(ge=0)


class StationStatus(_Strict):
    station_id: str
    bikes_available: int = Field(ge=0)
    ebikes_available: int = Field(ge=0)
    docks_available: int = Field(ge=0)
    is_renting: bool


# ── Envelopes (one per type, discriminated by event_type) ───────────────────
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
# EVENT_ADAPTER.validate_python(some_dict) raises a ValidationError on any contract breach.
EVENT_ADAPTER = TypeAdapter(Event)


class RawEvent(BaseModel):
    """An event as emitted. The payload is left loose because faults may have corrupted it."""

    event_id: str
    event_type: str
    schema_version: int
    seq: int
    event_time: datetime
    emitted_at: datetime
    payload: dict[str, Any]


class EventsPage(BaseModel):
    events: list[RawEvent]
    next_after_seq: int = Field(description="pass as after_seq to get the next page")
    gap: dict | None = Field(None, description="set when requested events fell out of the buffer")


# ── Reference and batch data ────────────────────────────────────────────────
class StationOut(BaseModel):
    station_id: str
    name: str
    lat: float
    lon: float
    capacity: int
    zone: Literal["residential", "business", "transit", "leisure"]
    installed_at: datetime
    updated_at: datetime


class BikeOut(BaseModel):
    bike_id: str
    bike_type: BikeType
    commissioned_at: datetime


class WeatherObservation(BaseModel):
    observed_at: datetime
    temperature_c: float
    precipitation_mm: float
    wind_kmh: float


# ── Admin ───────────────────────────────────────────────────────────────────
class ClockState(BaseModel):
    sim_time: datetime = Field(description="where the simulated clock is")
    engine_time: datetime = Field(description="up to where events have been generated")
    backlog_s: float = Field(description="sim_time − engine_time, large right after a fast-forward")
    speed: float
    paused: bool


class ClockUpdate(BaseModel):
    speed: float | None = Field(
        None, gt=0, le=100_000, description="simulated seconds per real second"
    )
    paused: bool | None = None


class AdvanceRequest(BaseModel):
    hours: float = Field(0, ge=0)
    minutes: float = Field(0, ge=0)


class FaultUpdate(BaseModel):
    enabled: bool | None = None
    rate: float | None = Field(None, ge=0)


class FaultTrigger(BaseModel):
    station_id: str | None = Field(
        None, description="episode faults only: which station (default: random)"
    )


class Health(BaseModel):
    status: Literal["ok", "degraded"]
    simulation_running: bool
    sim_time: datetime
    last_seq: int
