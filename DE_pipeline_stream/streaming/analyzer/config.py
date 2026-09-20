"""Settings of the Spark job, from environment variables (set by docker-compose.yml → spark).

Spark's own settings (master, memory, shuffle partitions, state store) are not
here. They live in streaming/conf/spark-defaults.conf.

    variable                 default               meaning
    KAFKA_BOOTSTRAP          kafka:9092            Kafka's INTERNAL listener
    SOURCE_URL               http://source-api:8000  for station reference data (/v1/stations)
    LAKE_DIR                 /data/lake            Parquet output (bind-mounted to ./data/lake)
    CHECKPOINT_DIR           /data/checkpoints     one sub-folder per query (./data/checkpoints)
    TRIGGER_SECONDS          10                    one micro-batch every N real seconds
    WATERMARK                30 minutes            how late (in event time) an event may be and still count
    STATION_WINDOW_MIN       30                    per-station metrics windows (→ the lake)
    HEALTH_WINDOW_MIN        120                   per-station windows judged for frozen/silent (→ alerts)
    STATUS_INTERVAL_MIN      5                     the source's status cadence: must match
                                                   config/source.toml [emission].status_interval_min
    MAX_TRIP_DURATION        3 hours               a start with no end after this is an orphan
    LATE_AFTER_MIN           25                    emitted_at − event_time above this raises late_event
    STARTING_OFFSETS         earliest              only for a query without a checkpoint yet
    MAX_OFFSETS_PER_TRIGGER  20000                 caps a micro-batch when catching up

Event-time durations are SIMULATED time. At the default 60× speed, the 30-minute
watermark is 30 real seconds.
"""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Topics:
    """Must match kafka/create-topics.sh."""

    trip_events: str = "bikeshare.trip-events.v1"
    station_status: str = "bikeshare.station-status.v1"
    alerts: str = "bikeshare.alerts.v1"


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap: str = "kafka:9092"
    source_url: str = "http://source-api:8000"
    lake_dir: str = "/data/lake"
    checkpoint_dir: str = "/data/checkpoints"
    trigger_seconds: int = 10
    watermark: str = "30 minutes"
    station_window_min: int = 30
    health_window_min: int = 120
    status_interval_min: int = 5
    max_trip_duration: str = "3 hours"
    late_after_min: float = 25.0
    starting_offsets: str = "earliest"
    max_offsets_per_trigger: int = 20_000
    topics: Topics = Topics()


def load_settings(env=None) -> Settings:
    env = os.environ if env is None else env
    d = Settings()
    return Settings(
        kafka_bootstrap=env.get("KAFKA_BOOTSTRAP", d.kafka_bootstrap),
        source_url=env.get("SOURCE_URL", d.source_url).rstrip("/"),
        lake_dir=env.get("LAKE_DIR", d.lake_dir),
        checkpoint_dir=env.get("CHECKPOINT_DIR", d.checkpoint_dir),
        trigger_seconds=int(env.get("TRIGGER_SECONDS", d.trigger_seconds)),
        watermark=env.get("WATERMARK", d.watermark),
        station_window_min=int(env.get("STATION_WINDOW_MIN", d.station_window_min)),
        health_window_min=int(env.get("HEALTH_WINDOW_MIN", d.health_window_min)),
        status_interval_min=int(env.get("STATUS_INTERVAL_MIN", d.status_interval_min)),
        max_trip_duration=env.get("MAX_TRIP_DURATION", d.max_trip_duration),
        late_after_min=float(env.get("LATE_AFTER_MIN", d.late_after_min)),
        starting_offsets=env.get("STARTING_OFFSETS", d.starting_offsets),
        max_offsets_per_trigger=int(env.get("MAX_OFFSETS_PER_TRIGGER", d.max_offsets_per_trigger)),
    )
