"""Settings of the bridge, from environment variables only.

Set by docker-compose.yml (service `bridge`), or by `make bridge-run` for a run on the host:

    variable            meaning                                  in a container         on the host
    SOURCE_URL          base URL of the source API               http://source-api:8000  http://localhost:8000
    KAFKA_BOOTSTRAP     where the producer first connects        kafka:9092 (INTERNAL)   localhost:9094 (EXTERNAL)
    BRIDGE_STATE_DIR    where checkpoint.json is kept            data/bridge (mounted)   data/bridge
    BRIDGE_START_FROM   first start only (no checkpoint yet):
                        "oldest" replays the source buffer, "live" starts from now
    CHECKPOINT_EVERY_S  flush Kafka + save the checkpoint every N real seconds (default 2)
    REPORT_EVERY_S      throughput log line every N real seconds (default 30)
    LOG_LEVEL           default info

The two Kafka addresses exist because Kafka tells each client which address to
use next (its "advertised listener"). See the kafka service in docker-compose.yml.
"""

import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal


@dataclass(frozen=True)
class Topics:
    """Topic names. They must match kafka/create-topics.sh (the broker won't auto-create them)."""

    trip_events: str = "bikeshare.trip-events.v1"
    station_status: str = "bikeshare.station-status.v1"
    dlq: str = "bikeshare.dlq.v1"

    def all(self) -> list[str]:
        return [self.trip_events, self.station_status, self.dlq]


@dataclass(frozen=True)
class Settings:
    source_url: str = "http://localhost:8000"
    kafka_bootstrap: str = "localhost:9094"
    state_dir: Path = Path("data/bridge")
    start_from: Literal["oldest", "live"] = "oldest"
    checkpoint_every_s: float = 2.0
    report_every_s: float = 30.0
    log_level: str = "info"
    topics: Topics = field(default_factory=Topics)


def load_settings(env: Mapping[str, str] | None = None) -> Settings:
    env = os.environ if env is None else env
    start_from = env.get("BRIDGE_START_FROM", "oldest")
    if start_from not in ("oldest", "live"):
        raise ValueError(f"BRIDGE_START_FROM must be 'oldest' or 'live', got {start_from!r}")
    return Settings(
        source_url=env.get("SOURCE_URL", Settings.source_url).rstrip("/"),
        kafka_bootstrap=env.get("KAFKA_BOOTSTRAP", Settings.kafka_bootstrap),
        state_dir=Path(env.get("BRIDGE_STATE_DIR", str(Settings.state_dir))),
        start_from=start_from,
        checkpoint_every_s=float(env.get("CHECKPOINT_EVERY_S", Settings.checkpoint_every_s)),
        report_every_s=float(env.get("REPORT_EVERY_S", Settings.report_every_s)),
        log_level=env.get("LOG_LEVEL", Settings.log_level),
    )
