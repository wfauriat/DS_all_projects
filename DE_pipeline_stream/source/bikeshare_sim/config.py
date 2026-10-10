"""Settings of the source, loaded from config/source.toml plus a few env overrides.

Who calls this: api.create_app() at startup, and the tests.
Resolution order: TOML file → env overrides → pydantic validation.

Environment variables (set by docker-compose.yml from .env, or by `make source-run`):
    BIKESHARE_CONFIG  path of the TOML file   (default: config/source.toml)
    SIM_SEED          overrides [world].seed
    SIM_SPEED         overrides [clock].speed
    SIM_STATE_DIR     where state.pkl is saved (default: data/source)
The relative defaults resolve against the working directory: the repo root when
run locally, /app in the container, where compose mounts ./config and ./data/source.
"""

import os
import tomllib
from collections.abc import Mapping
from datetime import datetime
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

DEFAULT_CONFIG_PATH = "config/source.toml"
DEFAULT_STATE_DIR = "data/source"


class WorldConfig(BaseModel):
    seed: int = 42
    n_stations: int = Field(40, ge=3, le=150)
    n_bikes: int = Field(600, ge=1)
    ebike_share: float = Field(0.35, ge=0, le=1)
    city_center: tuple[float, float] = (45.0, 5.0)
    city_radius_km: float = Field(4.0, gt=0.5)
    trips_per_station_day: float = Field(75.0, ge=0)
    member_share: float = Field(0.7, ge=0, le=1)


class ClockConfig(BaseModel):
    start: datetime
    speed: float = Field(60, gt=0)
    step_seconds: int = Field(10, ge=1, le=300)
    tick_ms: int = Field(200, ge=10)

    @field_validator("start")
    @classmethod
    def _must_be_utc_aware(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("clock.start needs a timezone, e.g. 2026-03-02T05:00:00Z")
        return v


class EmissionConfig(BaseModel):
    status_interval_min: float = Field(5, gt=0)
    rebalancing_every_h: float = Field(3, gt=0)
    capacity_changes_per_day: float = Field(0.2, ge=0)
    buffer_size: int = Field(50_000, ge=100)


class FaultConfig(BaseModel):
    """One [faults.<name>] table. Any key besides enabled/rate is a fault parameter."""

    model_config = ConfigDict(extra="allow")

    enabled: bool = True
    rate: float = Field(0.0, ge=0)

    def param(self, key: str, default: Any) -> Any:
        return (self.model_extra or {}).get(key, default)


class Settings(BaseModel):
    world: WorldConfig
    clock: ClockConfig
    emission: EmissionConfig
    faults: dict[str, FaultConfig] = {}
    state_dir: Path = Path(DEFAULT_STATE_DIR)


def load_settings(path: str | Path | None = None, env: Mapping[str, str] | None = None) -> Settings:
    env = os.environ if env is None else env
    config_path = Path(path or env.get("BIKESHARE_CONFIG", DEFAULT_CONFIG_PATH))
    raw = tomllib.loads(config_path.read_text())

    if "SIM_SEED" in env:
        raw.setdefault("world", {})["seed"] = int(env["SIM_SEED"])
    if "SIM_SPEED" in env:
        raw.setdefault("clock", {})["speed"] = float(env["SIM_SPEED"])
    raw["state_dir"] = env.get("SIM_STATE_DIR", DEFAULT_STATE_DIR)

    return Settings.model_validate(raw)
