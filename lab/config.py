"""Configuration loading for the lab.

Reads ``lab.toml`` from the repository root.  Also owns the canonical paths of
everything the lab persists under ``.state/``.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import tomllib
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

# Both overridable so the lab's own test suite can build throwaway worlds
# without disturbing a live run.  Nothing in normal operation sets them.
CONFIG_PATH = Path(os.environ.get("REFUND_LAB_CONFIG") or (REPO_ROOT / "lab.toml"))
STATE_DIR = Path(os.environ.get("REFUND_LAB_STATE") or (REPO_ROOT / ".state"))
LEDGER_PATH = STATE_DIR / "ledger.sealed"
CLOCK_PATH = STATE_DIR / "clock.json"
WORLD_PATH = STATE_DIR / "world.db"
RUN_PATH = STATE_DIR / "run.json"


class ConfigError(RuntimeError):
    pass


@dataclass(frozen=True)
class WorldCfg:
    scale: float
    horizon_sim_days: int


@dataclass(frozen=True)
class ClockCfg:
    sim_start: dt.datetime
    sim_days_per_real_hour: float


@dataclass(frozen=True)
class ApiCfg:
    host: str
    port: int
    hostility: int
    token: str


@dataclass(frozen=True)
class DropCfg:
    path: Path


@dataclass(frozen=True)
class FaultsCfg:
    n_sampled: int


@dataclass(frozen=True)
class Config:
    world: WorldCfg
    clock: ClockCfg
    api: ApiCfg
    drop: DropCfg
    faults: FaultsCfg

    @property
    def fingerprint(self) -> str:
        """Identity of the *world*, as opposed to the way it is served.

        Anything hashed here voids the ledger when it changes; anything not
        hashed here can be retuned between restarts.
        """
        material = {
            "scale": self.world.scale,
            "horizon_sim_days": self.world.horizon_sim_days,
            "sim_start": self.clock.sim_start.isoformat(),
            "n_sampled": self.faults.n_sampled,
        }
        blob = json.dumps(material, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(blob.encode()).hexdigest()[:16]

    @property
    def sim_end(self) -> dt.datetime:
        return self.clock.sim_start + dt.timedelta(days=self.world.horizon_sim_days)


def set_state_dir(path) -> None:
    """Re-point everything the lab persists.  Used by the lab's own tests."""
    global STATE_DIR, LEDGER_PATH, CLOCK_PATH, WORLD_PATH, RUN_PATH
    STATE_DIR = Path(path)
    LEDGER_PATH = STATE_DIR / "ledger.sealed"
    CLOCK_PATH = STATE_DIR / "clock.json"
    WORLD_PATH = STATE_DIR / "world.db"
    RUN_PATH = STATE_DIR / "run.json"
    STATE_DIR.mkdir(parents=True, exist_ok=True)


def _as_datetime(value: object, field: str) -> dt.datetime:
    if isinstance(value, dt.datetime):
        return value
    if isinstance(value, dt.date):
        return dt.datetime.combine(value, dt.time())
    if isinstance(value, str):
        return dt.datetime.fromisoformat(value)
    raise ConfigError(f"[clock].{field} must be a date or datetime, got {value!r}")


def load(path: Path | None = None) -> Config:
    path = path or CONFIG_PATH
    if not path.exists():
        raise ConfigError(f"missing config file: {path}")
    raw = tomllib.loads(path.read_text(encoding="utf-8"))

    world = raw.get("world", {})
    clock = raw.get("clock", {})
    api = raw.get("api", {})
    drop = raw.get("drop", {})
    faults = raw.get("faults", {})

    scale = float(world.get("scale", 1.0))
    if scale <= 0:
        raise ConfigError("[world].scale must be > 0")
    horizon = int(world.get("horizon_sim_days", 2200))
    if horizon < 1:
        raise ConfigError("[world].horizon_sim_days must be >= 1")

    hostility = int(api.get("hostility", 1))
    if hostility not in (0, 1, 2, 3):
        raise ConfigError("[api].hostility must be 0, 1, 2 or 3")

    token = str(api.get("token", "")).strip()
    if not token:
        raise ConfigError("[api].token must be a non-empty string")

    n_sampled = int(faults.get("n_sampled", 11))
    if not 8 <= n_sampled <= 14:
        raise ConfigError("[faults].n_sampled must be between 8 and 14")

    ratio = float(clock.get("sim_days_per_real_hour", 24.0))
    if ratio <= 0:
        raise ConfigError("[clock].sim_days_per_real_hour must be > 0")

    drop_path = Path(str(drop.get("path", "./drop")))
    if not drop_path.is_absolute():
        drop_path = (REPO_ROOT / drop_path).resolve()

    return Config(
        world=WorldCfg(scale=scale, horizon_sim_days=horizon),
        clock=ClockCfg(
            sim_start=_as_datetime(clock.get("sim_start", "2026-01-01"), "sim_start"),
            sim_days_per_real_hour=ratio,
        ),
        api=ApiCfg(
            host=str(api.get("host", "127.0.0.1")),
            port=int(api.get("port", 8088)),
            hostility=hostility,
            token=token,
        ),
        drop=DropCfg(path=drop_path),
        faults=FaultsCfg(n_sampled=n_sampled),
    )
