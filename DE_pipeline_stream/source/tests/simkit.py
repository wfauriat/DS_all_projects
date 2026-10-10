"""Test helpers: a small world built from the real config/source.toml.

The tests drive time themselves. A fake wall clock never moves on its own, so
run_for() advances the simulated clock and lets the engine catch up
synchronously, with no asyncio and no sleeping.

(This is a plain module rather than conftest.py so that tests can import it:
pytest runs in importlib mode, and pyproject.toml puts this folder on `pythonpath`.)
"""

import json
from datetime import timedelta
from pathlib import Path

from bikeshare_sim.config import Settings, load_settings
from bikeshare_sim.simulation import Simulation

CONFIG = Path(__file__).parents[2] / "config" / "source.toml"


class FakeWall:
    def __init__(self) -> None:
        self.t = 0.0

    def __call__(self) -> float:
        return self.t


def make_settings(tmp_path: Path, *, faults_on: bool = False, **world) -> Settings:
    """The real config file with a smaller world, and all faults off unless asked."""
    s = load_settings(CONFIG, env={"SIM_STATE_DIR": str(tmp_path)})
    s.world = s.world.model_copy(update={"n_stations": 12, "n_bikes": 150, **world})
    if not faults_on:
        for name, cfg in s.faults.items():
            s.faults[name] = cfg.model_copy(update={"enabled": False})
    return s


def make_sim(settings: Settings) -> Simulation:
    return Simulation(settings, wall=FakeWall())


def run_for(sim: Simulation, **delta) -> None:
    sim.clock.advance(timedelta(**delta))
    while sim.catch_up(max_steps=10_000):
        pass


def emitted(sim: Simulation) -> list[dict]:
    records, _ = sim.log.read_after(0, 10**9)
    return [json.loads(data) for _, _, data in records]
