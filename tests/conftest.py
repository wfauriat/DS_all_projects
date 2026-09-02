"""Shared fixtures for the lab's own test suite.

OPERATOR: this directory is lab internals, like ``lab/faults/``.  The suite
force-fires faults and asserts their signatures; reading it tells you what the
signatures are, which is the part you are meant to work out from the data.
"""

from __future__ import annotations

import datetime as dt
import os
import shutil
from dataclasses import dataclass
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent

SMALL_TOML = """
[world]
scale = {scale}
horizon_sim_days = {horizon}
[clock]
sim_start = "{sim_start}"
sim_days_per_real_hour = {ratio}
[api]
host = "127.0.0.1"
port = 8099
hostility = {hostility}
token = "rl_test_0123456789abcdef"
[drop]
path = "{drop}"
[faults]
n_sampled = {n_sampled}
"""


@dataclass
class Lab:
    name: str
    cfg: object
    state_dir: Path
    drop_dir: Path
    plan: list

    def activate(self):
        from lab import config as cfgmod

        cfgmod.set_state_dir(self.state_dir)
        return self

    def state(self, hostility: int = 0, effects=None, run_drop: bool = False):
        from lab import api as apimod
        from lab.clock import Clock
        from lab.faults.effects import Effects
        from lab.world import Store

        self.activate()
        return apimod.build_state(
            cfg=self.cfg,
            store=Store(self.state_dir / "world.db"),
            clock=Clock(self.cfg, path=self.state_dir / "clock.json"),
            effects=effects if effects is not None else Effects(self.plan),
            hostility=hostility,
        )


class LabFactory:
    def __init__(self, root: Path):
        self.root = root
        self._cache: dict[str, Lab] = {}

    def make(
        self,
        name: str,
        fault_ids: list[str] | None = None,
        times: dict | None = None,
        scale: float = 0.08,
        horizon: int = 500,
        sim_start: str = "2026-01-01",
        ratio: float = 24.0,
        seed: bytes | None = None,
    ) -> Lab:
        if name in self._cache:
            return self._cache[name].activate()

        from lab import config as cfgmod
        from lab.faults import harness

        base = self.root / name
        if base.exists():
            shutil.rmtree(base)
        state_dir = base / "state"
        drop_dir = base / "drop"
        state_dir.mkdir(parents=True)
        toml = base / "lab.toml"
        toml.write_text(
            SMALL_TOML.format(
                scale=scale, horizon=horizon, sim_start=sim_start, ratio=ratio,
                hostility=0, drop=drop_dir, n_sampled=11,
            )
        )
        cfg = cfgmod.load(toml)
        cfgmod.set_state_dir(state_dir)
        kw = {} if seed is None else {"seed": seed}
        plan, _stats = harness.build(cfg, list(fault_ids or []), times, **kw)
        lab = Lab(name=name, cfg=cfg, state_dir=state_dir, drop_dir=drop_dir, plan=plan)
        self._cache[name] = lab
        return lab


@pytest.fixture(scope="session")
def labs(tmp_path_factory) -> LabFactory:
    return LabFactory(tmp_path_factory.mktemp("labs"))


@pytest.fixture(scope="session")
def clean_lab(labs) -> Lab:
    """A world with no sampled faults at all."""
    return labs.make("clean")


@pytest.fixture
def client_factory():
    from fastapi.testclient import TestClient

    from lab import api as apimod

    def make(state, run_drop: bool = False):
        app = apimod.create_app(state=state, run_drop=run_drop)
        return TestClient(app)

    return make


def token_for(state) -> str:
    from lab.api import issue_token

    tok, _ = issue_token(state.cfg.api.token, state.clock.now(), 24 * 365 * 10)
    return tok
