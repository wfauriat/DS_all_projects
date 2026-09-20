"""Fixtures for the source tests. The helpers themselves live in simkit.py."""

from pathlib import Path

import pytest

from bikeshare_sim.config import Settings

from simkit import make_settings


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return make_settings(tmp_path)
