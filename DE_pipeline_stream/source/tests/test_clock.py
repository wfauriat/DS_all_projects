from datetime import UTC, datetime, timedelta

import pytest

from bikeshare_sim.clock import SimClock

from simkit import FakeWall

START = datetime(2026, 3, 2, 5, tzinfo=UTC)


def test_speed_maps_wall_seconds_to_sim_seconds():
    wall = FakeWall()
    clock = SimClock(START, speed=60, wall=wall)
    wall.t = 10  # 10 real seconds at 60x
    assert clock.now() == START + timedelta(minutes=10)


def test_speed_change_keeps_time_continuous():
    wall = FakeWall()
    clock = SimClock(START, speed=60, wall=wall)
    wall.t = 10
    clock.set_speed(120)  # re-anchors at START + 10 min
    wall.t = 20
    assert clock.now() == START + timedelta(minutes=10 + 20)


def test_pause_freezes_and_resume_continues():
    wall = FakeWall()
    clock = SimClock(START, speed=60, wall=wall)
    wall.t = 10
    clock.pause()
    wall.t = 1000
    assert clock.now() == START + timedelta(minutes=10)
    clock.resume()
    wall.t = 1001
    assert clock.now() == START + timedelta(minutes=11)


def test_advance_jumps_forward_only():
    clock = SimClock(START, speed=60, wall=FakeWall())
    clock.advance(timedelta(hours=6))
    assert clock.now() == START + timedelta(hours=6)
    with pytest.raises(ValueError):
        clock.advance(timedelta(0))


def test_naive_start_is_rejected():
    with pytest.raises(ValueError):
        SimClock(datetime(2026, 3, 2), speed=1)  # noqa: DTZ001 (naive on purpose)
