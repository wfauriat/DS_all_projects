"""Phase 2: simulated time."""

from __future__ import annotations

import datetime as dt
import time

from lab.clock import Clock


def _clock(lab, name="clock.json"):
    return Clock(lab.cfg, path=lab.state_dir / name)


def test_advances_at_the_configured_ratio(clean_lab):
    c = _clock(clean_lab, "c_ratio.json")
    a = c.now()
    time.sleep(1.0)
    b = c.now()
    days = (b - a).total_seconds() / 86400.0
    expected = clean_lab.cfg.clock.sim_days_per_real_hour / 3600.0
    assert 0.5 * expected < days < 2.0 * expected


def test_pause_and_resume(clean_lab):
    c = _clock(clean_lab, "c_pause.json")
    c.pause()
    a = c.now()
    time.sleep(0.4)
    assert c.now() == a
    assert c.describe()["paused"] is True
    c.resume()
    time.sleep(0.3)
    assert c.now() > a


def test_restart_resumes_rather_than_rewinds(clean_lab):
    c = _clock(clean_lab, "c_restart.json")
    time.sleep(0.3)
    before = c.now()
    again = Clock(clean_lab.cfg, path=clean_lab.state_dir / "c_restart.json")
    assert again.now() >= before


def test_ratio_change_does_not_rewind(clean_lab):
    c = _clock(clean_lab, "c_speed.json")
    time.sleep(0.3)
    before = c.now()
    c.set_ratio(240.0)
    assert c.now() >= before
    a = c.now()
    time.sleep(0.5)
    fast = (c.now() - a).total_seconds() / 86400.0
    assert fast > 240.0 * 0.3 / 3600.0


def test_advance_records_the_jump(clean_lab):
    c = _clock(clean_lab, "c_jump.json")
    before = c.now()
    d = c.advance(30)
    assert dt.datetime.fromisoformat(d["sim_now"]) >= before + dt.timedelta(days=29.9)
    assert len(d["manual_jumps"]) == 1
    assert d["manual_jumps"][0]["days"] > 29.9


def test_clock_stops_at_the_horizon(clean_lab):
    c = _clock(clean_lab, "c_end.json")
    c.advance(10_000)
    assert c.now() == clean_lab.cfg.sim_end
    assert c.describe()["at_horizon_end"] is True


def test_lab_endpoints_drive_the_clock(clean_lab, client_factory):
    st = clean_lab.state(hostility=3)
    st.clock = _clock(clean_lab, "c_http.json")
    c = client_factory(st)
    assert c.get("/_lab/clock").json()["paused"] is False
    assert c.post("/_lab/pause").json()["paused"] is True
    assert c.post("/_lab/resume").json()["paused"] is False
    assert c.post("/_lab/ratio", params={"sim_days_per_real_hour": 48}).json()[
        "sim_days_per_real_hour"] == 48
    before = dt.datetime.fromisoformat(c.get("/_lab/clock").json()["sim_now"])
    after = dt.datetime.fromisoformat(c.post("/_lab/advance", params={"days": 5}).json()["sim_now"])
    assert after >= before + dt.timedelta(days=4.9)
