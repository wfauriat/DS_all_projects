"""Phase 0: init and sealing.  Phase 7: the grader.

These drive the real CLI in a subprocess against a throwaway state directory,
so the ledger involved is a genuine sealed ledger, not a harness fixture.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import subprocess
import sys

import pytest

from lab.faults import schedule as sched

REPO = __import__("pathlib").Path(__file__).resolve().parent.parent

TOML = """
[world]
scale = 0.02
horizon_sim_days = 300
[clock]
sim_start = "2026-01-01"
sim_days_per_real_hour = 24.0
[api]
host = "127.0.0.1"
port = 8099
hostility = 1
token = "rl_test_0123456789abcdef"
[drop]
path = "{drop}"
[faults]
n_sampled = 9
"""


@pytest.fixture(scope="module")
def run(tmp_path_factory):
    base = tmp_path_factory.mktemp("init")
    state = base / "state"
    toml = base / "lab.toml"
    toml.write_text(TOML.format(drop=base / "drop"))
    env = dict(os.environ, REFUND_LAB_CONFIG=str(toml), REFUND_LAB_STATE=str(state))

    def cli(*args, expect=0):
        p = subprocess.run([sys.executable, *args], cwd=REPO, env=env,
                           capture_output=True, text=True)
        assert p.returncode == expect, p.stderr or p.stdout
        return p

    first = cli("lab.py", "init")
    return {"base": base, "state": state, "toml": toml, "env": env, "cli": cli,
            "stdout": first.stdout}


# ---------------------------------------------------------------------------
# phase 0
# ---------------------------------------------------------------------------


def test_init_prints_a_run_id_and_a_digest(run):
    out = run["stdout"]
    ledger = sched.read_ledger(run["state"] / "ledger.sealed")
    assert f"run-id            {ledger['run_id']}" in out
    assert sched.sealed_digest(run["state"] / "ledger.sealed") in out
    assert len(ledger["run_id"]) == 8


def test_the_seed_is_nowhere_in_plaintext(run):
    ledger = sched.read_ledger(run["state"] / "ledger.sealed")
    seed = ledger["seed"]
    assert seed and len(seed) == 32
    assert seed not in run["stdout"]
    for root in (REPO, run["base"]):
        for path in root.rglob("*"):
            if not path.is_file() or ".git/" in str(path) or path.suffix in (".db", ".pyc"):
                continue
            try:
                text = path.read_text(encoding="utf-8", errors="ignore")
            except OSError:
                continue
            assert seed not in text, path


def test_init_prints_no_fault_names(run):
    for fid in sched.CATALOG:
        assert fid not in run["stdout"]


def test_reinit_refuses_without_force(run):
    p = run["cli"]("lab.py", "init", expect=2)
    assert "refusing to re-initialise" in p.stderr
    ledger = sched.read_ledger(run["state"] / "ledger.sealed")
    assert ledger["run_id"] == json.loads((run["state"] / "run.json").read_text())["run_id"]


def test_ledger_holds_the_required_shape(run):
    ledger = sched.read_ledger(run["state"] / "ledger.sealed")
    assert len(ledger["faults"]) == 9
    ids = {f["id"] for f in ledger["faults"]}
    assert {"label_censoring", "backfill_restatement"} <= ids
    for f in ledger["faults"]:
        assert {"id", "family", "group", "sim_start", "sim_end", "params",
                "affected_keys"} <= set(f)
        assert f["sim_start"] <= f["sim_end"]


def test_sealed_blob_is_not_readable_by_eye(run):
    raw = (run["state"] / "ledger.sealed").read_text()
    assert "do not decode" in raw.lower()
    for fid in sched.CATALOG:
        assert fid not in raw


# ---------------------------------------------------------------------------
# phase 7
# ---------------------------------------------------------------------------


def _log(path, records):
    path.write_text("".join(json.dumps(r) + "\n" for r in records), encoding="utf-8")
    return path


def _perfect(ledger, offset_days=2.0):
    out = []
    for f in ledger["faults"]:
        t = dt.datetime.fromisoformat(f["sim_start"]) + dt.timedelta(days=offset_days)
        out.append({
            "detected_at_sim": t.isoformat(),
            "detected_at_real": dt.datetime(2026, 6, 1).isoformat(),
            "symptom": "row counts moved",
            "suspected_family": f["family"],
            "suspected_fault": f["id"],
            "confidence": "high",
            "resolution": "reprocessed",
        })
    return out


def grade(run, path, *extra):
    return run["cli"]("grade.py", "--incidents", str(path), *extra).stdout


def test_perfect_log_scores_everything(run, tmp_path):
    ledger = sched.read_ledger(run["state"] / "ledger.sealed")
    out = grade(run, _log(tmp_path / "perfect.jsonl", _perfect(ledger)))
    n = len(ledger["faults"])
    assert f"detected              {n} of {n}   (100%)" in out
    assert f"named correctly     {n}" in out
    assert "false positives       0" in out
    assert "MTTD (simulated)      2.0 days" in out


def test_empty_log_scores_nothing(run, tmp_path):
    ledger = sched.read_ledger(run["state"] / "ledger.sealed")
    out = grade(run, _log(tmp_path / "empty.jsonl", []))
    assert f"detected              0 of {len(ledger['faults'])}   (0%)" in out
    assert "false positives       0" in out


def test_one_spurious_entry_is_one_false_positive(run, tmp_path):
    ledger = sched.read_ledger(run["state"] / "ledger.sealed")
    recs = _perfect(ledger)
    recs.append({
        "detected_at_sim": (dt.datetime.fromisoformat(ledger["sim_end"])
                            - dt.timedelta(days=1)).isoformat(),
        "symptom": "imagined it",
        "suspected_family": "ingestion",
        "suspected_fault": "dup_exact_payments",
        "confidence": "low",
    })
    out = grade(run, _log(tmp_path / "spurious.jsonl", recs))
    assert "false positives       1" in out


def test_family_only_claims_get_partial_credit(run, tmp_path):
    ledger = sched.read_ledger(run["state"] / "ledger.sealed")
    recs = []
    for f in ledger["faults"]:
        t = dt.datetime.fromisoformat(f["sim_start"]) + dt.timedelta(days=1)
        recs.append({"detected_at_sim": t.isoformat(), "symptom": "something broke",
                     "suspected_family": f["group"]})
    out = grade(run, _log(tmp_path / "family.jsonl", recs))
    n = len(ledger["faults"])
    assert f"detected              {n} of {n}" in out
    assert f"family only         {n}" in out
    assert "named correctly     0" in out


def test_summary_names_nothing_the_operator_did_not(run, tmp_path):
    ledger = sched.read_ledger(run["state"] / "ledger.sealed")
    named = {ledger["faults"][0]["id"]}
    recs = [{
        "detected_at_sim": (dt.datetime.fromisoformat(ledger["faults"][0]["sim_start"])
                            + dt.timedelta(days=1)).isoformat(),
        "symptom": "noticed",
        "suspected_fault": ledger["faults"][0]["id"],
    }]
    out = grade(run, _log(tmp_path / "partial.jsonl", recs), "--summary")
    for f in ledger["faults"]:
        if f["id"] not in named:
            assert f["id"] not in out, f["id"]
    assert ledger["faults"][0]["id"] not in out or True   # naming your own claim is fine


def test_reveal_unseals_and_is_recorded(run, tmp_path):
    ledger = sched.read_ledger(run["state"] / "ledger.sealed")
    out = grade(run, _log(tmp_path / "empty2.jsonl", []), "--reveal", "--yes")
    for f in ledger["faults"]:
        assert f["id"] in out
    assert out.count("[MISSED]") == len(ledger["faults"])
    assert (run["state"] / "revealed").exists()
    after = grade(run, _log(tmp_path / "empty3.jsonl", []))
    assert "already unsealed" in after


def test_service_refuses_a_world_that_no_longer_matches_the_config(run):
    """Retuning the world voids the run; retuning delivery does not."""
    toml = run["toml"]
    original = toml.read_text()

    toml.write_text(original.replace("hostility = 1", "hostility = 3")
                            .replace("sim_days_per_real_hour = 24.0",
                                     "sim_days_per_real_hour = 96.0"))
    from lab import api as apimod
    from lab import config as cfgmod

    cfgmod.set_state_dir(run["state"])
    apimod.verify_run(cfgmod.load(toml))          # delivery knobs: fine

    toml.write_text(original.replace("scale = 0.02", "scale = 0.5"))
    with pytest.raises(apimod.RunMismatch):
        apimod.verify_run(cfgmod.load(toml))

    toml.write_text(original)


# ---------------------------------------------------------------------------
# grading policy, on a synthetic ledger so the assertions do not depend on
# which faults this run happened to draw
# ---------------------------------------------------------------------------


def _synthetic_ledger(faults):
    return {"run_id": "deadbeef", "sim_start": "2026-01-01T00:00:00",
            "sim_end": "2032-01-10T00:00:00", "faults": faults}


def _fault(fid, start, end):
    from lab.faults.catalog import CATALOG

    fam, grp, _desc = CATALOG[fid]
    return {"id": fid, "family": fam, "group": grp,
            "sim_start": start, "sim_end": end, "params": {}, "affected_keys": []}


@pytest.mark.parametrize("lag_days,expect", [(0.5, 1), (7, 1), (59, 1), (61, 0)])
def test_symptom_credit_does_not_depend_on_how_long_the_fault_lasted(lag_days, expect):
    """A six-hour outage is not detectable in six hours; the window is the policy."""
    import grade

    led = _synthetic_ledger([
        _fault("sustained_outage", "2027-05-01T00:00:00", "2027-05-01T06:00:00"),
    ])
    t = dt.datetime(2027, 5, 1) + dt.timedelta(days=lag_days)
    inc, bad = grade.load_incidents_from([
        {"detected_at_sim": t.isoformat(), "symptom": "api 503s",
         "suspected_family": "operational"}
    ])
    res = grade.score(led, inc, bad)
    assert len(res.matches) == expect
    assert len(res.false_positives) == 1 - expect


def test_naming_the_fault_is_credited_however_late():
    import grade

    led = _synthetic_ledger([
        _fault("sustained_outage", "2027-05-01T00:00:00", "2027-05-01T06:00:00"),
    ])
    inc, bad = grade.load_incidents_from([
        {"detected_at_sim": "2029-01-01T00:00:00", "symptom": "found it eventually",
         "suspected_fault": "sustained_outage"}
    ])
    res = grade.score(led, inc, bad)
    assert len(res.matches) == 1 and res.matches[0].exact
    assert res.matches[0].lag_sim_days > 600


def test_a_claim_before_the_fault_fired_is_a_false_positive():
    import grade

    led = _synthetic_ledger([
        _fault("missing_delivery", "2027-05-01T00:00:00", "2027-05-02T00:00:00"),
    ])
    inc, bad = grade.load_incidents_from([
        {"detected_at_sim": "2027-04-01T00:00:00", "symptom": "premonition",
         "suspected_fault": "missing_delivery"}
    ])
    res = grade.score(led, inc, bad)
    assert not res.matches and len(res.false_positives) == 1


def test_an_always_on_fault_stays_creditable_for_the_whole_run():
    """A defect that is still happening is still fresh."""
    import grade

    led = _synthetic_ledger([
        _fault("label_censoring", "2026-01-01T00:00:00", "2032-01-10T00:00:00"),
    ])
    for when in ("2026-01-20T00:00:00", "2028-06-01T00:00:00", "2031-11-01T00:00:00"):
        inc, bad = grade.load_incidents_from([
            {"detected_at_sim": when, "symptom": "orders near the edge have no label row",
             "suspected_family": "statistical"}
        ])
        res = grade.score(led, inc, bad)
        assert len(res.matches) == 1, when
        assert not res.matches[0].exact
    # ...but still not before it started
    inc, bad = grade.load_incidents_from([
        {"detected_at_sim": "2025-06-01T00:00:00", "symptom": "x",
         "suspected_family": "statistical"}
    ])
    assert not grade.score(led, inc, bad).matches
