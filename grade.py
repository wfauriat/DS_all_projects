#!/usr/bin/env python3
"""Score an incident log against the sealed ledger.

    python grade.py --incidents ../refund-lab-pipeline/incidents.jsonl --summary
    python grade.py --incidents ../refund-lab-pipeline/incidents.jsonl --reveal

``--summary`` is safe to run as often as you like: it reports counts, and it
names no fault that your own log did not already name.

``--reveal`` unseals everything -- every fault drawn for this run, when it fired
and with what parameters, matched and unmatched.  It is a one-way door.  Once
you have read it you cannot un-know it, and the run is over as an exercise.

Incident log format, one JSON object per line:

    {"detected_at_sim": "2027-03-14T08:00:00",
     "detected_at_real": "2026-11-02T14:20:00",
     "symptom": "payments row count for 2027-03-12 dropped 40%",
     "suspected_family": "operational",
     "suspected_fault": "missing_delivery",
     "confidence": "medium",
     "resolution": "reprocessed from API"}

Only ``detected_at_sim`` and one of ``suspected_family`` / ``suspected_fault``
are required.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import statistics
import sys
from dataclasses import dataclass, field
from pathlib import Path

from lab import config as cfgmod
from lab.faults.catalog import CATALOG
from lab.faults import schedule as sched

# How long after a fault starts a symptom-based (family-only) claim still counts.
# Naming the fault outright is credited whenever it was noticed.
DETECTION_WINDOW_DAYS = 60
EARLY_TOLERANCE_DAYS = 1.0


@dataclass
class Incident:
    line_no: int
    detected_at_sim: dt.datetime
    detected_at_real: dt.datetime | None
    symptom: str
    suspected_family: str | None
    suspected_fault: str | None
    confidence: str | None
    resolution: str | None
    raw: dict


@dataclass
class Match:
    fault_id: str
    incident: Incident
    exact: bool
    lag_sim_days: float


@dataclass
class Result:
    total: int
    matches: list[Match] = field(default_factory=list)
    unmatched_faults: list[dict] = field(default_factory=list)
    false_positives: list[Incident] = field(default_factory=list)
    malformed: list[tuple[int, str]] = field(default_factory=list)


def _dt(v):
    return dt.datetime.fromisoformat(str(v).replace("Z", "+00:00")).replace(tzinfo=None)


def load_incidents(path: Path) -> tuple[list[Incident], list[tuple[int, str]]]:
    if not path.exists():
        raise SystemExit(f"no incident log at {path}")
    lines = path.read_text(encoding="utf-8").splitlines()
    return load_incidents_from(lines)


def load_incidents_from(lines) -> tuple[list[Incident], list[tuple[int, str]]]:
    """``lines`` may be raw JSON lines or already-parsed dicts."""
    incidents: list[Incident] = []
    bad: list[tuple[int, str]] = []
    for i, line in enumerate(lines, start=1):
        if isinstance(line, str):
            line = line.strip()
            if not line or line.startswith("#"):
                continue
        try:
            rec = json.loads(line) if isinstance(line, str) else line
            inc = Incident(
                line_no=i,
                detected_at_sim=_dt(rec["detected_at_sim"]),
                detected_at_real=_dt(rec["detected_at_real"]) if rec.get("detected_at_real") else None,
                symptom=str(rec.get("symptom", "")),
                suspected_family=rec.get("suspected_family"),
                suspected_fault=rec.get("suspected_fault"),
                confidence=rec.get("confidence"),
                resolution=rec.get("resolution"),
                raw=rec,
            )
            if not inc.suspected_family and not inc.suspected_fault:
                raise ValueError("needs suspected_family or suspected_fault")
            incidents.append(inc)
        except Exception as exc:
            bad.append((i, str(exc)))
    return incidents, bad


def _window(fault: dict) -> tuple[dt.datetime, dt.datetime]:
    """How long a symptom-based claim stays creditable.

    Deliberately independent of how long the effect itself lasted. A six-hour
    outage is not detectable within six hours by a consumer that reads once a
    simulated day, and a one-day gap in a feed is often only noticed at the next
    weekly reconciliation. Naming the fault outright is credited whenever it was
    noticed; this window only bounds the partial credit.
    """
    start = _dt(fault["sim_start"])
    return start, start + dt.timedelta(days=DETECTION_WINDOW_DAYS)


def _family_ok(inc: Incident, fault: dict) -> bool:
    claimed = {c for c in (inc.suspected_family,) if c}
    if inc.suspected_fault in CATALOG:
        claimed |= {CATALOG[inc.suspected_fault][0], CATALOG[inc.suspected_fault][1]}
    return bool(claimed & {fault["family"], fault["group"]})


def score(ledger: dict, incidents: list[Incident], malformed) -> Result:
    faults = ledger["faults"]
    res = Result(total=len(faults), malformed=list(malformed))
    taken: set[int] = set()
    matched_ids: set[str] = set()

    ordered = sorted(incidents, key=lambda i: i.detected_at_sim)
    # exact fault-name claims first: they are the stronger evidence
    for exact_pass in (True, False):
        for fault in faults:
            if fault["id"] in matched_ids:
                continue
            start, end = _window(fault)
            for inc in ordered:
                if id(inc) in taken:
                    continue
                if inc.detected_at_sim < start - dt.timedelta(days=EARLY_TOLERANCE_DAYS):
                    continue
                if exact_pass:
                    ok = inc.suspected_fault == fault["id"]
                else:
                    ok = inc.detected_at_sim <= end and _family_ok(inc, fault)
                if not ok:
                    continue
                taken.add(id(inc))
                matched_ids.add(fault["id"])
                res.matches.append(
                    Match(
                        fault_id=fault["id"], incident=inc, exact=exact_pass,
                        lag_sim_days=round(
                            (inc.detected_at_sim - start).total_seconds() / 86400.0, 3
                        ),
                    )
                )
                break

    res.false_positives = [i for i in ordered if id(i) not in taken]
    res.unmatched_faults = [f for f in faults if f["id"] not in matched_ids]
    return res


def _claimed_families(incidents: list[Incident]) -> set[str]:
    out: set[str] = set()
    for inc in incidents:
        if inc.suspected_family:
            out.add(inc.suspected_family)
        if inc.suspected_fault in CATALOG:
            out.add(CATALOG[inc.suspected_fault][0])
            out.add(CATALOG[inc.suspected_fault][1])
    return out


def _elapsed(ledger: dict) -> str:
    if not cfgmod.CLOCK_PATH.exists():
        return "clock state not found"
    st = json.loads(cfgmod.CLOCK_PATH.read_text())
    start = _dt(ledger["sim_start"])
    now = _dt(st["anchor_sim"])
    horizon = (_dt(ledger["sim_end"]) - start).days
    jumped = sum(j.get("days", 0) for j in st.get("jumps", []))
    line = (
        f"{(now - start).days} of {horizon} simulated days elapsed"
        f" ({100.0 * (now - start).days / max(horizon, 1):.0f}%)"
    )
    if jumped:
        line += f"; {jumped:.0f} of them skipped by /_lab/advance"
    return line


def print_summary(ledger: dict, res: Result, incidents: list[Incident]) -> None:
    n = len(res.matches)
    exact = sum(1 for m in res.matches if m.exact)
    print(f"run-id                {ledger['run_id']}")
    print(f"sealed ledger sha256  {sched.sealed_digest(cfgmod.LEDGER_PATH)}")
    print(f"horizon               {_elapsed(ledger)}")
    print()
    print(f"detected              {n} of {res.total}"
          f"   ({100.0 * n / max(res.total, 1):.0f}%)")
    print(f"  named correctly     {exact}")
    print(f"  family only         {n - exact}")
    print(f"false positives       {len(res.false_positives)}")
    print(f"incidents logged      {len(incidents)}")
    if res.malformed:
        print(f"unreadable lines      {len(res.malformed)}"
              f"  (first: line {res.malformed[0][0]})")
    if res.matches:
        lags = [m.lag_sim_days for m in res.matches]
        print()
        print(f"MTTD (simulated)      {statistics.mean(lags):.1f} days"
              f"   median {statistics.median(lags):.1f}"
              f"   worst {max(lags):.1f}")

    claimed = _claimed_families(incidents)
    if claimed:
        print()
        print("per-family, restricted to families you have yourself claimed:")
        for fam in sorted(claimed):
            hits = [m for m in res.matches
                    if fam in (CATALOG[m.fault_id][0], CATALOG[m.fault_id][1])]
            tries = [i for i in incidents
                     if i.suspected_family == fam
                     or (i.suspected_fault in CATALOG
                         and fam in CATALOG[i.suspected_fault][:2])]
            print(f"  {fam:<14} {len(hits)} confirmed from {len(tries)} claim(s)")
    print()
    print("This summary names nothing you did not name first, and says nothing")
    print("about what is still out there. Run with --reveal when you are done.")


def print_reveal(ledger: dict, res: Result) -> None:
    matched = {m.fault_id: m for m in res.matches}
    print(f"run-id                {ledger['run_id']}")
    print(f"created               {ledger['created_real']}")
    print(f"horizon               {ledger['sim_start']} .. {ledger['sim_end']}")
    print(f"                      {_elapsed(ledger)}")
    print(f"faults drawn          {len(ledger['faults'])}")
    print()
    for f in ledger["faults"]:
        m = matched.get(f["id"])
        mark = "FOUND " if m else "MISSED"
        print(f"[{mark}] {f['id']}  ({f['family']}/{f['group']})")
        print(f"         {CATALOG[f['id']][2]}")
        print(f"         fired {f['sim_start']} -> {f['sim_end']}")
        if f.get("n_affected"):
            print(f"         rows affected: {f['n_affected']:,}")
        if f.get("params"):
            print(f"         params: {json.dumps(f['params'], ensure_ascii=False)[:400]}")
        if m:
            print(f"         detected {m.incident.detected_at_sim.isoformat()}"
                  f"  (+{m.lag_sim_days:.1f} sim days,"
                  f" {'named' if m.exact else 'family only'})")
            if m.incident.symptom:
                print(f"         you wrote: {m.incident.symptom[:160]}")
        if f.get("affected_keys"):
            keys = f["affected_keys"]
            print(f"         keys: {', '.join(map(str, keys[:8]))}"
                  f"{' ...' if len(keys) > 8 else ''}")
        print()
    if res.false_positives:
        print(f"false positives ({len(res.false_positives)}):")
        for inc in res.false_positives:
            print(f"  line {inc.line_no}  {inc.detected_at_sim.isoformat()}"
                  f"  {inc.suspected_fault or inc.suspected_family}"
                  f"  {inc.symptom[:80]}")
        print()
    print("Ambient hostility (rate limits, transient errors, pagination quirks) is")
    print("not in the ledger and is not scored. It was always on.")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--incidents", required=True, type=Path)
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--summary", action="store_true", help="counts only (default)")
    mode.add_argument("--reveal", action="store_true", help="unseal everything; one-way")
    ap.add_argument("--yes", action="store_true", help="skip the --reveal confirmation")
    args = ap.parse_args(argv)

    ledger = sched.read_ledger(cfgmod.LEDGER_PATH)
    incidents, malformed = load_incidents(args.incidents)
    res = score(ledger, incidents, malformed)

    if args.reveal:
        marker = cfgmod.STATE_DIR / "revealed"
        if not args.yes and sys.stdin.isatty():
            print("--reveal unseals the whole run. There is no way back.")
            if input('Type "REVEAL" to continue: ').strip() != "REVEAL":
                print("aborted; nothing was unsealed")
                return 1
        marker.write_text(
            json.dumps({"at": dt.datetime.now().replace(microsecond=0).isoformat(),
                        "run_id": ledger["run_id"]}) + "\n",
            encoding="utf-8",
        )
        print_reveal(ledger, res)
        return 0

    marker = cfgmod.STATE_DIR / "revealed"
    if marker.exists():
        print(f"note: this run was already unsealed ({json.loads(marker.read_text())['at']})")
        print()
    print_summary(ledger, res, incidents)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
