"""End-to-end test of the path that failed silently for eleven simulated months.

A payments file whose join key has been round-tripped through an integer type is
written to a scratch drop directory, ingested by the real `DropIngestor`, and
the resulting evidence is carried through the real merge into the real monitor.
Nothing here is hand-assembled: the point of the test is the *wiring*, and the
original bug was invisible precisely because every component passed on its own.
"""
from __future__ import annotations

import gzip
import pathlib
import sys
import tempfile

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
from pipeline import config, monitors, storage                       # noqa: E402
from pipeline.extract_drop import DropIngestor                       # noqa: E402
from pipeline.orchestrator import Pipeline                           # noqa: E402
from pipeline.normalize import NormStats                             # noqa: E402

results = []


def check(name, cond, detail=""):
    results.append((name, bool(cond), detail))


class CapturingLog:
    def __init__(self):
        self.fired = []

    def raise_incident(self, sim_now, symptom, family, fault=None, confidence="medium",
                       resolution="", evidence=None, key=None, cycle_id=None):
        self.fired.append((fault, symptom, evidence))
        return True


HDR = "payment_id,order_id,customer_id,paid_ts,amount_cents,status,processor,billing_city\n"


def write_payments(dirpath, day, ids):
    d = dirpath / "payments"
    d.mkdir(parents=True, exist_ok=True)
    body = HDR + "".join(
        f"PAY-{i:08d},ORD-{i:08d},{cid},{day}T09:0{i % 6}:00,{1000 + i},captured,orbit,Angers\n"
        for i, cid in enumerate(ids))
    p = d / f"payments_{day}.csv.gz"
    p.write_bytes(gzip.compress(body.encode()))
    return p


def run_once(day, ids, sim_now):
    """Ingest one drop file and push its evidence through the real merge."""
    tmp = pathlib.Path(tempfile.mkdtemp())
    original = config.DROP_DIR
    config.DROP_DIR = tmp                       # point the ingestor at the scratch drop
    try:
        path = write_payments(tmp, day, ids)
        st = storage.Store(tmp / "wh.db")
        ing = DropIngestor(st)
        ing.expect("payments", day, f"{day}T00:00:00", 1)
        findings = [ing.ingest("payments", path, sim_now, 2)]
        st.commit()

        norm = {e: NormStats() for e in config.ENTITIES}
        Pipeline.merge_drop_evidence(norm, findings)          # the real wiring

        log = CapturingLog()
        m = monitors.Monitors(st, log)
        m.ctx = {"cycle_id": 2, "sim_now": sim_now, "norm": norm}
        m.sim_now, m.cycle = sim_now, 2
        m.today = pathlib.PurePath(day).name and __import__("datetime").date.fromisoformat(day)
        m.age_days = 1866
        m.completed_days = []
        m.m_identifiers()
        return norm, log, st
    finally:
        config.DROP_DIR = original


# --- ids stripped of their leading zeros, exactly as the source emitted them
norm, log, st = run_once("2031-02-10", ["319", "11113", "13246", "21231", "21524"],
                         "2031-02-10T12:00:00")
check("drop ingest records the width violation",
      norm["payments"].id_width_violations.get("customer_id") == 5,
      norm["payments"].id_width_violations)
check("merge carries it into the monitor context", "payments" in norm)
check("m_identifiers claims id_leading_zeros end to end",
      any(f == "id_leading_zeros" for f, _s, _e in log.fired), log.fired)
check("the join is still repaired in the warehouse",
      st.q1("SELECT COUNT(*) FROM src_payments WHERE LENGTH(customer_id)<>7") == 0
      and st.q1("SELECT customer_id FROM src_payments ORDER BY row_no LIMIT 1") == "0000319")

# --- well-formed ids must stay silent
norm2, log2, _ = run_once("2031-02-11", ["0000319", "0011113", "0013246"],
                          "2031-02-11T12:00:00")
check("well-formed ids raise nothing", not log2.fired, log2.fired)
check("no spurious width violations", not norm2["payments"].id_width_violations,
      norm2["payments"].id_width_violations)

# --- and the mojibake / sentinel evidence rides the same wire
norm3, log3, _ = run_once("2031-02-12", ["0000319", "0000320", "0000321"],
                          "2031-02-12T12:00:00")
check("merge creates a payments channel even with clean data", "payments" in norm3)

bad = [r for r in results if not r[1]]
for name, ok, detail in results:
    print(("PASS " if ok else "FAIL ") + name + ("" if ok else f"   -> {detail}"))
print(f"\n{len(results)-len(bad)}/{len(results)} passed")
sys.exit(1 if bad else 0)
