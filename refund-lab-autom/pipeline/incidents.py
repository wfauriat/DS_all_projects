"""The incident log -- the deliverable.

Written *during* the run, at the moment a monitor fires, never reconstructed
afterwards.  ``detected_at_sim`` is the simulated clock as it stood when the
detector tripped, so mean-time-to-detect means what it says.

Deduplication policy: one line per named fault, one line per unnamed
(family, symptom-class) pair.  A detector that keeps firing updates a repeat
counter in the warehouse but does not append again -- a fault reported five
times is not five detections, and repeated claims can only add false positives.
"""
from __future__ import annotations

import datetime as dt
import json

from . import config, util

log = util.get_logger("incidents")


class IncidentLog:
    def __init__(self, store, path=None):
        self.st = store
        self.path = path or config.INCIDENTS_PATH
        self.path.touch(exist_ok=True)

    def already(self, key) -> bool:
        return self.st.q1("SELECT 1 FROM incident_log WHERE dedupe_key=?", (key,)) is not None

    def raise_incident(self, sim_now, symptom, family, fault=None, confidence="medium",
                       resolution="", evidence=None, key=None, cycle_id=None):
        """Append one incident, unless an equivalent one is already on file."""
        dedupe = key or fault or f"family:{family}"
        if self.already(dedupe):
            self.st.ex("UPDATE incident_log SET n_repeats=n_repeats+1 WHERE dedupe_key=?",
                       (dedupe,))
            return False
        rec = {
            "detected_at_sim": sim_now,
            "detected_at_real": dt.datetime.now().replace(microsecond=0).isoformat(),
            "symptom": symptom,
            "suspected_family": family,
            "confidence": confidence,
            "resolution": resolution or "logged by monitor; pipeline continued",
        }
        if fault:
            rec["suspected_fault"] = fault
        if evidence:
            rec["evidence"] = evidence
        with open(self.path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(rec, ensure_ascii=False, default=str) + "\n")
            fh.flush()
        self.st.ex("INSERT OR IGNORE INTO incident_log(dedupe_key,suspected_fault,"
                   "suspected_family,detected_at_sim) VALUES(?,?,?,?)",
                   (dedupe, fault, family, sim_now))
        self.st.alert(cycle_id, sim_now, f"INCIDENT:{fault or family}", symptom, "incident")
        self.st.commit()
        log.warning("INCIDENT %s | %s | %s", sim_now, fault or family, symptom)
        return True

    def count(self):
        return self.st.q1("SELECT COUNT(*) FROM incident_log") or 0
