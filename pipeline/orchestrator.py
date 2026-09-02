"""The scheduled loop.

One cycle == one pipeline increment == one simulated day (by default).  Each
cycle is idempotent and crash-safe: everything it writes is either an
``INSERT OR IGNORE`` into an append-only table or a version-guarded upsert, so
re-running a cycle after a failure cannot duplicate or lose a row.

    advance the world  ->  wait for the drop  ->  ingest both channels
      ->  reconcile against the source's own counts  ->  audit history
      ->  rebuild the changed partitions  ->  test them  ->  run the monitors
      ->  retrain / score  ->  checkpoint

The monitors run inside the loop rather than after it, and write
``incidents.jsonl`` as they fire.
"""
from __future__ import annotations

import datetime as dt
import json
import random
import threading
import time
from collections import Counter, defaultdict

from . import config, extract_api, extract_drop, incidents, ml, monitors, storage, transform, util
from .normalize import NormStats
from .vendor import VendorClient, VendorError

log = util.get_logger("orchestrator")

DOMAIN_SOURCES = {
    "orders": [("channel", "channel"), ("payment_method", "payment_method"),
               ("shipping_speed", "shipping_speed")],
    "customers": [("segment", "segment"), ("country", "country")],
    "events": [("event_type", "event_type")],
}


class Pipeline:
    def __init__(self, step_days=None):
        self.st = storage.Store()
        self.cl = VendorClient()
        self.ex = extract_api.ApiExtractor(self.st, self.cl)
        self.drop = extract_drop.DropIngestor(self.st)
        self.tf = transform.Transformer(self.st)
        self.inc = incidents.IncidentLog(self.st)
        self.mon = monitors.Monitors(self.st, self.inc)
        self.model = ml.RiskModel(self.st, self.inc)
        self.step = step_days or config.STEP_DAYS
        self.cycle_id = self.st.get_state("cycle_id", 0)
        self.probe_budget = 0
        self._retry_pending = False
        self.outage_hits = 0

    # ------------------------------------------------------------ clock
    def clock(self):
        c = self.cl.clock()
        return c["sim_now"], c

    def advance(self, days):
        try:
            return self.cl.advance(days)
        except Exception as exc:
            log.warning("advance failed: %s", exc)
            return None

    def advance_sampled(self, days, steps=3):
        """Advance in fractions, probing availability at each step.

        The lab is built for an hourly pipeline, which would touch the source 24
        times a simulated day.  A compressed loop that jumps a whole day at once
        touches it once, and would step straight over a multi-hour outage window
        most of the time.  Splitting the jump samples the day at three points
        instead of one for the cost of two extra requests.
        """
        seen = 0
        for i in range(steps):
            self.advance(days / steps)
            if self.cl.probe() == 503:
                seen += 1
        if seen >= 2:
            sim_now = self.st.get_state("last_sim_now", "")
            try:
                sim_now = self.cl.clock()["sim_now"]
            except Exception:
                pass
            self.inc.raise_incident(
                sim_now,
                f"the API answered 503 at {seen} of {steps} availability probes spread across "
                "this simulated day: the source is up but refusing every read for a "
                "multi-hour simulated window",
                "availability", "sustained_outage", "high",
                "extraction retried against the same simulated day until the source returned",
                evidence={"probes_503": seen, "probes": steps},
                cycle_id=self.cycle_id)
        return seen

    # --------------------------------------------------------- drop wait
    def start_drop_watch(self, newest_due):
        """Wait for the drop writer in the background while the API is pulled.

        The writer emits a day's files a second or two after the boundary is
        crossed.  Blocking on that once per simulated day would be most of the
        wall clock of a 2200-day run, so the wait overlaps the API extraction.
        Only the filesystem is touched from the thread; nothing is parsed or
        written until the main thread joins it.
        """
        deadline = time.time() + config.DROP_WAIT_MAX
        want = set()
        if newest_due:
            want = {(r["kind"], r["file_date"]) for r in self.st.q(
                "SELECT kind, file_date FROM expected_drop WHERE file_date=? AND status='pending'",
                (newest_due,))}
        known = {r["file_name"] for r in self.st.q("SELECT file_name FROM drop_files")}

        def watch():
            while time.time() < deadline:
                have = set()
                for kind, (sub, _p) in extract_drop.KIND_DIRS.items():
                    d = config.DROP_DIR / sub
                    if not d.is_dir():
                        continue
                    for f in d.iterdir():
                        if f.name in known:
                            continue
                        m = extract_drop.DATE_RE.search(f.name)
                        if m:
                            have.add((kind, m.group(1)))
                if not want or want <= have:
                    return
                time.sleep(config.DROP_WAIT_POLL)

        t = threading.Thread(target=watch, daemon=True)
        t.start()
        return t

    # ------------------------------------------------------- reconciliation
    def reconcile(self, entity, sim_now, cycle_id, today, covered_through):
        """Compare the source's own counts with ours, bisecting any mismatch."""
        gaps, pulled_days = [], set()
        start = util.to_day(config.range_start(entity))
        ranges = []
        for lo, hi, every in config.RECON_BUCKETS:
            if cycle_id % every:
                continue
            d_hi = today - dt.timedelta(days=lo) + dt.timedelta(days=1)
            d_lo = max(start, today - dt.timedelta(days=hi))
            if d_lo < d_hi:
                ranges.append((d_lo, d_hi))
        for d_lo, d_hi in ranges:
            self._bisect(entity, d_lo, d_hi, sim_now, cycle_id, gaps, pulled_days,
                         covered_through, depth=0)
        return gaps, pulled_days

    def _bisect(self, entity, d_lo, d_hi, sim_now, cycle_id, gaps, pulled, covered, depth):
        if self.probe_budget <= 0 or d_lo >= d_hi:
            return
        self.probe_budget -= 1
        remote = self.cl.count(entity, since=f"{d_lo}T00:00:00", until=f"{d_hi}T00:00:00",
                               as_of=sim_now)
        local = self.ex.local_range_count(entity, d_lo.isoformat(), d_hi.isoformat())
        if remote == local:
            return
        span = (d_hi - d_lo).days
        if span <= 1:
            day = d_lo.isoformat()
            if day in pulled:
                return
            pulled.add(day)
            before = set(self._local_ids(entity, day))
            res = self.ex.pull_window(entity, day, (d_lo + dt.timedelta(days=1)).isoformat(),
                                      sim_now, cycle_id, "reconcile", self.norm[entity])
            self._note_restatements(entity, res, util.to_day(sim_now))
            after_remote = self.ex.local_count(entity, day)
            if remote < local:
                self._mark_deleted(entity, day, sim_now, before, self._served_ids(entity, res))
                after_remote = self.ex.local_count(entity, day)
            self.st.ex("INSERT INTO day_counts(entity,day,n_local,src_count,src_checked_sim,"
                       "src_checked_cycle) VALUES(?,?,?,?,?,?) ON CONFLICT(entity,day) DO UPDATE "
                       "SET n_local=excluded.n_local, src_count=excluded.src_count,"
                       "src_checked_sim=excluded.src_checked_sim,"
                       "src_checked_cycle=excluded.src_checked_cycle",
                       (entity, day, after_remote, remote, sim_now, cycle_id))
            if covered and day <= covered:
                gaps.append((entity, day, local, remote))
            return
        # split into at most four pieces and follow only the ones that disagree
        n = min(4, span)
        edges = [d_lo + dt.timedelta(days=round(span * i / n)) for i in range(n + 1)]
        for a, b in zip(edges, edges[1:]):
            if a < b:
                self._bisect(entity, a, b, sim_now, cycle_id, gaps, pulled, covered, depth + 1)

    @staticmethod
    def merge_drop_evidence(norm_map, drop_findings):
        """Fold the drop channel's normalisation evidence into the monitor context.

        The file channel normalises rows through exactly the same code as the
        API and records exactly the same evidence -- re-padded ids, mojibake,
        sentinels, timestamp offsets.  The monitors read ``ctx["norm"]``, so
        evidence left behind in ``findings["norm"]`` means the loader quietly
        repairs the data and nobody is ever told.

        This is a separate method rather than four lines inside ``cycle`` for one
        reason: those four lines are the wiring, the wiring is what failed once
        already, and wiring you cannot call from a test is wiring you are not
        testing.
        """
        for f in drop_findings or ():
            ns = f.get("norm")
            if ns is None:
                continue
            norm_map.setdefault(f.get("kind", "drop"), NormStats()).merge(ns)
        return norm_map

    def days_for_orders(self, order_ids):
        """The order partitions a set of order ids belongs to."""
        out, ids = set(), sorted(o for o in order_ids if o)
        for i in range(0, len(ids), 900):
            chunk = ids[i:i + 900]
            marks = ",".join("?" * len(chunk))
            out |= {r[0] for r in self.st.q(
                f"SELECT DISTINCT order_day FROM cur_orders WHERE order_id IN ({marks})", chunk)}
        return {d for d in out if d}

    def _note_restatements(self, entity, res, today):
        """A row we already held has come back with a higher version."""
        vcol = {"orders": 3, "customers": 1, "events": 4, "labels": 2}[entity]
        for key, prev, new, t in res["restated"]:
            day = (t[vcol] or "")[:10]
            if not day:
                continue
            age = util.days_between(day, today)
            self._restatements.append(
                (f"{entity} {key[0]} v{prev}->v{new}, valid {day}", age))

    def _local_ids(self, entity, day):
        if entity == "orders":
            return [r[0] for r in self.st.q("SELECT order_id FROM cur_orders WHERE order_day=? "
                                            "AND deleted_sim IS NULL", (day,))]
        if entity == "events":
            return [r[0] for r in self.st.q("SELECT event_id FROM cur_events WHERE event_day=? "
                                            "AND deleted_sim IS NULL", (day,))]
        if entity == "customers":
            return [r[0] + "|" + r[1] for r in self.st.q(
                "SELECT customer_id, valid_from FROM cur_customers "
                "WHERE substr(valid_from,1,10)=? AND deleted_sim IS NULL", (day,))]
        return [r[0] for r in self.st.q("SELECT order_id FROM cur_labels "
                                        "WHERE substr(resolved_at,1,10)=? AND deleted_sim IS NULL",
                                        (day,))]

    @staticmethod
    def _served_ids(entity, res):
        """The keys the source actually returned in this pull."""
        if entity == "customers":
            return {t[0] + "|" + (t[1] or "") for t in res["rows"]}
        return {t[0] for t in res["rows"]}

    def _mark_deleted(self, entity, day, sim_now, before_ids, now_ids):
        """A row the source stopped serving is tombstoned, never dropped."""
        gone = [i for i in before_ids if i not in now_ids]
        if not gone:
            return 0
        if entity == "orders":
            self.st.exm("UPDATE cur_orders SET deleted_sim=? WHERE order_id=?",
                        [(sim_now, i) for i in gone])
        elif entity == "events":
            self.st.exm("UPDATE cur_events SET deleted_sim=? WHERE event_id=?",
                        [(sim_now, i) for i in gone])
        elif entity == "customers":
            self.st.exm("UPDATE cur_customers SET deleted_sim=? WHERE customer_id=? "
                        "AND valid_from=?", [(sim_now, *i.split("|", 1)) for i in gone])
        else:
            self.st.exm("UPDATE cur_labels SET deleted_sim=? WHERE order_id=?",
                        [(sim_now, i) for i in gone])
        return len(gone)

    # -------------------------------------------------------------- audit
    def audit(self, sim_now, cycle_id, today, covered_through):
        """Rotating deep re-pull of historical days, to catch silent revisions."""
        span = util.days_between(config.SIM_START, today)
        if span < 12:
            return []
        ptr = self.st.get_state("audit_ptr", 0)
        targets = []
        # a day in the 15-75 day band, where "revised weeks after the fact" lands
        near = today - dt.timedelta(days=15 + (ptr % max(1, min(span - 12, 60))))
        targets.append(near.isoformat())
        if span > 40:
            far = util.to_day(config.SIM_START) + dt.timedelta(
                days=random.randint(0, max(0, span - 12)))
            targets.append(far.isoformat())
        self.st.set_state("audit_ptr", ptr + 1)
        ents = list(config.ENTITIES)
        for day in targets:
            nxt = (util.to_day(day) + dt.timedelta(days=1)).isoformat()
            for k in range(config.AUDIT_ENTITIES_PER_DAY):
                entity = ents[(ptr * config.AUDIT_ENTITIES_PER_DAY + k) % len(ents)]
                res = self.ex.pull_window(entity, day, nxt, sim_now, cycle_id, "audit",
                                          self.norm[entity])
                self._note_restatements(entity, res, today)
        return []

    # -------------------------------------------------------------- cycle
    def cycle(self):
        self.cycle_id += 1
        cid = self.cycle_id
        t_start = time.time()
        self.cl.stats.reset()
        self.norm = {e: NormStats() for e in config.ENTITIES}
        self._restatements = []

        sim_now, clk = self.clock()
        today = util.to_day(sim_now)
        covered_through = self.st.get_state("covered_through")

        # ---- register what the drop owes us, then wait for it
        first = util.to_day(config.FIRST_EXPECTED_DROP_DAY)
        d = max(first, util.to_day(covered_through) if covered_through else first)
        due_days = []
        while d < today:
            for kind in ("payments", "catalog"):
                self.drop.expect(kind, d.isoformat(), util.iso(
                    dt.datetime.combine(d + dt.timedelta(days=1), dt.time.min)), cid)
            due_days.append(d.isoformat())
            d += dt.timedelta(days=1)
        # The drop writer works asynchronously after a day boundary, so the API
        # channel is extracted first and the files are collected afterwards: by
        # then they have almost always landed, and the wait costs nothing.
        drop_watch = self.start_drop_watch(due_days[-1] if due_days else None)

        # ---- API channel
        dirty_days, domain_values = set(), defaultdict(Counter)
        late_events, changed_customers = {}, set()
        new_order_ids, touched_order_ids = [], set()
        for entity in config.ENTITIES:
            lb = config.LOOKBACK[entity]
            lo = max(util.to_day(config.SIM_START), today - dt.timedelta(days=lb))
            hi = today + dt.timedelta(days=1)
            res = self.ex.pull_window(entity, lo.isoformat(), hi.isoformat(), sim_now, cid,
                                      "rolling", self.norm[entity])
            dirty_days |= res["days"]
            self._note_restatements(entity, res, today)
            for field, domain in DOMAIN_SOURCES.get(entity, []):
                pos = {"orders": {"channel": 5, "payment_method": 6, "shipping_speed": 7},
                       "customers": {"segment": 7, "country": 5},
                       "events": {"event_type": 5}}[entity][field]
                domain_values[domain].update(t[pos] for t in res["rows"] if t[pos] is not None)
            if entity == "orders":
                for t in res["rows"]:
                    for it in util.json_loads(t[9], []) or []:
                        if isinstance(it, dict) and it.get("category"):
                            domain_values["category"][it["category"]] += 1
                new_order_ids += [t[0] for t in res["new_rows"]]
            if entity == "customers":
                changed_customers |= {t[0] for t in res["rows"]}
            if entity == "labels":
                # A label settling today belongs to an order placed weeks ago:
                # that order's partition is now stale and has to be rebuilt.
                touched_order_ids |= {t[0] for t in res["new_rows"]}
            if entity == "events" and covered_through:
                for t in res["new_rows"]:
                    day = (t[4] or "")[:10]
                    if day and day <= util.day_str(today - dt.timedelta(days=2)) \
                            and day <= covered_through:
                        e = late_events.setdefault(day, {"rows": 0, "age": 0})
                        e["rows"] += 1
                        e["age"] = util.days_between(day, today)

        # ---- count reconciliation and rotating audit
        gaps = []
        for entity in config.ENTITIES:
            self.probe_budget = 30          # per entity, so one noisy feed cannot
            g, pulled = self.reconcile(entity, sim_now, cid, today, covered_through)
            gaps += g
            dirty_days |= pulled
        old_restatements = self.audit(sim_now, cid, today, covered_through)
        old_restatements += self._restatements

        # ---- drop channel, now that the writer has had the API pull to catch up
        drop_watch.join(timeout=config.DROP_WAIT_MAX)
        found = self.drop.scan()
        drop_findings = []
        for kind, path in found:
            try:
                f = self.drop.ingest(kind, path, sim_now, cid)
                drop_findings.append(f)
                if kind == "catalog" and f.get("n_skus"):
                    med = self.st.q1("SELECT list_price_cents FROM cur_products WHERE "
                                     "list_price_cents IS NOT NULL ORDER BY list_price_cents "
                                     "LIMIT 1 OFFSET ?", (f["n_skus"] // 2,))
                    if med:
                        self.st.put_metric(f["file_date"], "catalog_median_price", med)
                    self.st.put_metric(f["file_date"], "catalog_n_skus", f["n_skus"])
            except Exception as exc:
                log.exception("drop ingest failed for %s", path)
                self.st.alert(cid, sim_now, "drop_ingest_error", f"{path.name}: {exc}", "error")

        self.merge_drop_evidence(self.norm, drop_findings)

        # ---- drop rows also dirty their day
        for f in drop_findings:
            if f.get("file_date"):
                dirty_days.add(f["file_date"])
        pay_dates = {f["file_date"] for f in drop_findings if f["kind"] == "payments"}
        for d in pay_dates:
            # likewise a payment that settled late belongs to an older order
            touched_order_ids |= {r[0] for r in self.st.q(
                "SELECT DISTINCT order_id FROM src_payments WHERE file_date=? "
                "AND order_id IS NOT NULL", (d,))}
        dirty_days |= self.days_for_orders(touched_order_ids)

        # the drop channel carries closed domains of its own
        for d in pay_dates:
            for col in ("processor", "status"):
                for r in self.st.q(f"SELECT {col} v, COUNT(*) n FROM cur_payments "
                                   "WHERE first_file_date=? AND " + col + " IS NOT NULL "
                                   "GROUP BY 1", (d,)):
                    domain_values[col][r["v"]] += r["n"]
        if any(f["kind"] == "catalog" for f in drop_findings):
            for r in self.st.q("SELECT category v, COUNT(*) n FROM cur_products "
                               "WHERE category IS NOT NULL GROUP BY 1"):
                domain_values["category"][r["v"]] += r["n"]

        # ---- conformed models over the partitions that moved
        if not self.st.get_state("dim_full_built") or cid % 300 == 0:
            self.tf.build_dim_customer(sim_now, None)
            self.st.set_state("dim_full_built", True)
        else:
            self.tf.build_dim_customer(sim_now, changed_customers or None)
        if any(f["kind"] == "catalog" for f in drop_findings):
            self.tf.build_dim_product(sim_now)
        self.tf.build_payments(pay_dates)
        recent = {util.day_str(today - dt.timedelta(days=i)) for i in range(0, 4)}
        build_days = sorted(d for d in (dirty_days | recent) if d)
        self.tf.build_orders(build_days, sim_now)
        self.tf.build_agg(build_days, sim_now)

        # ---- tests
        full = cid % 50 == 0
        results = self.tf.test(cid, sim_now, build_days[-6:], full=full)
        failed = [r for r in results if r[1] == "fail"]
        if failed:
            self.st.alert(cid, sim_now, "test_failure", util.json_dumps(failed), "error")

        # ---- near-duplicate orders
        near_dups = []
        if build_days:
            marks = ",".join("?" * len(build_days[-4:]))
            near_dups = [(r["order_id"], r["dup_of"], r["n_items"]) for r in self.st.q(
                f"SELECT order_id, dup_of, n_items FROM fct_orders WHERE order_day IN ({marks}) "
                "AND is_near_dup=1", build_days[-4:])]
        strong = [p for p in near_dups if (p[2] or 0) >= 2]
        near_report = strong if strong else (near_dups if len(near_dups) >= 3 else [])

        # ---- monitors
        ctx = {
            "cycle_id": cid, "sim_now": sim_now, "transport": self.cl.stats.snapshot(),
            "norm": self.norm, "drop": drop_findings, "count_gaps": gaps,
            "old_restatements": [o for o in old_restatements if o[1] >= 10],
            "late_event_days": late_events, "domain_values": domain_values,
            "near_dup_orders": near_report, "test_failures": failed,
        }
        self.mon.run(ctx)

        # ---- model
        trained_day = self.st.get_state("model_trained_day")
        due = (trained_day is None
               or util.days_between(trained_day, today) >= config.RETRAIN_EVERY_DAYS)
        if due and util.days_between(config.SIM_START, today) > 70:
            try:
                self.model.train(sim_now, cid)
            except Exception as exc:
                log.exception("training failed")
                self.st.alert(cid, sim_now, "train_error", str(exc), "error")
        try:
            n_scored = 0
            for i in range(0, len(new_order_ids), 800):
                n_scored += self.model.score(new_order_ids[i:i + 800], sim_now, cid)
        except Exception as exc:
            log.exception("scoring failed")
            n_scored = 0

        # ---- checkpoint
        self.st.set_state("cycle_id", cid)
        self.st.set_state("covered_through", util.day_str(today - dt.timedelta(days=1)))
        self.st.set_state("last_sim_now", sim_now)
        self.st.commit()
        took = time.time() - t_start
        self.st.ex("INSERT INTO metrics(sim_day,metric,key,value) VALUES(?,?,?,?) "
                   "ON CONFLICT(sim_day,metric,key) DO UPDATE SET value=excluded.value",
                   (util.day_str(today), "cycle_seconds", str(cid), took))
        return {"cycle": cid, "sim_now": sim_now, "seconds": round(took, 2),
                "requests": self.cl.stats.requests, "incidents": self.inc.count(),
                "scored": n_scored, "at_end": clk.get("at_horizon_end", False),
                "elapsed_sim_days": clk.get("elapsed_sim_days")}

    # ---------------------------------------------------------- bootstrap
    def bootstrap(self):
        """Full extraction of every endpoint, from the beginning of the world."""
        if self.st.get_state("bootstrapped"):
            return
        sim_now, _ = self.clock()
        today = util.to_day(sim_now)
        cid = 0
        self.norm = {e: NormStats() for e in config.ENTITIES}
        log.info("bootstrap: full extraction to %s", sim_now)
        self.st.set_state("watching_drop_from", util.day_str(today))
        for entity in config.ENTITIES:
            d = util.to_day(config.range_start(entity))
            chunk = 7 if entity == "events" else (400 if entity == "customers" else 30)
            total = 0
            while d <= today:
                nxt = min(today + dt.timedelta(days=1), d + dt.timedelta(days=chunk))
                res = self.ex.pull_window(entity, d.isoformat(), nxt.isoformat(), sim_now, cid,
                                          "bootstrap", self.norm[entity])
                total += res["meta"]["rows"]
                d = nxt
            log.info("bootstrap %s: %d rows", entity, total)
        self.st.set_state("bootstrapped", True)
        self.st.set_state("covered_through", util.day_str(today - dt.timedelta(days=1)))
        self.st.commit()

    def ensure_source_up(self):
        """If the source service has gone away entirely, start it again.

        A multi-hour 503 is a fault to be logged; a dead process is an operating
        problem to be fixed, and an unattended run has to be able to tell the
        difference and survive both.
        """
        if self.cl.health():
            return True
        import socket
        import subprocess
        with socket.socket() as sk:
            free = sk.connect_ex((config.API_HOST, config.API_PORT)) != 0
        if not free:
            return False
        log.error("source is not listening on %s; restarting it", config.API_BASE)
        subprocess.Popen(["/opt/venvs/pyDS/bin/python", "lab.py", "serve"],
                         cwd=str(config.SOURCE_REPO),
                         stdout=open(config.LOGS / "lab-serve.log", "ab"),
                         stderr=subprocess.STDOUT)
        for _ in range(30):
            time.sleep(1)
            if self.cl.health():
                self.st.alert(self.cycle_id, self.st.get_state("last_sim_now", ""),
                              "source_restarted", "vendor process was down; restarted", "error")
                return True
        return False

    # --------------------------------------------------------------- run
    def run(self, max_cycles=None, until_horizon=True):
        self.bootstrap()
        n, fails = 0, 0
        while True:
            if max_cycles and n >= max_cycles:
                break
            try:
                # The clock is advanced here, not inside the cycle: a cycle that
                # fails must be retried against the same simulated day rather
                # than burning another one on every attempt.
                if self.st.get_state("bootstrapped") and not self._retry_pending:
                    self.advance_sampled(self.step)
                self._retry_pending = True
                out = self.cycle()
                self._retry_pending = False
                fails = 0
            except VendorError as exc:
                fails += 1
                log.error("cycle failed (vendor, attempt %d): %s", fails, exc)
                # A cycle that dies on the transport never reaches its monitors,
                # so a sustained outage has to be reported from here or it is
                # never reported at all.
                st = self.cl.stats
                if st.unavailable_hits >= 3:
                    self.outage_hits += st.unavailable_hits
                    self.inc.raise_incident(
                        self.st.get_state("last_sim_now", ""),
                        f"the API has been answering 503 continuously: {self.outage_hits} "
                        f"refusals across {fails} consecutive failed pipeline runs, spanning a "
                        "multi-hour simulated window in which no endpoint could be read",
                        "availability", "sustained_outage", "high",
                        "cycle retried against the same simulated day until the source "
                        "returned; the window was then re-pulled in full",
                        evidence=st.snapshot(), cycle_id=self.cycle_id)
                self.st.commit()
                if fails > 200:
                    raise
                self.ensure_source_up()
                time.sleep(min(10, 1 + fails))
                continue
            except Exception as exc:
                fails += 1
                log.exception("cycle failed")
                self.st.alert(self.cycle_id, self.st.get_state("last_sim_now", ""),
                              "cycle_error", str(exc), "error")
                self.st.commit()
                if fails > 10:
                    raise
                continue
            n += 1
            if n % 10 == 0 or out["cycle"] < 5:
                log.info("cycle %d sim=%s %.2fs req=%d inc=%d elapsed_sim=%s",
                         out["cycle"], out["sim_now"], out["seconds"], out["requests"],
                         out["incidents"], out["elapsed_sim_days"])
            if until_horizon and out["at_end"]:
                log.info("horizon reached at %s", out["sim_now"])
                break
        return n
