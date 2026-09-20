"""Monitors.

Each detector answers one question about the increment that just landed, and
fires at most once per run.  The design rule throughout is *specificity over
sensitivity*: a claim that turns out to match nothing is a false positive, so a
detector only speaks when the signal cannot plausibly be the documented
baseline (weekday seasonality, 18% growth, the Q4 peak, holiday spikes,
autocorrelated demand shocks, catalogue turnover) or ambient hostility.

Detectors fall into three groups:

* **deterministic** -- a truncated file, a header that changed, a timestamp with
  an offset where every other one is naive, a value outside a documented closed
  domain.  These fire on a single observation because they cannot happen by
  chance.
* **reconciliation** -- the source's own count for a settled past day no longer
  matches ours, or a row we already hold comes back with a higher version.
* **statistical** -- a distribution moved further than growth and seasonality
  can explain.  These carry a trailing baseline and a robust threshold, and
  fire late rather than often.
"""
from __future__ import annotations

import datetime as dt
import json
import math
from collections import Counter, defaultdict

from . import config, util

log = util.get_logger("monitors")

LEARN_DAYS = 90          # domains not documented as closed are learned for this long
LEARNED_MIN_SHARE = 0.02  # a learned domain needs a substantial novel value, not a rare tail
MIN_DAY_ROWS = 60        # below this a day is too thin to reason about distributions


def _dst_boundaries(year):
    """Last Sunday of March and of October, the European DST switch days."""
    out = []
    for month in (3, 10):
        d = dt.date(year, month, 31)
        while d.weekday() != 6:
            d -= dt.timedelta(days=1)
        out.append(d)
    return out


def near_dst(day: dt.date) -> bool:
    for b in _dst_boundaries(day.year):
        if abs((day - b).days) <= 1:
            return True
    return False


def robust_baseline(series, exclude_last=1):
    """Median and MAD of a metric series, ignoring the most recent points."""
    vals = [v for _, v in series[:len(series) - exclude_last] if v is not None]
    if len(vals) < 8:
        return None, None
    med = util.median(vals)
    mad = util.median([abs(v - med) for v in vals]) or 0.0
    return med, mad


def psi(a, b, bins=10):
    """Population stability index between two samples of numbers."""
    a = [x for x in a if x is not None]
    b = [x for x in b if x is not None]
    if len(a) < 200 or len(b) < 200:
        return None
    cuts = [util.quantile(a, i / bins) for i in range(1, bins)]
    cuts = sorted(set(cuts))
    if not cuts:
        return None

    def hist(xs):
        h = [0] * (len(cuts) + 1)
        for x in xs:
            i = 0
            while i < len(cuts) and x > cuts[i]:
                i += 1
            h[i] += 1
        return [max(c, 1) / len(xs) for c in h]

    ha, hb = hist(a), hist(b)
    return sum((q - p) * math.log(q / p) for p, q in zip(ha, hb))


def auc(scores, labels):
    """Rank-based AUC; returns None when a class is missing."""
    pairs = sorted(zip(scores, labels))
    n1 = sum(labels)
    n0 = len(labels) - n1
    if n0 == 0 or n1 == 0:
        return None
    rank, i, s = 0.0, 0, 0.0
    while i < len(pairs):
        j = i
        while j + 1 < len(pairs) and pairs[j + 1][0] == pairs[i][0]:
            j += 1
        avg = (i + j) / 2.0 + 1
        for k in range(i, j + 1):
            if pairs[k][1]:
                s += avg
        i = j + 1
    return (s - n1 * (n1 + 1) / 2.0) / (n0 * n1)


class Monitors:
    def __init__(self, store, incidents):
        self.st = store
        self.inc = incidents

    # ================================================================ entry
    def run(self, ctx):
        """Run every detector against one cycle's context."""
        self.ctx = ctx
        self.sim_now = ctx["sim_now"]
        self.cycle = ctx["cycle_id"]
        self.today = util.to_day(self.sim_now)
        self.age_days = util.days_between(config.SIM_START, self.today)

        self.record_metrics()
        for fn in (self.m_transport, self.m_schema_drift, self.m_text_encoding,
                   self.m_identifiers, self.m_timestamps, self.m_domains,
                   self.m_price_units, self.m_duplicates, self.m_delivery,
                   self.m_reconciliation, self.m_restatement, self.m_late_events,
                   self.m_hourly_profile, self.m_null_rates, self.m_label_maturation,
                   self.m_leakage, self.m_periodic):
            try:
                fn()
            except Exception as exc:                     # a monitor must never
                log.exception("monitor %s failed: %s", fn.__name__, exc)   # stop the loop
                self.st.alert(self.cycle, self.sim_now, "monitor_error",
                              f"{fn.__name__}: {exc}", "error")

    def fire(self, symptom, family, fault=None, confidence="medium", resolution="",
             evidence=None, key=None):
        return self.inc.raise_incident(self.sim_now, symptom, family, fault, confidence,
                                       resolution, evidence, key, self.cycle)

    # ============================================================= metrics
    def record_metrics(self):
        """Summarise every day that completed since the last cycle."""
        last = self.st.get_state("metrics_through", None)
        start = util.to_day(last) + dt.timedelta(days=1) if last else util.to_day(config.SIM_START)
        end = self.today                                  # today is still in progress
        days = []
        d = start
        while d < end and len(days) < 40:
            days.append(d.isoformat())
            d += dt.timedelta(days=1)
        for day in days:
            self._metrics_for_day(day)
        if days:
            self.st.set_state("metrics_through", days[-1])
        self.completed_days = days

    def _metrics_for_day(self, day):
        put = lambda m, v, k="": self.st.put_metric(day, m, v, k)   # noqa: E731
        row = self.st.q(
            "SELECT COUNT(*) n, AVG(order_total_cents) avg_t, AVG(n_units) avg_u, "
            "AVG(n_items) avg_i FROM cur_orders WHERE order_day=? AND deleted_sim IS NULL",
            (day,))[0]
        put("orders_n", row["n"])
        if row["n"]:
            put("orders_avg_total", row["avg_t"] or 0)
            put("orders_avg_units", row["avg_u"] or 0)
            put("orders_avg_items", row["avg_i"] or 0)
            med = self.st.q1(
                "SELECT order_total_cents FROM cur_orders WHERE order_day=? AND deleted_sim IS NULL"
                " AND order_total_cents IS NOT NULL ORDER BY order_total_cents "
                "LIMIT 1 OFFSET ?", (day, max(0, row["n"] // 2)))
            if med is not None:
                put("orders_median_total", med)
            lag = self.st.q1(
                "SELECT AVG(l) FROM (SELECT (julianday(knowledge_time)-julianday(occurred_at))"
                "*86400.0 l FROM cur_orders WHERE order_day=? AND knowledge_time IS NOT NULL "
                "AND occurred_at IS NOT NULL)", (day,))
            if lag is not None:
                put("orders_lag_s", lag)
            neg = self.st.q1(
                "SELECT COUNT(*) FROM cur_orders WHERE order_day=? AND knowledge_time < occurred_at",
                (day,))
            put("orders_neg_lag", neg or 0)
            orph = self.st.q1(
                "SELECT COUNT(*) FROM cur_orders o WHERE o.order_day=? AND NOT EXISTS("
                "SELECT 1 FROM cur_customers c WHERE c.customer_id=o.customer_id)", (day,))
            put("orders_orphan", orph or 0)

        ev = self.st.q("SELECT COUNT(*) n, AVG(lag_seconds) avg_lag, MAX(lag_seconds) max_lag "
                       "FROM cur_events WHERE event_day=?", (day,))[0]
        put("events_n", ev["n"])
        if ev["n"]:
            put("events_avg_lag_s", ev["avg_lag"] or 0)
            put("events_max_lag_s", ev["max_lag"] or 0)
            for h, c in self.st.q("SELECT substr(occurred_at,12,2) h, COUNT(*) c FROM cur_events "
                                  "WHERE event_day=? GROUP BY 1", (day,)):
                put("events_hour", c, h)
        if row["n"]:
            for h, c in self.st.q("SELECT substr(occurred_at,12,2) h, COUNT(*) c FROM cur_orders "
                                  "WHERE order_day=? AND deleted_sim IS NULL GROUP BY 1", (day,)):
                put("orders_hour", c, h)

        pay = self.st.q("SELECT COUNT(*) n, AVG(amount_cents) avg_a FROM cur_payments "
                        "WHERE first_file_date=?", (day,))[0]
        put("payments_n", pay["n"])
        if pay["n"]:
            put("payments_avg_amount", pay["avg_a"] or 0)
            med = self.st.q1("SELECT amount_cents FROM cur_payments WHERE first_file_date=? "
                             "AND amount_cents IS NOT NULL ORDER BY amount_cents LIMIT 1 OFFSET ?",
                             (day, max(0, pay["n"] // 2)))
            if med is not None:
                put("payments_median_amount", med)

        lab = self.st.q("SELECT COUNT(*) n, SUM(COALESCE(refunded_within_30d,0)) r "
                        "FROM cur_labels WHERE substr(resolved_at,1,10)=?", (day,))[0]
        put("labels_n", lab["n"])
        if lab["n"]:
            put("labels_refund_rate", (lab["r"] or 0) / lab["n"])

    # ========================================================== detectors
    def m_transport(self):
        t = self.ctx["transport"]
        if t["unavailable_hits"] >= 3 or t["unavailable_streak_s"] >= 15:
            self.fire(
                f"API returned 503 {t['unavailable_hits']} times this cycle "
                f"(longest continuous unavailability {t['unavailable_streak_s']}s real, "
                "spanning a multi-hour simulated window); reads blocked until it cleared",
                "availability", "sustained_outage", "high",
                "retried with backoff; the window was re-pulled once the source returned",
                evidence=t)

    def m_schema_drift(self):
        for entity, ns in self.ctx["norm"].items():
            if ns.renamed:
                self.fire(
                    f"{entity} feed: field(s) {ns.renamed} changed name mid-stream "
                    f"({', '.join(f'{v} now carries {k}' for k, v in ns.renamed.items())})",
                    "ingestion", "field_rename_midstream", "high",
                    "extractor follows the rename by name similarity; both names retained in "
                    "the landing zone",
                    evidence={"entity": entity, "renamed": ns.renamed,
                              "missing": ns.missing_fields, "new": ns.unexpected_fields})
            elif ns.unexpected_fields and self.age_days > 20:
                new = {k: v for k, v in ns.unexpected_fields.items()}
                self.st.alert(self.cycle, self.sim_now, "new_api_field",
                              json.dumps({"entity": entity, "fields": new}))
                self.st.set_state("pending_new_fields",
                                  {**(self.st.get_state("pending_new_fields") or {}),
                                   entity: sorted(new)})

    def m_text_encoding(self):
        hits = {}
        for entity, ns in self.ctx["norm"].items():
            for field, n in ns.mojibake.items():
                hits[f"{entity}.{field}"] = hits.get(f"{entity}.{field}", 0) + n
        if hits:
            self.fire(
                "text fields contain UTF-8-decoded-as-latin-1 artefacts "
                f"(e.g. GijÃ³n for Gijón): {hits}",
                "ingestion", "mojibake_city", "high",
                "rows landed verbatim; a repair pass re-encodes latin-1 mojibake for the "
                "conformed dimension",
                evidence=hits)

    def m_identifiers(self):
        viol = Counter()
        padded = 0
        for entity, ns in self.ctx["norm"].items():
            for f, n in ns.id_width_violations.items():
                viol[f"{entity}.{f}"] += n
            padded += ns.padded_ids
        if viol:
            self.fire(
                f"join keys arrived without their leading zeros ({dict(viol)}); "
                f"{padded} ids re-padded to restore the join",
                "ingestion", "id_leading_zeros", "high",
                "ids re-padded to the modal width on load; raw form kept in the landing zone",
                evidence=dict(viol))
        # orphan facts are the same fault seen from the other side
        for day in self.completed_days[-3:]:
            n = self.st.q1("SELECT value FROM metrics WHERE sim_day=? AND metric='orders_orphan'",
                           (day,)) or 0
            tot = self.st.q1("SELECT value FROM metrics WHERE sim_day=? AND metric='orders_n'",
                             (day,)) or 0
            if tot >= MIN_DAY_ROWS and n / tot > 0.02:
                self.st.alert(self.cycle, self.sim_now, "orphan_orders",
                              f"{day}: {n:.0f}/{tot:.0f} orders have no customer row")

    def m_timestamps(self):
        off = sum(ns.tz_offsets for ns in self.ctx["norm"].values())
        naive = sum(ns.tz_naive for ns in self.ctx["norm"].values())
        if off:
            seen_before = self.st.get_state("tz_offset_seen", 0)
            self.st.set_state("tz_offset_seen", seen_before + off)
            self.fire(
                f"timestamps started carrying UTC offsets ({off} of {off + naive} parsed this "
                "cycle) where the feed had been naive throughout",
                "time", "tz_mixed", "high",
                "offsets normalised to UTC on load; the mixture is recorded per row",
                evidence={"with_offset": off, "naive": naive})

        # clock skew: knowledge ahead of the clock, or knowledge before the event
        neg = sum(self.st.q1("SELECT value FROM metrics WHERE sim_day=? AND metric='orders_neg_lag'",
                             (d,)) or 0 for d in self.completed_days)
        # Offsets normalised to UTC can legitimately reorder a naive comparison,
        # so a mixed-timezone feed is not evidence of drifting clocks.
        if neg >= 3 and not off:
            self.fire(
                f"{neg:.0f} orders carry knowledge_time earlier than occurred_at -- source "
                "timestamps are inconsistent with each other by minutes",
                "time", "clock_skew", "medium",
                "rows kept; lag treated as unreliable for those days",
                evidence={"negative_lag_rows": neg})
            return
        ser = self.st.metric_series("orders_lag_s")
        if len(ser) >= 20 and self.completed_days:
            med, mad = robust_baseline(ser, exclude_last=len(self.completed_days))
            cur = [v for d, v in ser if d in self.completed_days]
            if med is not None and cur:
                worst = max(cur, key=lambda v: abs(v - med))
                delta = abs(worst - med)
                if delta > max(120.0, 8 * (mad or 1.0)) and delta < 6 * 3600:
                    self.fire(
                        f"order knowledge lag stepped from a stable {med:.0f}s to {worst:.0f}s "
                        f"(shift of {delta / 60:.1f} minutes) with no change in volume",
                        "time", "clock_skew", "medium",
                        "timestamps kept as delivered; downstream freshness checks widened",
                        evidence={"baseline_s": med, "observed_s": worst, "mad": mad})

    def m_domains(self):
        """A value outside a closed domain is never baseline behaviour."""
        counters = self.ctx["domain_values"]
        for domain, counter in counters.items():
            if not counter:
                continue
            self.st.note_values(domain, counter, self.sim_now)
            known = config.DOMAINS.get(domain)
            documented = known is not None
            if not documented:
                # Not documented as closed, so the domain has to be learned.  A
                # rare-but-legitimate value must not look like a fault, hence the
                # long learning period and the minimum share below.
                if self.age_days < LEARN_DAYS:
                    continue
                known = {r["value"] for r in self.st.q(
                    "SELECT value FROM value_seen WHERE domain=? AND first_seen_sim < ?",
                    (domain, util.iso(self.today - dt.timedelta(days=14))))}
                if not known:
                    continue
            novel = [v for v in counter if v not in known]
            if not novel:
                continue
            share = sum(counter[v] for v in novel) / max(1, sum(counter.values()))
            if not documented and (share < LEARNED_MIN_SHARE
                                   or sum(counter[v] for v in novel) < 10):
                continue
            # did an established value stop at the same moment?
            cutoff = util.iso(self.today - dt.timedelta(days=10))
            stopped = [r["value"] for r in self.st.q(
                "SELECT value, last_seen_sim FROM value_seen WHERE domain=? AND value NOT IN "
                "(SELECT value FROM value_seen WHERE domain=? AND last_seen_sim>=?) "
                "AND n_seen > 50", (domain, domain, cutoff))]
            if stopped:
                self.fire(
                    f"categorical '{domain}': new value(s) {novel} appeared "
                    f"({share:.0%} of the day) at the same time as {stopped} stopped being "
                    "emitted -- one class has been renamed and is now split in two",
                    "semantic", "category_rename", "high",
                    "conformed models map the old and new spellings onto one class",
                    evidence={"domain": domain, "new": novel, "stopped": stopped,
                              "share": round(share, 4)},
                    key=f"category_rename:{domain}")
            else:
                self.fire(
                    f"categorical '{domain}': value(s) {novel} exist only in the most recent "
                    f"period ({share:.0%} of the day), {self.age_days} days into the feed, "
                    "with every previous value still present",
                    "semantic", "new_category_late", "high",
                    "domain allow-list extended; models treat it as an unseen level",
                    evidence={"domain": domain, "new": novel, "share": round(share, 4)},
                    key=f"new_category_late:{domain}")

        sent = {}
        for entity, ns in self.ctx["norm"].items():
            for k, n in ns.sentinels.items():
                sent[f"{entity}:{k}"] = sent.get(f"{entity}:{k}", 0) + n
        if sent:
            self.fire(
                f"magic values standing in for NULL appeared in the feed: {sent}",
                "semantic", "sentinel_values", "high",
                "sentinels mapped to NULL in the conformed layer; raw values retained",
                evidence=sent)

    def m_price_units(self):
        for metric, what in (("orders_median_total", "order_total_cents"),
                             ("payments_median_amount", "payments.amount_cents"),
                             ("catalog_median_price", "products.list_price_cents")):
            ser = self.st.metric_series(metric)
            if len(ser) < 25 or not self.completed_days:
                continue
            med, _ = robust_baseline(ser, exclude_last=len(self.completed_days))
            cur = [v for d, v in ser if d in self.completed_days and v]
            if not med or not cur:
                continue
            ratio = max(cur) / med if max(cur) > med else med / max(min(cur), 1e-9)
            if ratio >= 20:
                self.fire(
                    f"{what} changed scale at a date boundary: trailing median {med:.0f} -> "
                    f"{cur[-1]:.0f} ({ratio:.0f}x) with volume and basket composition unchanged",
                    "semantic", "unit_switch_price", "high",
                    "unit converted back at the boundary date in the conformed layer",
                    evidence={"metric": metric, "baseline": med, "observed": cur[-1]},
                    key="unit_switch_price")

    def m_duplicates(self):
        d = self.ctx["drop"]
        dup_rows = sum(f.get("dup_exact_rows", 0) for f in d)
        if dup_rows:
            self.fire(
                f"payments drop contains {dup_rows} byte-identical duplicate rows within a "
                "single file -- a write retry was not idempotent",
                "ingestion", "dup_exact_payments", "high",
                "deduplicated on the full row hash into the conformed payments table",
                evidence={"duplicate_rows": dup_rows,
                          "files": [f["file"] for f in d if f.get("dup_exact_rows")]})

        pairs = self.ctx.get("near_dup_orders") or []
        if pairs:
            self.fire(
                f"{len(pairs)} order pairs share a customer, an identical item basket and an "
                "identical total, submitted seconds apart under different order_ids "
                f"(e.g. {pairs[0]})",
                "ingestion", "dup_near_orders", "high",
                "near-duplicates collapsed to the first order_id in the conformed fact table",
                evidence={"n_pairs": len(pairs), "example": pairs[:3]})

    def m_delivery(self):
        for f in self.ctx["drop"]:
            if f.get("truncated"):
                self.fire(
                    f"drop file {f['file']} is truncated ({'; '.join(f['notes'])})",
                    "delivery", "partial_write", "high",
                    "partial rows discarded; the day was reconstructed from the API where possible",
                    evidence=f["notes"])
            if f.get("encoding_changed"):
                self.fire(
                    f"drop file {f['file']} is no longer valid UTF-8 and decodes as cp1252",
                    "encoding", "encoding_change", "high",
                    "reader sniffs the encoding per file and re-encodes to UTF-8",
                    evidence=f["notes"])
            if f.get("compression_changed") or any("compression" in n for n in f.get("notes", [])):
                self.fire(
                    f"drop file {f['file']} compression no longer matches its extension "
                    f"({'; '.join(f['notes'])})",
                    "encoding", "compression_change", "high",
                    "reader decides compression from the magic number, not the file name",
                    evidence=f["notes"])
            if f.get("header_added"):
                self.fire(
                    f"drop file {f['file']} gained column(s) {f['header_added']} at a date "
                    "boundary",
                    "schema", "column_added", "high",
                    "loader is positional-by-name; the new column is landed and carried",
                    evidence={"added": f["header_added"], "file": f["file"]})
            if f.get("header_removed"):
                # A disappearing column is not itself in the catalogue; claiming a
                # named fault here would risk a false positive, so this one goes
                # in as a schema-family observation only.
                self.fire(
                    f"drop file {f['file']} lost column(s) {f['header_removed']} at a date "
                    "boundary",
                    "schema", None, "medium",
                    "loader binds by header name and keeps the column nullable downstream",
                    evidence={"removed": f["header_removed"], "file": f["file"]},
                    key="column_removed")
            if f.get("header_reordered"):
                self.fire(
                    f"drop file {f['file']} has the same columns in a different order",
                    "schema", "column_reordered", "high",
                    "loader binds columns by header name, so the reorder was absorbed",
                    evidence={"file": f["file"]})
            if f.get("duplicate_of") and f["kind"] == "payments":
                self.fire(
                    f"drop file {f['file']} is byte-identical to {f['duplicate_of']} -- the same "
                    "payload delivered twice under different names",
                    "delivery", "duplicate_delivery", "high",
                    "content hash checked before load; the second copy contributed no new rows",
                    evidence={"file": f["file"], "same_as": f["duplicate_of"]})
            if f.get("lag_days", 0) >= 1:
                self.fire(
                    f"{f['kind']} file for {f['file_date']} arrived {f['lag_days']} simulated "
                    "day(s) after it was due",
                    "delivery", "late_partition", "high",
                    "partition reprocessed on arrival; downstream aggregates recomputed",
                    evidence={"file": f["file"], "lag_days": f["lag_days"]})

        # two files for the same content day, whatever their names
        same_day = self.st.q(
            "SELECT kind, file_date, COUNT(*) n, GROUP_CONCAT(file_name) fs FROM drop_files "
            "GROUP BY kind, file_date HAVING n > 1 LIMIT 3")
        if same_day:
            r = same_day[0]
            self.fire(
                f"{r['n']} separate {r['kind']} files delivered for content day {r['file_date']} "
                f"({r['fs']})",
                "delivery", "duplicate_delivery", "high",
                "deduplicated by content hash on load",
                evidence={"kind": r["kind"], "file_date": r["file_date"], "files": r["fs"]},
                key="duplicate_delivery_same_day")

        # nothing arrived at all
        overdue = self.st.q(
            "SELECT kind, file_date, due_sim FROM expected_drop WHERE status='pending' "
            "AND file_date < ? ORDER BY file_date",
            (util.day_str(self.today - dt.timedelta(days=4)),))
        if overdue:
            days = [f"{r['kind']}:{r['file_date']}" for r in overdue]
            r = overdue[0]
            if util.days_between(r["file_date"], self.today) <= 400:
                self.fire(
                    f"{r['kind']} file for {r['file_date']} never arrived; still absent "
                    f"{util.days_between(r['file_date'], self.today)} simulated days later "
                    f"({len(overdue)} outstanding: {days[:5]})",
                    "delivery", "missing_delivery", "high",
                    "day marked unavailable; payments for it reconstructed from later files "
                    "where settlement re-reported them",
                    evidence={"outstanding": days[:10]})

    def m_reconciliation(self):
        """The source's own count for a settled day no longer matches ours."""
        for entity, day, local, remote in self.ctx.get("count_gaps", []):
            if remote < local:
                fault = ("survivorship_customers" if entity == "customers"
                         else "silent_row_deletion")
                fam = "integrity"
                self.fire(
                    f"{entity}: {local - remote} row(s) that we hold for {day} are no longer "
                    "served by the source, with no tombstone and no version bump "
                    f"(source now reports {remote}, we hold {local})",
                    fam, fault, "high",
                    "rows kept and marked deleted in the warehouse so historical facts still join",
                    evidence={"entity": entity, "day": day, "local": local, "source": remote})
        for entity, day, local, remote in self.ctx.get("count_gaps", []):
            if remote <= local:
                continue
            age = util.days_between(day, self.today)
            if age <= 3:
                continue
            self.st.alert(self.cycle, self.sim_now, "late_rows",
                          f"{entity} {day}: source {remote} > local {local}")
            # Baseline: customers and labels routinely gain one or two rows at
            # age 4-7, because their knowledge lag slightly exceeds the rolling
            # window.  Events never do -- so a block of events materialising for
            # a settled day is the fault, not the knowledge lag.
            if entity == "events" and (remote - local) >= 10:
                self.fire(
                    f"{remote - local} web events for {day} only became knowable {age} "
                    "simulated days after they occurred; the day was complete and settled "
                    f"when we extracted it (we held {local}, the source now serves {remote})",
                    "time", "late_arriving_events", "high",
                    "the partition was reprocessed from the source once reconciliation found "
                    "it had grown; daily aggregates and features rebuilt",
                    evidence={"day": day, "age_days": age, "held": local, "source": remote},
                    key="late_arriving_events")

    def m_restatement(self):
        old = self.ctx.get("old_restatements") or []
        if old:
            worst = max(old, key=lambda r: r[1])
            self.fire(
                f"{len(old)} historical row(s) came back with a higher version and a later "
                f"knowledge_time, the oldest revised {worst[1]} simulated days after the fact "
                f"({worst[0]}); no notification from the source",
                "restatement", "backfill_restatement", "high",
                "warehouse is append-only per version, so both beliefs remain queryable; "
                "conformed models rebuilt for the affected partitions",
                evidence={"n": len(old), "examples": [list(map(str, o)) for o in old[:5]]})

    def m_late_events(self):
        late = self.ctx.get("late_event_days") or {}
        if late:
            worst_day, info = max(late.items(), key=lambda kv: kv[1]["age"])
            if info["age"] >= 2 and info["rows"] >= 15:
                self.fire(
                    f"{sum(v['rows'] for v in late.values())} events for day(s) "
                    f"{sorted(late)[:4]} only became knowable {info['age']} simulated days "
                    "after they occurred; the days looked complete when first extracted",
                    "time", "late_arriving_events", "high",
                    "event partitions are reprocessed on a rolling lookback, so the day was "
                    "restated rather than lost",
                    evidence={"days": {k: v for k, v in list(late.items())[:6]}})
        ser = self.st.metric_series("events_avg_lag_s")
        if len(ser) >= 25 and self.completed_days:
            med, mad = robust_baseline(ser, exclude_last=len(self.completed_days))
            cur = [v for d, v in ser if d in self.completed_days]
            if med and cur and max(cur) > max(med * 6, med + 12 * (mad or 1)) and max(cur) > 7200:
                self.fire(
                    f"event knowledge lag jumped from {med / 60:.0f} min to "
                    f"{max(cur) / 3600:.1f} hours",
                    "time", "late_arriving_events", "medium",
                    "rolling lookback widened for the affected partitions",
                    evidence={"baseline_s": med, "observed_s": max(cur)},
                    key="late_arriving_events")

    def m_hourly_profile(self):
        """A wall-clock hour that repeats or disappears.

        Runs over *both* fact streams.  A civil-time transition is injected into
        whichever table the source chooses, and watching only one of them means
        watching the wrong one half the time.
        """
        for metric, total_metric, min_total, label in (
                ("events_hour", "events_n", 200, "web-event"),
                ("orders_hour", "orders_n", 60, "order")):
            for day in self.completed_days:
                tot = self.st.q1("SELECT value FROM metrics WHERE sim_day=? AND metric=?",
                                 (day, total_metric)) or 0
                if tot < min_total:
                    continue
                obs = {r["key"]: r["value"] for r in self.st.q(
                    "SELECT key, value FROM metrics WHERE sim_day=? AND metric=?",
                    (day, metric))}
                hist = defaultdict(list)
                for r in self.st.q(
                        "SELECT key, value FROM metrics WHERE metric=? AND sim_day < ? "
                        "AND sim_day >= ?", (metric, day, util.day_str(
                            util.to_day(day) - dt.timedelta(days=28)))):
                    hist[r["key"]].append(r["value"])
                if len(hist) < 20:
                    continue
                exp_tot = sum(sum(v) for v in hist.values()) or 1
                findings = []
                for h in [f"{i:02d}" for i in range(24)]:
                    exp_share = sum(hist.get(h, [])) / exp_tot
                    o = obs.get(h, 0)
                    o_share = o / tot
                    # A pile-up is judged without the share floor: the hour a
                    # civil-time transition repeats into is a *quiet* hour, so
                    # requiring it to be busy on an ordinary day would filter out
                    # exactly the signal being looked for.
                    if o >= 15 and o_share > max(exp_share * 2.5, 0.15):
                        findings.append((h, "doubled", exp_share, o_share))
                    elif exp_share >= 0.015 and o == 0:
                        findings.append((h, "missing", exp_share, 0.0))
                if not findings:
                    continue
                if not near_dst(util.to_day(day)):
                    # An empty hour on an ordinary day is Poisson noise: a night
                    # hour holding 1.5% of a day's rows is empty by chance often
                    # enough that the horizon's ~50k hour-checks guarantee some.
                    # Only a day at a civil-time transition can carry the fault
                    # this detector is named for.
                    self.st.alert(self.cycle, self.sim_now, "hour_profile_anomaly",
                                  util.json_dumps({"day": day, "metric": metric,
                                                   "findings": findings}))
                    continue
                self.fire(
                    f"{label} hourly profile on {day} is not a normal day: "
                    + "; ".join(f"hour {h} {what} (expected {e:.1%}, saw {o:.1%})"
                                for h, what, e, o in findings)
                    + " -- this is a European DST switch day",
                    "time", "dst_duplicate_hour", "high",
                    "hourly aggregates rebuilt against a DST-aware calendar",
                    evidence={"day": day, "stream": metric, "findings": findings,
                              "dst_boundary": True})
                return

    def m_null_rates(self):
        """A column that never had nulls suddenly having them is worth saying."""
        checks = [("cur_orders", "order_day", ["channel", "payment_method", "shipping_speed",
                                               "order_total_cents", "customer_id"]),
                  ("cur_payments", "first_file_date", ["billing_city", "processor",
                                                       "amount_cents", "status"])]
        for day in self.completed_days[-2:]:
            for table, daycol, cols in checks:
                tot = self.st.q1(f"SELECT COUNT(*) FROM {table} WHERE {daycol}=?", (day,)) or 0
                if tot < MIN_DAY_ROWS:
                    continue
                for c in cols:
                    n = self.st.q1(f"SELECT COUNT(*) FROM {table} WHERE {daycol}=? AND {c} IS NULL",
                                   (day,)) or 0
                    self.st.put_metric(day, "null_rate", n / tot, f"{table}.{c}")
        for table, _daycol, cols in checks:
            for c in cols:
                key = f"{table}.{c}"
                ser = self.st.metric_series("null_rate", key)
                if len(ser) < 25 or not self.completed_days:
                    continue
                base = [v for d, v in ser if d not in self.completed_days]
                cur = [v for d, v in ser if d in self.completed_days]
                if not cur or not base:
                    continue
                b = util.median(base) or 0.0
                if max(cur) > 0.03 and max(cur) > b + 0.03:
                    self.st.alert(self.cycle, self.sim_now, "null_rate_jump",
                                  f"{key}: {b:.3f} -> {max(cur):.3f}")
                    self.st.set_state("null_jump_cols",
                                      sorted(set((self.st.get_state("null_jump_cols") or [])
                                                 + [key])))

    def m_label_maturation(self):
        """Absence of a label is not a negative near the edge of `as_of`."""
        if self.age_days < 45 or self.st.get_state("label_censoring_logged"):
            return
        young_lo = util.day_str(self.today - dt.timedelta(days=config.LABEL_WINDOW_DAYS - 5))
        young_hi = util.day_str(self.today - dt.timedelta(days=5))
        old_lo = util.day_str(self.today - dt.timedelta(days=config.LABEL_WINDOW_DAYS + 25))
        old_hi = util.day_str(self.today - dt.timedelta(days=config.LABEL_WINDOW_DAYS + 5))

        def coverage(lo, hi):
            tot = self.st.q1("SELECT COUNT(*) FROM cur_orders WHERE order_day>=? AND order_day<?",
                             (lo, hi)) or 0
            lab = self.st.q1(
                "SELECT COUNT(*) FROM cur_orders o JOIN cur_labels l USING(order_id) "
                "WHERE o.order_day>=? AND o.order_day<?", (lo, hi)) or 0
            return tot, lab

        ty, ly = coverage(young_lo, young_hi)
        to, lo_ = coverage(old_lo, old_hi)
        if ty < 200 or to < 200:
            return
        cy, co = util.pct(ly, ty), util.pct(lo_, to)
        if co - cy > 0.25:
            self.st.set_state("label_censoring_logged", True)
            self.fire(
                f"label coverage depends on order age: orders {5}-{config.LABEL_WINDOW_DAYS - 5} "
                f"days old have labels for {cy:.0%} of rows, orders "
                f"{config.LABEL_WINDOW_DAYS + 5}-{config.LABEL_WINDOW_DAYS + 25} days old for "
                f"{co:.0%}. A non-refunded order gets no row at all until its 30-day window "
                "closes, so recent unlabelled orders are undetermined, not negatives",
                "statistical", "label_censoring", "high",
                "training excludes orders whose refund window has not closed as of the "
                "prediction date; absence is never encoded as a negative",
                evidence={"young_coverage": round(cy, 3), "mature_coverage": round(co, 3),
                          "n_young": ty, "n_mature": to})

    def m_leakage(self):
        """A column you have at training time but not at prediction time.

        Its signature does not need labels: the field is populated for orders
        old enough to have resolved and empty for fresh ones, so its fill rate
        is a function of order age.  That is exactly what makes it unusable --
        at the moment an order needs scoring, the column is not there.
        """
        pending = self.st.get_state("pending_new_fields") or {}
        fields = pending.get("orders") or []
        if not fields or self.cycle % 10 or self.st.get_state("leakage_logged"):
            return
        fresh_lo = util.day_str(self.today - dt.timedelta(days=12))
        fresh_hi = util.day_str(self.today)
        mature_lo = util.day_str(self.today - dt.timedelta(days=95))
        mature_hi = util.day_str(self.today - dt.timedelta(days=40))
        for f in fields:
            def fill(lo, hi, f=f):
                r = self.st.q(
                    "SELECT COUNT(*) n, SUM(json_extract(extra_json,'$.' || ?) IS NOT NULL) k "
                    "FROM cur_orders WHERE order_day>=? AND order_day<? AND deleted_sim IS NULL",
                    (f, lo, hi))[0]
                return r["n"], (r["k"] or 0)

            nf, kf = fill(fresh_lo, fresh_hi)
            nm, km = fill(mature_lo, mature_hi)
            if nf < 150 or nm < 150 or km == 0:
                continue
            rate_f, rate_m = kf / nf, km / nm
            if rate_m > 0.5 and rate_f < 0.15:
                self.st.set_state("leakage_logged", True)
                self.fire(
                    f"the orders feed carries a field '{f}' that is present on "
                    f"{rate_m:.0%} of orders old enough to have resolved and on only "
                    f"{rate_f:.0%} of orders from the last 12 days. It is available when a "
                    "model is trained and absent when an order needs scoring -- target "
                    "leakage, not a usable feature",
                    "leakage", "leakage_column", "high",
                    "field excluded from the feature set; retained in the warehouse for audit",
                    evidence={"field": f, "fill_mature": round(rate_m, 3),
                              "fill_fresh": round(rate_f, 3), "n_mature": nm, "n_fresh": nf})
                return

    # ------------------------------------------------------- heavier passes
    def m_periodic(self):
        every = 25
        if self.cycle % every or self.age_days < 200:
            return
        self._covariate_shift()
        self._mnar()
        self._id_reuse()

    def _covariate_shift(self):
        if self.st.get_state("covariate_shift_logged"):
            return
        recent_lo = util.day_str(self.today - dt.timedelta(days=45))
        base_hi = util.day_str(self.today - dt.timedelta(days=120))
        base_lo = util.day_str(self.today - dt.timedelta(days=300))
        rec = [r[0] for r in self.st.q(
            "SELECT order_total_cents FROM cur_orders WHERE order_day>=? AND "
            "order_total_cents IS NOT NULL", (recent_lo,))]
        base = [r[0] for r in self.st.q(
            "SELECT order_total_cents FROM cur_orders WHERE order_day>=? AND order_day<? "
            "AND order_total_cents IS NOT NULL", (base_lo, base_hi))]
        p = psi(base, rec)
        worst = [("order_total_cents", p)] if p else []
        for col, table in (("channel", "cur_orders"), ("payment_method", "cur_orders"),
                           ("shipping_speed", "cur_orders")):
            a = Counter(r[0] for r in self.st.q(
                f"SELECT {col} FROM {table} WHERE order_day>=? AND order_day<?",
                (base_lo, base_hi)))
            b = Counter(r[0] for r in self.st.q(
                f"SELECT {col} FROM {table} WHERE order_day>=?", (recent_lo,)))
            na, nb = sum(a.values()), sum(b.values())
            if na < 500 or nb < 500:
                continue
            # Only levels that existed before the baseline window opened.  A
            # level that arrives during the comparison and then grows is a new
            # category -- the closed-domain monitor already owns that event and
            # has already named it.  Including it here would re-report one
            # phenomenon under a second fault name, which with eleven faults
            # drawn from thirty is a false positive far more often than not.
            stable = {r["value"] for r in self.st.q(
                "SELECT value FROM value_seen WHERE domain=? AND first_seen_sim <= ?",
                (col, base_lo))}
            keys = (set(a) & set(b) & stable) if stable else (set(a) & set(b))
            na = sum(a[k] for k in keys) or 1
            nb = sum(b[k] for k in keys) or 1
            if not keys or nb < 500:
                continue
            val = sum((b[k] / nb - a[k] / na) * math.log(max(b[k] / nb, 1e-4) /
                                                         max(a[k] / na, 1e-4)) for k in keys)
            worst.append((col, val))
        worst = [(c, v) for c, v in worst if v is not None]
        if not worst:
            return
        col, val = max(worst, key=lambda kv: kv[1])
        self.st.put_metric(util.day_str(self.today), "psi", val, col)
        if val > 0.4:
            self.st.set_state("covariate_shift_logged", True)
            self.fire(
                f"feature distribution differs between the early and late periods: PSI("
                f"{col}) = {val:.2f} comparing the last 45 days with days 120-300 ago, which "
                "growth and the annual cycle do not explain (basket value is flat by design)",
                "statistical", "covariate_shift", "medium",
                "model retrained on a recency-weighted window and monitored per period",
                evidence={"feature": col, "psi": round(val, 3),
                          "all": [(c, round(v, 3)) for c, v in worst]})

    def _mnar(self):
        if self.st.get_state("mnar_logged"):
            return
        cutoff = util.day_str(self.today - dt.timedelta(days=config.LABEL_WINDOW_DAYS + 2))
        cols = [("cur_orders", "shipping_speed"), ("cur_orders", "payment_method"),
                ("cur_orders", "channel"), ("cur_orders", "order_total_cents")]
        for table, col in cols:
            rows = self.st.q(
                f"SELECT ({col} IS NULL) AS is_missing, AVG(l.refunded_within_30d) rate, "
                "COUNT(*) n FROM cur_orders o JOIN cur_labels l USING(order_id) "
                "WHERE o.order_day < ? GROUP BY 1", (cutoff,))
            d = {r["is_missing"]: (r["rate"], r["n"]) for r in rows}
            if 1 not in d or 0 not in d:
                continue
            (r1, n1), (r0, n0) = d[1], d[0]
            if n1 < 120 or r0 is None or r1 is None or r0 <= 0:
                continue
            if abs(r1 - r0) / r0 > 0.5:
                self.st.set_state("mnar_logged", True)
                self.fire(
                    f"missingness in {table}.{col} is not at random: refund rate is {r1:.1%} "
                    f"among the {n1} rows where it is NULL against {r0:.1%} elsewhere -- the "
                    "column goes missing as a function of the outcome",
                    "statistical", "mnar_missing", "high",
                    "missingness encoded as an explicit indicator rather than imputed",
                    evidence={"column": f"{table}.{col}", "rate_null": round(r1, 4),
                              "rate_present": round(r0, 4), "n_null": n1, "n_present": n0})
                return

    def _id_reuse(self):
        """A retired identifier now describing a different entity.

        Deliberately narrow.  A restatement legitimately revises attributes of a
        key, and a category rename legitimately rewrites a categorical across the
        whole catalogue; neither is identifier reuse.  What is not legitimate is
        an *immutable* attribute changing: when a customer id acquires both a new
        signup date years away and a new country, when a product code acquires a
        different product name, or when a payment id starts describing a
        different order for a different amount.
        """
        if self.st.get_state("id_reuse_logged"):
            return
        ev = {}
        rows = self.st.q(
            "SELECT customer_id, COUNT(DISTINCT signup_date) nd, COUNT(DISTINCT country) nc, "
            "MIN(signup_date) a, MAX(signup_date) b FROM cur_customers "
            "WHERE signup_date IS NOT NULL AND deleted_sim IS NULL GROUP BY customer_id "
            "HAVING nd > 1 AND nc > 1 LIMIT 8")
        cust = [r for r in rows if util.days_between(r["a"], r["b"]) > 180]
        if cust:
            ev["customers"] = [(r["customer_id"], r["a"], r["b"]) for r in cust[:4]]

        skus = self.st.q(
            "SELECT sku, COUNT(DISTINCT product_name) n FROM src_products "
            "WHERE product_name IS NOT NULL GROUP BY sku HAVING n > 1 LIMIT 4")
        if skus:
            ev["skus"] = [(r["sku"], r["n"]) for r in skus]

        # NB: a payment id re-delivered with a revised *amount* is a restatement
        # of that payment, which the drop channel does routinely.  Only the id
        # pointing at a different order or a different customer is reuse.
        pays = self.st.q(
            "SELECT payment_id, COUNT(DISTINCT order_id) no, COUNT(DISTINCT customer_id) nc "
            "FROM src_payments WHERE payment_id IS NOT NULL AND order_id IS NOT NULL "
            "GROUP BY payment_id HAVING no > 1 OR nc > 1 LIMIT 4")
        if pays:
            ev["payments"] = [(r["payment_id"], r["no"], r["nc"]) for r in pays]

        if not ev:
            return
        self.st.set_state("id_reuse_logged", True)
        self.fire(
            "a retired identifier has been reassigned: the same key now describes a "
            f"different entity, with immutable attributes changing under it ({ev})",
            "integrity", "id_reuse", "medium",
            "keys versioned by first-seen identity so historical facts keep pointing at the "
            "entity they were recorded against",
            evidence=ev)
