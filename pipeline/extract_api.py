"""API extraction: land raw, load bitemporally, reconcile counts, audit history.

The extraction strategy has four parts and each exists for a reason:

*rolling*      re-pull the last few valid-time days every cycle.  Cheap, and it
               is how ordinary late arrivals and fresh restatements show up.
*reconcile*    ask the source how many rows it thinks exist in a few coarse
               time buckets and compare with our own count.  A mismatch is
               bisected down to the day, then that day is re-pulled.  This is
               how rows appearing far in the past (late partitions, backfills)
               and rows *disappearing* (silent deletion) are found without
               re-reading the whole history every hour.
*audit*        a rotating deep re-pull of individual historical days, comparing
               payload hashes.  Catches restatements that do not change any
               count and so are invisible to reconciliation.
*bootstrap*    on the very first cycle, walk the whole history once.  This is
               the "full extraction of every endpoint" path.
"""
from __future__ import annotations

import datetime as dt
import gzip
import json
import time

from . import config, normalize, util
from .normalize import NormStats

log = util.get_logger("extract_api")

INSERT_SQL = {
    "orders": ("INSERT OR IGNORE INTO src_orders(order_id,version,knowledge_time,occurred_at,"
               "customer_id,channel,payment_method,shipping_speed,order_total_cents,items_json,"
               "n_items,n_units,extra_json,ts_had_offset,first_seen_sim,cycle_id,row_hash)"
               " VALUES(" + ",".join("?" * 17) + ")"),
    "customers": ("INSERT OR IGNORE INTO src_customers(customer_id,valid_from,version,valid_to,"
                  "signup_date,country,city,segment,lifetime_value_cents,is_active,"
                  "knowledge_time,extra_json,ts_had_offset,first_seen_sim,cycle_id,row_hash)"
                  " VALUES(" + ",".join("?" * 16) + ")"),
    "events": ("INSERT OR IGNORE INTO src_events(event_id,version,customer_id,session_id,"
               "occurred_at,event_type,sku,knowledge_time,extra_json,ts_had_offset,"
               "first_seen_sim,cycle_id,row_hash) VALUES(" + ",".join("?" * 13) + ")"),
    "labels": ("INSERT OR IGNORE INTO src_labels(order_id,version,resolved_at,refund_at,"
               "refunded_within_30d,knowledge_time,extra_json,ts_had_offset,first_seen_sim,"
               "cycle_id,row_hash) VALUES(" + ",".join("?" * 11) + ")"),
}

# (table, key columns, index of version in the src tuple)
KEYSPEC = {
    "orders":    ("cur_orders", ["order_id"], 1),
    "customers": ("cur_customers", ["customer_id", "valid_from"], 2),
    "events":    ("cur_events", ["event_id"], 1),
    "labels":    ("cur_labels", ["order_id"], 1),
}

VALID_COL = {"orders": "occurred_at", "customers": "valid_from",
             "events": "occurred_at", "labels": "resolved_at"}
DAY_COL = {"orders": "order_day", "events": "event_day"}


class ApiExtractor:
    def __init__(self, store, client):
        self.st = store
        self.cl = client

    # ------------------------------------------------------------- landing
    def land(self, entity, sim_now, purpose, rows, meta):
        """Append the raw records to the landing zone, exactly as received."""
        if not rows:
            return
        d = config.LANDING_API / entity
        d.mkdir(parents=True, exist_ok=True)
        path = d / f"{sim_now[:10]}.jsonl.gz"
        with gzip.open(path, "at", compresslevel=1, encoding="utf-8") as fh:
            fh.write(json.dumps({"_pull": {"sim_now": sim_now, "purpose": purpose, **meta}},
                                ensure_ascii=False, default=str) + "\n")
            for r in rows:
                fh.write(json.dumps(r, ensure_ascii=False, default=str) + "\n")

    # ---------------------------------------------------------------- load
    def load(self, entity, raw_rows, sim_now, cycle_id, stats: NormStats):
        """Normalise, classify against current belief, then write both layers.

        Returns a dict describing what was new and what was a restatement --
        which is the raw material for half the monitors.
        """
        if not raw_rows:
            return {"new": 0, "restated": [], "new_rows": [], "unchanged": 0,
                    "days": set(), "rows": []}
        table, keycols, vidx = KEYSPEC[entity]
        tuples = [normalize.normalize(entity, r, stats, sim_now, cycle_id)[1] for r in raw_rows]

        # positions of key columns inside the src tuple
        kpos = {"orders": [0], "customers": [0, 1], "events": [0], "labels": [0]}[entity]

        known = {}
        keys = [tuple(t[i] for i in kpos) for t in tuples]
        for i in range(0, len(keys), 400):
            chunk = keys[i:i + 400]
            clause = " OR ".join(["(" + " AND ".join(f"{c}=?" for c in keycols) + ")"] * len(chunk))
            args = [v for k in chunk for v in k]
            sql = f"SELECT {','.join(keycols)}, version FROM {table} WHERE {clause}"
            for row in self.st.q(sql, args):
                known[tuple(row[c] for c in keycols)] = row["version"]

        new, restated, unchanged, days = [], [], 0, set()
        vcol_idx = {"orders": 3, "customers": 1, "events": 4, "labels": 2}[entity]
        for t, k in zip(tuples, keys):
            ver = t[vidx]
            prev = known.get(k)
            vt = t[vcol_idx]
            if vt:
                days.add(vt[:10])
            if prev is None:
                new.append(t)
            elif ver > prev:
                restated.append((k, prev, ver, t))
                new.append(t)
            else:
                unchanged += 1

        self.st.exm(INSERT_SQL[entity], tuples)
        self.upsert_current(entity, [t for t in tuples])
        return {"new": len(new) - len(restated), "restated": restated, "new_rows": new,
                "unchanged": unchanged, "days": days, "rows": tuples}

    def upsert_current(self, entity, tuples):
        if not tuples:
            return
        if entity == "orders":
            self.st.exm(
                "INSERT INTO cur_orders(order_id,version,knowledge_time,occurred_at,order_day,"
                "customer_id,channel,payment_method,shipping_speed,order_total_cents,items_json,"
                "n_items,n_units,extra_json,first_seen_sim) VALUES(?,?,?,?,substr(?,1,10),"
                "?,?,?,?,?,?,?,?,?,?) ON CONFLICT(order_id) DO UPDATE SET "
                "version=excluded.version,knowledge_time=excluded.knowledge_time,"
                "occurred_at=excluded.occurred_at,order_day=excluded.order_day,"
                "customer_id=excluded.customer_id,channel=excluded.channel,"
                "payment_method=excluded.payment_method,shipping_speed=excluded.shipping_speed,"
                "order_total_cents=excluded.order_total_cents,items_json=excluded.items_json,"
                "n_items=excluded.n_items,n_units=excluded.n_units,extra_json=excluded.extra_json,"
                "restated=cur_orders.restated+1,deleted_sim=NULL "
                "WHERE excluded.version > cur_orders.version",
                [(t[0], t[1], t[2], t[3], t[3], t[4], t[5], t[6], t[7], t[8], t[9], t[10],
                  t[11], t[12], t[14]) for t in tuples])
        elif entity == "customers":
            self.st.exm(
                "INSERT INTO cur_customers(customer_id,valid_from,version,valid_to,signup_date,"
                "country,city,segment,lifetime_value_cents,is_active,knowledge_time,extra_json,"
                "first_seen_sim) VALUES(" + ",".join("?" * 13) + ") "
                "ON CONFLICT(customer_id,valid_from) DO UPDATE SET version=excluded.version,"
                "valid_to=excluded.valid_to,signup_date=excluded.signup_date,"
                "country=excluded.country,city=excluded.city,segment=excluded.segment,"
                "lifetime_value_cents=excluded.lifetime_value_cents,is_active=excluded.is_active,"
                "knowledge_time=excluded.knowledge_time,extra_json=excluded.extra_json,"
                "restated=cur_customers.restated+1,deleted_sim=NULL "
                "WHERE excluded.version > cur_customers.version",
                [(t[0], t[1], t[2], t[3], t[4], t[5], t[6], t[7], t[8], t[9], t[10], t[11],
                  t[13]) for t in tuples])
        elif entity == "events":
            rows = []
            for t in tuples:
                occ, kt = t[4], t[7]
                lag = None
                a, b = util.parse_ts(occ)[0], util.parse_ts(kt)[0]
                if a and b:
                    lag = int((b - a).total_seconds())
                rows.append((t[0], t[1], t[2], t[3], occ, occ, t[5], t[6], kt, lag, t[8], t[10]))
            self.st.exm(
                "INSERT INTO cur_events(event_id,version,customer_id,session_id,occurred_at,"
                "event_day,event_type,sku,knowledge_time,lag_seconds,extra_json,first_seen_sim) "
                "VALUES(?,?,?,?,?,substr(?,1,10),?,?,?,?,?,?) ON CONFLICT(event_id) DO UPDATE SET "
                "version=excluded.version,customer_id=excluded.customer_id,"
                "session_id=excluded.session_id,occurred_at=excluded.occurred_at,"
                "event_day=excluded.event_day,event_type=excluded.event_type,sku=excluded.sku,"
                "knowledge_time=excluded.knowledge_time,lag_seconds=excluded.lag_seconds,"
                "extra_json=excluded.extra_json WHERE excluded.version > cur_events.version",
                rows)
        elif entity == "labels":
            self.st.exm(
                "INSERT INTO cur_labels(order_id,version,resolved_at,refund_at,"
                "refunded_within_30d,knowledge_time,extra_json,first_seen_sim) "
                "VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(order_id) DO UPDATE SET "
                "version=excluded.version,resolved_at=excluded.resolved_at,"
                "refund_at=excluded.refund_at,refunded_within_30d=excluded.refunded_within_30d,"
                "knowledge_time=excluded.knowledge_time,extra_json=excluded.extra_json,"
                "restated=cur_labels.restated+1 WHERE excluded.version > cur_labels.version",
                [(t[0], t[1], t[2], t[3], t[4], t[5], t[6], t[8]) for t in tuples])

    # ----------------------------------------------------------- day counts
    def local_count(self, entity, day):
        if entity == "orders":
            return self.st.q1("SELECT COUNT(*) FROM cur_orders WHERE order_day=? "
                              "AND deleted_sim IS NULL", (day,)) or 0
        if entity == "events":
            return self.st.q1("SELECT COUNT(*) FROM cur_events WHERE event_day=? "
                              "AND deleted_sim IS NULL", (day,)) or 0
        if entity == "customers":
            return self.st.q1("SELECT COUNT(*) FROM cur_customers WHERE substr(valid_from,1,10)=? "
                              "AND deleted_sim IS NULL", (day,)) or 0
        return self.st.q1("SELECT COUNT(*) FROM cur_labels WHERE substr(resolved_at,1,10)=? "
                          "AND deleted_sim IS NULL", (day,)) or 0

    def refresh_day_counts(self, entity, days):
        rows = [(entity, d, self.local_count(entity, d)) for d in days if d]
        self.st.exm("INSERT INTO day_counts(entity,day,n_local) VALUES(?,?,?) "
                    "ON CONFLICT(entity,day) DO UPDATE SET n_local=excluded.n_local", rows)

    def local_range_count(self, entity, d0, d1):
        """Rows we currently believe in, for valid days in [d0, d1)."""
        col = {"orders": "order_day", "events": "event_day"}.get(entity)
        if col:
            return self.st.q1(f"SELECT COUNT(*) FROM cur_{entity} WHERE {col}>=? AND {col}<? "
                              "AND deleted_sim IS NULL", (d0, d1)) or 0
        if entity == "customers":
            return self.st.q1("SELECT COUNT(*) FROM cur_customers WHERE substr(valid_from,1,10)>=? "
                              "AND substr(valid_from,1,10)<? AND deleted_sim IS NULL",
                              (d0, d1)) or 0
        return self.st.q1("SELECT COUNT(*) FROM cur_labels WHERE substr(resolved_at,1,10)>=? "
                          "AND substr(resolved_at,1,10)<? AND deleted_sim IS NULL",
                          (d0, d1)) or 0

    # ------------------------------------------------------------- pulling
    def pull_window(self, entity, d0, d1, sim_now, cycle_id, purpose, stats):
        """Pull one valid-time window and load it.  `d0`/`d1` are date strings."""
        t0 = time.time()
        rows, meta = self.cl.pull(entity, since=f"{d0}T00:00:00", until=f"{d1}T00:00:00",
                                  as_of=sim_now)
        self.land(entity, sim_now, purpose, rows, meta)
        res = self.load(entity, rows, sim_now, cycle_id, stats)
        seen_fields = set()
        for r in rows[:200]:
            seen_fields |= set(r.keys())
        if seen_fields:
            self.st.note_fields(entity, seen_fields, sim_now)
        self.st.ex("INSERT INTO pull_log(cycle_id,sim_now,entity,purpose,since,until,as_of,"
                   "pages,rows,new_rows,reported_total,seconds) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                   (cycle_id, sim_now, entity, purpose, d0, d1, sim_now, meta["pages"],
                    meta["rows"], res["new"], meta["reported_total"], round(time.time() - t0, 3)))
        res["meta"] = meta
        res["raw"] = rows
        return res
