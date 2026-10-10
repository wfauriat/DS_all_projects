"""The warehouse: a bitemporal store on SQLite.

Two layers, both in one file:

``src_*``  append-only.  One row per (business key, version) ever served, with
           the source's own ``knowledge_time`` and our ``first_seen_sim``.
           Nothing is ever updated or deleted here, so the state of belief on
           any past simulated date is a ``WHERE knowledge_time <= X`` away.

``cur_*``  current belief.  Maintained incrementally as versions arrive, so the
           conformed models never have to scan history to find "the latest one".

The drop channel gets the same treatment: ``src_payments`` keeps every row of
every file exactly as delivered (including duplicates, which are evidence),
while ``cur_payments`` is the deduplicated truth.
"""
from __future__ import annotations

import json
import sqlite3

from . import config, util

log = util.get_logger("storage")

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=OFF;
PRAGMA temp_store=MEMORY;
PRAGMA cache_size=-120000;

-- ------------------------------------------------------------ API: raw
CREATE TABLE IF NOT EXISTS src_orders(
  order_id TEXT NOT NULL, version INTEGER NOT NULL,
  knowledge_time TEXT, occurred_at TEXT, customer_id TEXT,
  channel TEXT, payment_method TEXT, shipping_speed TEXT,
  order_total_cents INTEGER, items_json TEXT, n_items INTEGER, n_units INTEGER,
  extra_json TEXT, ts_had_offset INTEGER DEFAULT 0,
  first_seen_sim TEXT, cycle_id INTEGER, row_hash TEXT,
  PRIMARY KEY(order_id, version)) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS ix_src_orders_kt ON src_orders(knowledge_time);
CREATE INDEX IF NOT EXISTS ix_src_orders_day ON src_orders(substr(occurred_at,1,10));

CREATE TABLE IF NOT EXISTS src_customers(
  customer_id TEXT NOT NULL, valid_from TEXT NOT NULL, version INTEGER NOT NULL,
  valid_to TEXT, signup_date TEXT, country TEXT, city TEXT, segment TEXT,
  lifetime_value_cents INTEGER, is_active INTEGER, knowledge_time TEXT,
  extra_json TEXT, ts_had_offset INTEGER DEFAULT 0,
  first_seen_sim TEXT, cycle_id INTEGER, row_hash TEXT,
  PRIMARY KEY(customer_id, valid_from, version)) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS ix_src_cust_kt ON src_customers(knowledge_time);

CREATE TABLE IF NOT EXISTS src_events(
  event_id TEXT NOT NULL, version INTEGER NOT NULL,
  customer_id TEXT, session_id TEXT, occurred_at TEXT, event_type TEXT, sku TEXT,
  knowledge_time TEXT, extra_json TEXT, ts_had_offset INTEGER DEFAULT 0,
  first_seen_sim TEXT, cycle_id INTEGER, row_hash TEXT,
  PRIMARY KEY(event_id, version)) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS ix_src_events_day ON src_events(substr(occurred_at,1,10));

CREATE TABLE IF NOT EXISTS src_labels(
  order_id TEXT NOT NULL, version INTEGER NOT NULL,
  resolved_at TEXT, refund_at TEXT, refunded_within_30d INTEGER,
  knowledge_time TEXT, extra_json TEXT, ts_had_offset INTEGER DEFAULT 0,
  first_seen_sim TEXT, cycle_id INTEGER, row_hash TEXT,
  PRIMARY KEY(order_id, version)) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS ix_src_labels_kt ON src_labels(knowledge_time);

-- ------------------------------------------------------------ API: current
CREATE TABLE IF NOT EXISTS cur_orders(
  order_id TEXT PRIMARY KEY, version INTEGER, knowledge_time TEXT, occurred_at TEXT,
  order_day TEXT, customer_id TEXT, channel TEXT, payment_method TEXT,
  shipping_speed TEXT, order_total_cents INTEGER, items_json TEXT,
  n_items INTEGER, n_units INTEGER, extra_json TEXT, first_seen_sim TEXT,
  restated INTEGER DEFAULT 0, deleted_sim TEXT) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS ix_cur_orders_day ON cur_orders(order_day);
CREATE INDEX IF NOT EXISTS ix_cur_orders_cust ON cur_orders(customer_id);

CREATE TABLE IF NOT EXISTS cur_customers(
  customer_id TEXT NOT NULL, valid_from TEXT NOT NULL, version INTEGER,
  valid_to TEXT, signup_date TEXT, country TEXT, city TEXT, segment TEXT,
  lifetime_value_cents INTEGER, is_active INTEGER, knowledge_time TEXT,
  extra_json TEXT, first_seen_sim TEXT, restated INTEGER DEFAULT 0,
  deleted_sim TEXT,
  PRIMARY KEY(customer_id, valid_from)) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS cur_events(
  event_id TEXT PRIMARY KEY, version INTEGER, customer_id TEXT, session_id TEXT,
  occurred_at TEXT, event_day TEXT, event_type TEXT, sku TEXT, knowledge_time TEXT,
  lag_seconds INTEGER, extra_json TEXT, first_seen_sim TEXT,
  deleted_sim TEXT) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS ix_cur_events_day ON cur_events(event_day);
CREATE INDEX IF NOT EXISTS ix_cur_events_cust ON cur_events(customer_id);

CREATE TABLE IF NOT EXISTS cur_labels(
  order_id TEXT PRIMARY KEY, version INTEGER, resolved_at TEXT, refund_at TEXT,
  refunded_within_30d INTEGER, knowledge_time TEXT, extra_json TEXT,
  first_seen_sim TEXT, restated INTEGER DEFAULT 0, deleted_sim TEXT) WITHOUT ROWID;

-- ------------------------------------------------------------ drop channel
CREATE TABLE IF NOT EXISTS drop_files(
  file_name TEXT PRIMARY KEY, kind TEXT, file_date TEXT, sha256 TEXT,
  n_bytes INTEGER, n_rows INTEGER, encoding TEXT, compressed INTEGER,
  header_json TEXT, truncated INTEGER DEFAULT 0, parse_errors INTEGER DEFAULT 0,
  first_seen_sim TEXT, first_seen_cycle INTEGER, lag_days INTEGER,
  notes TEXT) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS expected_drop(
  kind TEXT NOT NULL, file_date TEXT NOT NULL, due_sim TEXT, due_cycle INTEGER,
  arrived_sim TEXT, arrived_cycle INTEGER, status TEXT DEFAULT 'pending',
  PRIMARY KEY(kind, file_date)) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS src_payments(
  file_name TEXT NOT NULL, row_no INTEGER NOT NULL, file_date TEXT,
  payment_id TEXT, order_id TEXT, customer_id TEXT, paid_ts TEXT,
  amount_cents INTEGER, status TEXT, processor TEXT, billing_city TEXT,
  extra_json TEXT, row_hash TEXT, first_seen_sim TEXT,
  PRIMARY KEY(file_name, row_no)) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS ix_src_pay_id ON src_payments(payment_id);
CREATE INDEX IF NOT EXISTS ix_src_pay_hash ON src_payments(row_hash);

CREATE TABLE IF NOT EXISTS cur_payments(
  payment_id TEXT PRIMARY KEY, order_id TEXT, customer_id TEXT, paid_ts TEXT,
  paid_day TEXT, amount_cents INTEGER, status TEXT, processor TEXT,
  billing_city TEXT, first_file_date TEXT, last_file_date TEXT,
  n_deliveries INTEGER DEFAULT 1, n_revisions INTEGER DEFAULT 0,
  settle_lag_seconds INTEGER, first_seen_sim TEXT) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS ix_cur_pay_order ON cur_payments(order_id);
CREATE INDEX IF NOT EXISTS ix_cur_pay_day ON cur_payments(paid_day);

CREATE TABLE IF NOT EXISTS src_products(
  sku TEXT NOT NULL, row_hash TEXT NOT NULL, product_name TEXT, category TEXT,
  list_price_cents INTEGER, active_from TEXT, discontinued_from TEXT,
  extra_json TEXT, first_file_date TEXT, last_file_date TEXT,
  PRIMARY KEY(sku, row_hash)) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS cur_products(
  sku TEXT PRIMARY KEY, product_name TEXT, category TEXT, list_price_cents INTEGER,
  active_from TEXT, discontinued_from TEXT, last_file_date TEXT,
  first_file_date TEXT, n_changes INTEGER DEFAULT 0, missing_since TEXT) WITHOUT ROWID;

-- ------------------------------------------------------------ operations
CREATE TABLE IF NOT EXISTS pull_log(
  cycle_id INTEGER, sim_now TEXT, entity TEXT, purpose TEXT, since TEXT, until TEXT,
  as_of TEXT, pages INTEGER, rows INTEGER, new_rows INTEGER, reported_total INTEGER,
  seconds REAL);
CREATE INDEX IF NOT EXISTS ix_pull_cycle ON pull_log(cycle_id);

CREATE TABLE IF NOT EXISTS day_counts(
  entity TEXT NOT NULL, day TEXT NOT NULL, n_local INTEGER DEFAULT 0,
  src_count INTEGER, src_checked_sim TEXT, src_checked_cycle INTEGER,
  PRIMARY KEY(entity, day)) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS metrics(
  sim_day TEXT NOT NULL, metric TEXT NOT NULL, key TEXT NOT NULL DEFAULT '',
  value REAL, PRIMARY KEY(sim_day, metric, key)) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS ix_metrics_metric ON metrics(metric, sim_day);

CREATE TABLE IF NOT EXISTS field_seen(
  entity TEXT NOT NULL, field TEXT NOT NULL, first_seen_sim TEXT,
  last_seen_sim TEXT, n_seen INTEGER DEFAULT 0,
  PRIMARY KEY(entity, field)) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS value_seen(
  domain TEXT NOT NULL, value TEXT NOT NULL, first_seen_sim TEXT,
  last_seen_sim TEXT, n_seen INTEGER DEFAULT 0,
  PRIMARY KEY(domain, value)) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS alerts(
  id INTEGER PRIMARY KEY AUTOINCREMENT, cycle_id INTEGER, sim_now TEXT,
  code TEXT, severity TEXT, detail TEXT);

CREATE TABLE IF NOT EXISTS test_results(
  cycle_id INTEGER, sim_now TEXT, test_name TEXT, status TEXT,
  observed REAL, detail TEXT);
CREATE INDEX IF NOT EXISTS ix_tests_cycle ON test_results(cycle_id);

CREATE TABLE IF NOT EXISTS incident_log(
  dedupe_key TEXT PRIMARY KEY, suspected_fault TEXT, suspected_family TEXT,
  detected_at_sim TEXT, n_repeats INTEGER DEFAULT 1) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS predictions(
  order_id TEXT NOT NULL, model_version TEXT NOT NULL, scored_at_sim TEXT,
  p_refund REAL, PRIMARY KEY(order_id, model_version)) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS model_runs(
  model_version TEXT PRIMARY KEY, trained_at_sim TEXT, n_train INTEGER,
  n_features INTEGER, auc_holdout REAL, base_rate REAL, notes TEXT) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS state(k TEXT PRIMARY KEY, v TEXT) WITHOUT ROWID;
"""


class Store:
    def __init__(self, path=None):
        self.path = str(path or config.DB_PATH)
        self.conn = sqlite3.connect(self.path, timeout=60)
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(SCHEMA)
        for ddl in ("ALTER TABLE cur_payments ADD COLUMN last_file_date TEXT",
                    "ALTER TABLE cur_payments ADD COLUMN n_revisions INTEGER DEFAULT 0"):
            try:
                self.conn.execute(ddl)
            except sqlite3.OperationalError:
                pass                      # already present
        self.conn.commit()

    # ------------------------------------------------------------- basics
    def q(self, sql, args=()):
        return self.conn.execute(sql, args).fetchall()

    def q1(self, sql, args=()):
        r = self.conn.execute(sql, args).fetchone()
        return r[0] if r else None

    def ex(self, sql, args=()):
        return self.conn.execute(sql, args)

    def exm(self, sql, rows):
        if rows:
            self.conn.executemany(sql, rows)

    def commit(self):
        self.conn.commit()

    # -------------------------------------------------------------- state
    def get_state(self, k, default=None):
        v = self.q1("SELECT v FROM state WHERE k=?", (k,))
        return json.loads(v) if v is not None else default

    def set_state(self, k, v):
        self.ex("INSERT INTO state(k,v) VALUES(?,?) "
                "ON CONFLICT(k) DO UPDATE SET v=excluded.v", (k, json.dumps(v)))

    # ------------------------------------------------------------ metrics
    def put_metric(self, sim_day, metric, value, key=""):
        self.ex("INSERT INTO metrics(sim_day,metric,key,value) VALUES(?,?,?,?) "
                "ON CONFLICT(sim_day,metric,key) DO UPDATE SET value=excluded.value",
                (sim_day, metric, key, float(value)))

    def metric_series(self, metric, key="", limit=400):
        return [(r["sim_day"], r["value"]) for r in self.q(
            "SELECT sim_day, value FROM metrics WHERE metric=? AND key=? "
            "ORDER BY sim_day DESC LIMIT ?", (metric, key, limit))][::-1]

    # ------------------------------------------------------------- alerts
    def alert(self, cycle_id, sim_now, code, detail, severity="warn"):
        self.ex("INSERT INTO alerts(cycle_id,sim_now,code,severity,detail) VALUES(?,?,?,?,?)",
                (cycle_id, sim_now, code, severity,
                 detail if isinstance(detail, str) else json.dumps(detail, default=str)))

    def test_result(self, cycle_id, sim_now, name, status, observed=None, detail=""):
        self.ex("INSERT INTO test_results(cycle_id,sim_now,test_name,status,observed,detail) "
                "VALUES(?,?,?,?,?,?)", (cycle_id, sim_now, name, status, observed, detail))

    # ------------------------------------------------ schema / value drift
    def note_fields(self, entity, fields, sim_now):
        rows = [(entity, f, sim_now, sim_now, 1) for f in fields]
        self.exm("INSERT INTO field_seen(entity,field,first_seen_sim,last_seen_sim,n_seen) "
                 "VALUES(?,?,?,?,?) ON CONFLICT(entity,field) DO UPDATE SET "
                 "last_seen_sim=excluded.last_seen_sim, n_seen=n_seen+1", rows)

    def note_values(self, domain, counter, sim_now):
        rows = [(domain, str(v), sim_now, sim_now, int(n)) for v, n in counter.items()
                if v is not None]
        self.exm("INSERT INTO value_seen(domain,value,first_seen_sim,last_seen_sim,n_seen) "
                 "VALUES(?,?,?,?,?) ON CONFLICT(domain,value) DO UPDATE SET "
                 "last_seen_sim=excluded.last_seen_sim, n_seen=n_seen+excluded.n_seen", rows)

    # ------------------------------------------------- bitemporal reads
    def belief_as_of(self, entity, as_of, key=None, day=None):
        """What the source told us about `entity` as of simulated time `as_of`.

        Reads only ``src_*`` (append-only), picking the highest version whose
        ``knowledge_time`` is at or before `as_of`.  This is the query that makes
        "what did the source say on date X" answerable, and different from today.
        """
        keycols = {"orders": ["order_id"], "events": ["event_id"], "labels": ["order_id"],
                   "customers": ["customer_id", "valid_from"]}[entity]
        table = f"src_{entity}"
        kc = ", ".join(keycols)
        where, args = ["knowledge_time <= ?"], [as_of]
        if key is not None:
            if isinstance(key, (list, tuple)):
                where += [f"{c} = ?" for c in keycols]
                args += list(key)
            else:
                where.append(f"{keycols[0]} = ?")
                args.append(key)
        if day is not None:
            col = {"orders": "occurred_at", "events": "occurred_at",
                   "labels": "resolved_at", "customers": "valid_from"}[entity]
            where.append(f"substr({col},1,10) = ?")
            args.append(day)
        sql = (f"SELECT * FROM {table} t WHERE {' AND '.join(where)} AND version = "
               f"(SELECT MAX(version) FROM {table} u WHERE "
               + " AND ".join(f"u.{c}=t.{c}" for c in keycols)
               + " AND u.knowledge_time <= ?)")
        return self.q(sql, tuple(args) + (as_of,))

    def close(self):
        self.conn.commit()
        self.conn.close()
