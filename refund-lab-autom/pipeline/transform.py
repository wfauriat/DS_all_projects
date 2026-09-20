"""Conformed models over both channels, plus the tests that guard them.

The models are built incrementally: each cycle only rebuilds the order
partitions that actually changed (new rows, restated rows, a re-delivered
payments file), which is what keeps a 2200-day run to a few seconds a day.

Repairs applied here are *conformance*, not concealment -- every one of them is
also reported by a monitor, and ``src_*`` still holds the delivered form:

* latin-1 mojibake in city names is re-encoded;
* a money column that switched unit at a date boundary is put back on one scale;
* a renamed categorical is mapped onto the class it split from;
* near-duplicate orders are marked, not deleted;
* payments are deduplicated on the full row hash.
"""
from __future__ import annotations

import datetime as dt
import json

from . import config, util

log = util.get_logger("transform")

MODELS = """
CREATE TABLE IF NOT EXISTS dim_customer(
  customer_id TEXT NOT NULL, valid_from TEXT NOT NULL, valid_to TEXT,
  signup_date TEXT, country TEXT, city TEXT, city_raw TEXT, segment TEXT,
  lifetime_value_cents INTEGER, is_active INTEGER, is_current INTEGER,
  knowledge_time TEXT, PRIMARY KEY(customer_id, valid_from)) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS ix_dimcust_id ON dim_customer(customer_id);

CREATE TABLE IF NOT EXISTS dim_product(
  sku TEXT PRIMARY KEY, product_name TEXT, category TEXT, category_conformed TEXT,
  list_price_cents INTEGER, active_from TEXT, discontinued_from TEXT,
  is_listed INTEGER, last_file_date TEXT) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS fct_orders(
  order_id TEXT PRIMARY KEY, order_day TEXT, occurred_at TEXT, knowledge_time TEXT,
  customer_id TEXT, country TEXT, segment TEXT, city TEXT,
  channel TEXT, payment_method TEXT, shipping_speed TEXT,
  order_total_cents INTEGER, order_total_eur REAL, n_items INTEGER, n_units INTEGER,
  top_category TEXT, distinct_categories INTEGER,
  paid_ts TEXT, paid_amount_cents INTEGER, processor TEXT, payment_status TEXT,
  settle_lag_days REAL, amount_matches INTEGER,
  is_near_dup INTEGER DEFAULT 0, dup_of TEXT,
  has_label INTEGER DEFAULT 0, refunded INTEGER, label_knowledge_time TEXT,
  window_closed_at TEXT, built_at_sim TEXT) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS ix_fct_orders_day ON fct_orders(order_day);
CREATE INDEX IF NOT EXISTS ix_fct_orders_cust ON fct_orders(customer_id);

CREATE TABLE IF NOT EXISTS fct_payments(
  payment_id TEXT PRIMARY KEY, order_id TEXT, customer_id TEXT, paid_ts TEXT,
  paid_day TEXT, amount_cents INTEGER, status TEXT, processor TEXT,
  billing_city TEXT, first_file_date TEXT, n_deliveries INTEGER,
  settle_lag_days INTEGER, orphan INTEGER) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS ix_fctpay_order ON fct_payments(order_id);

CREATE TABLE IF NOT EXISTS agg_daily(
  day TEXT PRIMARY KEY, orders INTEGER, revenue_cents INTEGER, units INTEGER,
  customers_active INTEGER, events INTEGER, payments INTEGER,
  payments_amount_cents INTEGER, refunds INTEGER, labelled INTEGER,
  api_rows INTEGER, drop_rows INTEGER, built_at_sim TEXT) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS conformance_rules(
  rule TEXT NOT NULL, key TEXT NOT NULL, value TEXT, created_sim TEXT,
  PRIMARY KEY(rule, key)) WITHOUT ROWID;
"""


class Transformer:
    def __init__(self, store):
        self.st = store
        self.st.conn.executescript(MODELS)
        self.st.commit()

    # ----------------------------------------------------- conformance rules
    def rule(self, rule, key, value, sim_now):
        self.st.ex("INSERT OR REPLACE INTO conformance_rules(rule,key,value,created_sim) "
                   "VALUES(?,?,?,?)", (rule, key, util.json_dumps(value), sim_now))

    def rules(self, rule):
        return {r["key"]: util.json_loads(r["value"])
                for r in self.st.q("SELECT key,value FROM conformance_rules WHERE rule=?", (rule,))}

    @staticmethod
    def demojibake(s):
        if not s:
            return s
        try:
            return s.encode("latin-1").decode("utf-8")
        except (UnicodeEncodeError, UnicodeDecodeError):
            return s

    # ------------------------------------------------------------ dimensions
    def build_dim_customer(self, sim_now, changed_ids=None):
        from .normalize import looks_mojibake
        sql = ("SELECT customer_id, valid_from, valid_to, signup_date, country, city, segment,"
               " lifetime_value_cents, is_active, knowledge_time FROM cur_customers"
               " WHERE deleted_sim IS NULL")
        args = []
        if changed_ids:
            ids = sorted(set(changed_ids))
            sql += " AND customer_id IN (" + ",".join("?" * len(ids)) + ")"
            args = ids
        rows = []
        for r in self.st.q(sql, args):
            city = r["city"]
            fixed = self.demojibake(city) if city and looks_mojibake(city) else city
            rows.append((r["customer_id"], r["valid_from"], r["valid_to"], r["signup_date"],
                         r["country"], fixed, city, r["segment"], r["lifetime_value_cents"],
                         r["is_active"], 1 if r["valid_to"] is None else 0, r["knowledge_time"]))
        self.st.exm(
            "INSERT INTO dim_customer(customer_id,valid_from,valid_to,signup_date,country,city,"
            "city_raw,segment,lifetime_value_cents,is_active,is_current,knowledge_time) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(customer_id,valid_from) DO UPDATE SET "
            "valid_to=excluded.valid_to,signup_date=excluded.signup_date,country=excluded.country,"
            "city=excluded.city,city_raw=excluded.city_raw,segment=excluded.segment,"
            "lifetime_value_cents=excluded.lifetime_value_cents,is_active=excluded.is_active,"
            "is_current=excluded.is_current,knowledge_time=excluded.knowledge_time", rows)
        return len(rows)

    def build_dim_product(self, sim_now):
        cat_map = self.rules("category_alias")
        rows = []
        for r in self.st.q("SELECT * FROM cur_products"):
            cat = r["category"]
            rows.append((r["sku"], r["product_name"], cat, cat_map.get(cat, cat),
                         r["list_price_cents"], r["active_from"], r["discontinued_from"],
                         0 if r["discontinued_from"] else 1, r["last_file_date"]))
        self.st.exm(
            "INSERT INTO dim_product(sku,product_name,category,category_conformed,"
            "list_price_cents,active_from,discontinued_from,is_listed,last_file_date) "
            "VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(sku) DO UPDATE SET "
            "product_name=excluded.product_name,category=excluded.category,"
            "category_conformed=excluded.category_conformed,"
            "list_price_cents=excluded.list_price_cents,active_from=excluded.active_from,"
            "discontinued_from=excluded.discontinued_from,is_listed=excluded.is_listed,"
            "last_file_date=excluded.last_file_date", rows)
        return len(rows)

    # ----------------------------------------------------------------- facts
    def build_payments(self, file_dates):
        if not file_dates:
            return 0
        marks = ",".join("?" * len(file_dates))
        rows = self.st.q(
            "SELECT p.*, (SELECT 1 FROM cur_orders o WHERE o.order_id=p.order_id) ok "
            "FROM cur_payments p WHERE p.payment_id IN (SELECT payment_id FROM src_payments "
            f"WHERE file_date IN ({marks}))", list(file_dates))
        out = [(r["payment_id"], r["order_id"], r["customer_id"], r["paid_ts"], r["paid_day"],
                r["amount_cents"], r["status"], r["processor"], r["billing_city"],
                r["first_file_date"], r["n_deliveries"], r["settle_lag_seconds"],
                0 if r["ok"] else 1) for r in rows]
        self.st.exm(
            "INSERT INTO fct_payments(payment_id,order_id,customer_id,paid_ts,paid_day,"
            "amount_cents,status,processor,billing_city,first_file_date,n_deliveries,"
            "settle_lag_days,orphan) VALUES(" + ",".join("?" * 13) + ") "
            "ON CONFLICT(payment_id) DO UPDATE SET amount_cents=excluded.amount_cents,"
            "status=excluded.status,n_deliveries=excluded.n_deliveries,orphan=excluded.orphan",
            out)
        return len(out)

    def build_orders(self, days, sim_now):
        """Rebuild the order fact for a set of order days."""
        if not days:
            return 0
        days = sorted(set(d for d in days if d))
        unit_fix = self.rules("unit_scale")           # day -> divisor
        cat_map = self.rules("category_alias")
        n = 0
        for day in days:
            rows = self.st.q(
                "SELECT o.*, "
                " (SELECT country FROM cur_customers c WHERE c.customer_id=o.customer_id"
                "   AND c.valid_from<=o.occurred_at AND (c.valid_to IS NULL OR c.valid_to>"
                "   o.occurred_at) LIMIT 1) country,"
                " (SELECT segment FROM cur_customers c WHERE c.customer_id=o.customer_id"
                "   AND c.valid_from<=o.occurred_at AND (c.valid_to IS NULL OR c.valid_to>"
                "   o.occurred_at) LIMIT 1) segment,"
                " (SELECT city FROM cur_customers c WHERE c.customer_id=o.customer_id"
                "   AND c.valid_from<=o.occurred_at AND (c.valid_to IS NULL OR c.valid_to>"
                "   o.occurred_at) LIMIT 1) city,"
                " l.refunded_within_30d refunded, l.knowledge_time label_kt "
                "FROM cur_orders o LEFT JOIN cur_labels l USING(order_id) "
                "WHERE o.order_day=? AND o.deleted_sim IS NULL", (day,))
            if not rows:
                continue
            pay = {r["order_id"]: r for r in self.st.q(
                "SELECT p.* FROM cur_payments p JOIN cur_orders o USING(order_id) "
                "WHERE o.order_day=?", (day,))}
            # near-duplicate baskets: same customer, same basket, same total, seconds apart
            buckets = {}
            for r in rows:
                buckets.setdefault((r["customer_id"], r["items_json"], r["order_total_cents"]),
                                   []).append(r)
            dup_of = {}
            for group in buckets.values():
                if len(group) < 2:
                    continue
                group = sorted(group, key=lambda r: r["occurred_at"] or "")
                first = group[0]
                for other in group[1:]:
                    a = util.parse_ts(first["occurred_at"])[0]
                    b = util.parse_ts(other["occurred_at"])[0]
                    if a and b and abs((b - a).total_seconds()) <= 900:
                        dup_of[other["order_id"]] = first["order_id"]

            div = unit_fix.get(day, 1) or 1
            out = []
            for r in rows:
                items = util.json_loads(r["items_json"], []) or []
                cats = [cat_map.get(i.get("category"), i.get("category"))
                        for i in items if isinstance(i, dict)]
                top = max(set(cats), key=cats.count) if cats else None
                p = pay.get(r["order_id"])
                total = r["order_total_cents"]
                total_eur = (total / div / 100.0) if total is not None else None
                lag = None
                if p and p["paid_ts"] and r["occurred_at"]:
                    a = util.parse_ts(r["occurred_at"])[0]
                    b = util.parse_ts(p["paid_ts"])[0]
                    if a and b:
                        lag = round((b - a).total_seconds() / 86400.0, 4)
                closed = None
                occ = util.parse_ts(r["occurred_at"])[0]
                if occ:
                    closed = util.iso(occ + dt.timedelta(days=config.LABEL_WINDOW_DAYS))
                out.append((
                    r["order_id"], day, r["occurred_at"], r["knowledge_time"], r["customer_id"],
                    r["country"], r["segment"], r["city"], r["channel"], r["payment_method"],
                    r["shipping_speed"], total, total_eur, r["n_items"], r["n_units"],
                    top, len(set(cats)),
                    p["paid_ts"] if p else None, p["amount_cents"] if p else None,
                    p["processor"] if p else None, p["status"] if p else None, lag,
                    1 if (p and total is not None and p["amount_cents"] == total) else 0,
                    1 if r["order_id"] in dup_of else 0, dup_of.get(r["order_id"]),
                    1 if r["refunded"] is not None else 0, r["refunded"], r["label_kt"],
                    closed, sim_now))
            self.st.exm(
                "INSERT OR REPLACE INTO fct_orders(order_id,order_day,occurred_at,knowledge_time,"
                "customer_id,country,segment,city,channel,payment_method,shipping_speed,"
                "order_total_cents,order_total_eur,n_items,n_units,top_category,"
                "distinct_categories,paid_ts,paid_amount_cents,processor,payment_status,"
                "settle_lag_days,amount_matches,is_near_dup,dup_of,has_label,refunded,"
                "label_knowledge_time,window_closed_at,built_at_sim) "
                "VALUES(" + ",".join("?" * 30) + ")", out)
            n += len(out)
        return n

    def build_agg(self, days, sim_now):
        for day in sorted(set(d for d in days if d)):
            o = self.st.q("SELECT COUNT(*) n, SUM(order_total_cents) rev, SUM(n_units) u, "
                          "COUNT(DISTINCT customer_id) c, SUM(has_label) lab, "
                          "SUM(COALESCE(refunded,0)) ref FROM fct_orders WHERE order_day=?",
                          (day,))[0]
            e = self.st.q1("SELECT COUNT(*) FROM cur_events WHERE event_day=?", (day,)) or 0
            p = self.st.q("SELECT COUNT(*) n, SUM(amount_cents) a FROM fct_payments "
                          "WHERE first_file_date=?", (day,))[0]
            self.st.ex(
                "INSERT OR REPLACE INTO agg_daily(day,orders,revenue_cents,units,"
                "customers_active,events,payments,payments_amount_cents,refunds,labelled,"
                "api_rows,drop_rows,built_at_sim) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (day, o["n"], o["rev"], o["u"], o["c"], e, p["n"], p["a"], o["ref"], o["lab"],
                 (o["n"] or 0) + e, p["n"] or 0, sim_now))

    # ----------------------------------------------------------------- tests
    def test(self, cycle_id, sim_now, days, full=False):
        """Data tests.  Partition-scoped every cycle, whole-table periodically."""
        res = []

        def check(name, ok, observed=None, detail=""):
            res.append((name, "pass" if ok else "fail", observed, detail))
            self.st.test_result(cycle_id, sim_now, name, "pass" if ok else "fail",
                                observed, detail)

        days = sorted(set(d for d in days if d))
        marks = ",".join("?" * len(days)) if days else "''"
        if days:
            n = self.st.q1(f"SELECT COUNT(*) FROM fct_orders WHERE order_day IN ({marks}) "
                           "AND (order_id IS NULL OR customer_id IS NULL OR occurred_at IS NULL)",
                           days) or 0
            check("fct_orders.not_null_keys", n == 0, n, f"days={days[:3]}")

            n = self.st.q1(f"SELECT COUNT(*) FROM fct_orders WHERE order_day IN ({marks}) "
                           "AND order_total_cents IS NULL", days) or 0
            tot = self.st.q1(f"SELECT COUNT(*) FROM fct_orders WHERE order_day IN ({marks})",
                             days) or 1
            check("fct_orders.total_present", n / tot <= 0.02, n / tot)

            bad = self.st.q1(
                f"SELECT COUNT(*) FROM fct_orders WHERE order_day IN ({marks}) AND channel IS NOT"
                " NULL AND channel NOT IN ('web','mobile','partner_api')", days) or 0
            check("fct_orders.channel_domain", bad == 0, bad)

            bad = self.st.q1(
                f"SELECT COUNT(*) FROM fct_orders WHERE order_day IN ({marks}) AND payment_method"
                " IS NOT NULL AND payment_method NOT IN ('card','wallet','bank_transfer',"
                "'voucher')", days) or 0
            check("fct_orders.method_domain", bad == 0, bad)

            orph = self.st.q1(f"SELECT COUNT(*) FROM fct_orders WHERE order_day IN ({marks}) "
                              "AND country IS NULL", days) or 0
            check("fct_orders.customer_join", orph / tot <= 0.02, orph / tot,
                  "orders with no customer dimension row at order time")

            neg = self.st.q1(f"SELECT COUNT(*) FROM fct_orders WHERE order_day IN ({marks}) "
                             "AND order_total_cents < 0", days) or 0
            check("fct_orders.total_non_negative", neg == 0, neg)

            fut = self.st.q1(f"SELECT COUNT(*) FROM fct_orders WHERE order_day IN ({marks}) "
                             "AND knowledge_time > ?", days + [sim_now]) or 0
            check("fct_orders.knowledge_not_future", fut == 0, fut)

        if full:
            dup = self.st.q1("SELECT COUNT(*) FROM (SELECT payment_id FROM fct_payments "
                             "GROUP BY payment_id HAVING COUNT(*)>1)") or 0
            check("fct_payments.unique_payment_id", dup == 0, dup)
            orph = self.st.q1("SELECT COUNT(*) FROM fct_payments WHERE orphan=1") or 0
            npay = self.st.q1("SELECT COUNT(*) FROM fct_payments") or 1
            check("fct_payments.order_exists", orph / npay <= 0.05, orph / npay)
            ov = self.st.q1(
                "SELECT COUNT(*) FROM dim_customer a JOIN dim_customer b "
                "ON a.customer_id=b.customer_id AND a.valid_from<b.valid_from "
                "WHERE a.valid_to IS NOT NULL AND a.valid_to > b.valid_from") or 0
            check("dim_customer.scd2_no_overlap", ov == 0, ov)
            lab = self.st.q1("SELECT COUNT(*) FROM fct_orders WHERE has_label=1 "
                             "AND refunded IS NULL") or 0
            check("fct_orders.label_is_boolean", lab == 0, lab)
        return res

    def freshness(self, sim_now):
        m = self.st.q1("SELECT MAX(order_day) FROM fct_orders")
        return util.days_between(m, util.to_day(sim_now)) if m else None
