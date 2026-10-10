import datetime
import sqlite3
import json
import logging

from .utils import content_hash
from .config import LANDING_DB
from .schema import (LEDGER_COLS, SCHEMA_LEDGER,
                     ORDERS_COLS, SCHEMA_ORDERS_RAW,
                     CUSTOMERS_COLS, SCHEMA_CUSTOMERS_RAW)


logger = logging.getLogger(__name__)

def init_db() -> sqlite3.Connection:
    conn = sqlite3.connect(LANDING_DB)
    conn.execute("PRAGMA journal_mode=WAL")
    # conn.execute("DROP TABLE IF EXISTS page_ledger") # Temp for dev rerun
    # conn.execute("DROP TABLE IF EXISTS orders_raw") # Temp for dev rerun
    conn.executescript(SCHEMA_LEDGER)
    conn.executescript(SCHEMA_ORDERS_RAW)
    conn.executescript(SCHEMA_CUSTOMERS_RAW)
    create_stg_orders(conn)
    create_stg_customers(conn)
    create_orders_current(conn)
    create_customers_current(conn)
    conn.commit()
    return conn


def complete_page(conn: sqlite3.Connection,
                  payload: dict, entity: str,
                  cursor: str | None, page: int,
                  as_of_demand: str | None,
                  since: str | None = None,
                  until: str | None = None):
    conn.execute((f"INSERT OR REPLACE INTO page_ledger ("
                  f"{', '.join(LEDGER_COLS[1:])}) "
                  f"VALUES ({', '.join('?' for _ in LEDGER_COLS[1:])});"),
            (entity, page, datetime.datetime.now().isoformat(),
             content_hash(payload),
            "succeeded", as_of_demand, payload["as_of"], since , until,
            cursor, payload["next_cursor"]))
    conn.commit()


LANDING_META = {"id", "received_at", "source_cursor", "source_page"}

def write_landing_entry(entry: dict, conn: sqlite3.Connection,
                         table: str, cols: tuple[str, ...],
                         datepage, cursor, page,
                         json_fields: frozenset[str] = frozenset()) -> bool:
    values = {}
    for col in cols:
        if col in ("content_hash", *LANDING_META):
            continue
        values[col] = json.dumps(entry[col]) if col in json_fields \
                      else entry[col]

    values["content_hash"] = content_hash(
        {k: v for k, v in entry.items() if k != "knowledge_time"})
    values["received_at"] = datepage
    values["source_cursor"] = cursor
    values["source_page"] = page

    cur = conn.execute(
        (f"INSERT OR IGNORE INTO {table} ("
         f"{', '.join(cols[1:])}) "
         f"VALUES ({', '.join('?' for _ in cols[1:])});"),
        tuple(values[col] for col in cols[1:])
    )
    return bool(cur.rowcount)

def write_order_entry(entry, conn, datepage, cursor, page):
    return write_landing_entry(entry, conn, "orders_raw", ORDERS_COLS,
                                datepage, cursor, page,
                                json_fields=frozenset({"items"}))

def write_customer_entry(entry, conn, datepage, cursor, page):
    return write_landing_entry(entry, conn, "customers_raw", CUSTOMERS_COLS,
                                datepage, cursor, page)

write_entry={"orders": write_order_entry, "customers": write_customer_entry}


def create_stg_orders(conn: sqlite3.Connection):
    conn.execute(f"DROP VIEW IF EXISTS stg_orders;")
    conn.execute(f"CREATE VIEW stg_orders AS "
                 f"SELECT order_id, customer_id, occurred_at, "
                 f"channel, payment_method, "
                 f"shipping_speed, order_total_cents, knowledge_time, "
                 f"content_hash, CAST(version AS INTEGER) AS version "
                 f"FROM orders_raw")
    # Default : DDL does commit on default, DML does not. 


def create_orders_current(conn: sqlite3.Connection):
    # CTE (WITH ranked AS (...)): a named, query-scoped temp result.           
    # ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...): window function —     
    # numbers rows within each order_id group without collapsing them,         
    # so the outer SELECT can filter to rn = 1 (latest version per order).  
    conn.execute(f"DROP VIEW IF EXISTS orders_current")
    conn.execute(f"CREATE VIEW orders_current AS "
                 f"WITH ranked AS ("
                 f"SELECT *, "
                 f"ROW_NUMBER() OVER (PARTITION BY order_id "
                 f"ORDER BY version DESC, knowledge_time DESC) AS rn "
                 f"FROM stg_orders) "
                 f"SELECT order_id, customer_id, occurred_at, "
                 f"channel, payment_method, shipping_speed, "
                 f"order_total_cents, knowledge_time, content_hash, "
                 f" version FROM ranked WHERE rn = 1;")


def create_stg_customers(conn: sqlite3.Connection):
    conn.execute(f"DROP VIEW IF EXISTS stg_customers;")
    conn.execute(f"CREATE VIEW stg_customers AS "
                 f"SELECT customer_id, segment, country, city, "
                 f"signup_date, CAST(is_active AS INTEGER) AS is_active, "
                 f"lifetime_value_cents, "
                 f"valid_from, valid_to, knowledge_time, "
                 f"content_hash, CAST(version AS INTEGER) AS version "
                 f"FROM customers_raw")


def create_customers_current(conn: sqlite3.Connection):
    conn.execute(f"DROP VIEW IF EXISTS customers_current")
    conn.execute(f"CREATE VIEW customers_current AS "
                 f"WITH ranked AS ("
                 f"SELECT *, "
                 f"ROW_NUMBER() OVER (PARTITION BY customer_id "
                 f"ORDER BY version DESC, knowledge_time DESC) AS rn "
                 f"FROM stg_customers "
                 f"WHERE valid_to IS NULL) "
                 f"SELECT * FROM ranked WHERE rn = 1;")