-- Typed target schema for the marketplace data.
-- Create:  rm -f marketplace.db; sqlite3 marketplace.db < schema.sql
-- STRICT tables (SQLite 3.37+) enforce the declared column types; without STRICT, a TEXT value in an INTEGER column is silently accepted.
-- No date type in SQLite: dates/timestamps are ISO-8601 TEXT ('2025-05-28' / '2025-05-28 14:51:36').
-- Foreign keys are enforced only if `PRAGMA foreign_keys = ON` is set on EVERY connection (the loader must do it).

PRAGMA foreign_keys = ON;

-- ---------- dimensions ----------
CREATE TABLE dim_country (
    country_id    INTEGER PRIMARY KEY,
    country_name  TEXT NOT NULL UNIQUE,
    region        TEXT NOT NULL
) STRICT;

CREATE TABLE dim_city (
    city_id     INTEGER PRIMARY KEY,
    city_name   TEXT NOT NULL,
    country_id  INTEGER NOT NULL REFERENCES dim_country(country_id)
) STRICT;

CREATE TABLE dim_category (
    category_id         INTEGER PRIMARY KEY,
    category_name       TEXT NOT NULL,
    parent_category_id  INTEGER REFERENCES dim_category(category_id)    -- NULL = top level
) STRICT;

CREATE TABLE dim_customer (
    customer_id  INTEGER PRIMARY KEY,
    name         TEXT NOT NULL,
    email        TEXT,                                                  -- NULL = unknown (CSV '' becomes NULL at load)
    city_id      INTEGER NOT NULL REFERENCES dim_city(city_id),
    signup_date  TEXT NOT NULL CHECK (signup_date GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]')
) STRICT;

CREATE TABLE dim_seller (
    seller_id    INTEGER PRIMARY KEY,
    seller_name  TEXT NOT NULL,
    city_id      INTEGER NOT NULL REFERENCES dim_city(city_id),
    joined_date  TEXT NOT NULL CHECK (joined_date GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'),
    rating       REAL CHECK (rating BETWEEN 0 AND 5)                    -- NULL = not rated yet
) STRICT;

CREATE TABLE dim_product (
    product_id    INTEGER PRIMARY KEY,
    product_name  TEXT NOT NULL,
    category_id   INTEGER NOT NULL REFERENCES dim_category(category_id),
    seller_id     INTEGER NOT NULL REFERENCES dim_seller(seller_id),
    list_price    REAL NOT NULL CHECK (list_price > 0)
) STRICT;

-- ---------- facts ----------
CREATE TABLE fact_orders (
    order_id     INTEGER PRIMARY KEY,
    customer_id  INTEGER NOT NULL REFERENCES dim_customer(customer_id),
    order_ts     TEXT NOT NULL,                                         -- 'YYYY-MM-DD HH:MM:SS'
    status       TEXT NOT NULL CHECK (status IN ('completed','shipped','cancelled','refunded'))
) STRICT;

CREATE TABLE fact_order_items (
    order_item_id  INTEGER PRIMARY KEY,
    order_id       INTEGER NOT NULL REFERENCES fact_orders(order_id),
    product_id     INTEGER NOT NULL REFERENCES dim_product(product_id),
    quantity       INTEGER NOT NULL CHECK (quantity > 0),
    unit_price     REAL NOT NULL CHECK (unit_price > 0),
    discount_pct   INTEGER CHECK (discount_pct BETWEEN 0 AND 100)      -- NULL = unknown (NOT the same as 0)
) STRICT;

-- ---------- from payments.jsonl (nested `card` flattened into two nullable columns) ----------
CREATE TABLE payments (
    order_id    INTEGER PRIMARY KEY REFERENCES fact_orders(order_id),   -- one payment per order
    paid_at     TEXT NOT NULL,
    method      TEXT NOT NULL CHECK (method IN ('card','paypal','transfer')),
    amount      REAL NOT NULL CHECK (amount >= 0),
    card_brand  TEXT,
    card_last4  TEXT
) STRICT;

-- ---------- from web_events.log (only well-formed lines; junk goes to a rejects file) ----------
CREATE TABLE web_events (
    event_id    INTEGER PRIMARY KEY,
    event_ts    TEXT NOT NULL,                                          -- 'YYYY-MM-DDTHH:MM:SSZ' as in the log
    user_id     INTEGER NOT NULL REFERENCES dim_customer(customer_id),
    event       TEXT NOT NULL CHECK (event IN ('view','add_to_cart','search')),
    product_id  INTEGER NOT NULL REFERENCES dim_product(product_id),
    session_id  TEXT NOT NULL
) STRICT;
