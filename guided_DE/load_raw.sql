-- Throwaway raw load: everything as TEXT, no constraints.
-- Run: sqlite3 scratch.db < load_raw.sql      (rm scratch.db to start over)
.mode csv
.import data/dim_country.csv     dim_country
.import data/dim_city.csv        dim_city
.import data/dim_category.csv    dim_category
.import data/dim_customer.csv    dim_customer
.import data/dim_seller.csv      dim_seller
.import data/dim_product.csv     dim_product
.import data/fact_orders.csv     fact_orders
.import data/fact_order_items.csv fact_order_items

-- JSONL: one JSON document per row, query it with json_extract()
CREATE TABLE raw_payments (doc TEXT);
CREATE TABLE raw_log (line TEXT);
.mode list
.separator "\t"
.import data/payments.jsonl raw_payments
.import data/web_events.log raw_log
