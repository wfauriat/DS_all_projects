-- Flash-forward: one-liners per remaining SQL topic. Run: sqlite3 scratch.db < flash_forward.sql
-- (the last two INSERTs in section 5 fail on purpose: constraint demos). Tables are TEMP, nothing persists.
.headers on
.mode box
.nullvalue NULL
-- 1 CASE
SELECT product_id, list_price,
  CASE WHEN CAST(list_price AS REAL) < 100 THEN 'cheap' WHEN CAST(list_price AS REAL) < 500 THEN 'mid' ELSE 'premium' END AS tier
FROM dim_product LIMIT 3;
SELECT count(*) AS n,  sum(CASE WHEN status='refunded' THEN 1 ELSE 0 END) AS refunded,
  count(*) FILTER (WHERE status='cancelled') AS cancelled,
  round(100.0*avg(status IN ('cancelled','refunded')),1) AS pct_lost
FROM fact_orders;
SELECT customer_id,
  sum(CASE WHEN status='completed' THEN 1 ELSE 0 END) AS completed,
  sum(CASE WHEN status='shipped' THEN 1 ELSE 0 END) AS shipped
FROM fact_orders GROUP BY customer_id LIMIT 3;
-- 2 DISTINCT / subqueries
SELECT count(DISTINCT customer_id) FROM fact_orders;
SELECT DISTINCT status FROM fact_orders;
SELECT product_id, list_price FROM dim_product WHERE CAST(list_price AS REAL) > (SELECT avg(CAST(list_price AS REAL)) FROM dim_product) LIMIT 3;
SELECT count(*) FROM dim_customer WHERE customer_id IN (SELECT customer_id FROM fact_orders WHERE status='refunded');
SELECT customer_id, name FROM dim_customer c WHERE NOT EXISTS (SELECT 1 FROM fact_orders o WHERE o.customer_id=c.customer_id);
SELECT o.order_id, o.customer_id, o.order_ts FROM fact_orders o
WHERE o.order_ts = (SELECT max(order_ts) FROM fact_orders WHERE customer_id=o.customer_id) LIMIT 3;
-- 3 set ops
SELECT customer_id FROM fact_orders WHERE status='refunded' INTERSECT SELECT customer_id FROM fact_orders WHERE status='cancelled' LIMIT 3;
SELECT customer_id FROM dim_customer EXCEPT SELECT customer_id FROM fact_orders;
SELECT 'order' AS src, count(*) FROM fact_orders UNION ALL SELECT 'item', count(*) FROM fact_order_items;
-- 4 scalar functions
SELECT upper(name), length(name), substr(name,1,3), replace(name,' ','_'), instr(name,' '), trim('  x  '), name || ' <' || email || '>' FROM dim_customer LIMIT 2;
SELECT strftime('%Y-%m-%d',order_ts), strftime('%w',order_ts) AS dow, date(order_ts,'+7 days'), date(order_ts,'start of month'), julianday('2025-06-30')-julianday('2025-01-01') FROM fact_orders LIMIT 2;
SELECT 7/2, 7/2.0, 7%2, round(2.5), round(3.14159,2), abs(-3), CAST('12abc' AS INT), typeof(1), typeof(1.0), typeof('1'), typeof(NULL);
-- 9 JSON
SELECT json_extract(doc,'$.card.brand'), json_type(doc,'$.card') FROM raw_payments LIMIT 3;
SELECT j.key, j.value FROM raw_payments, json_each(raw_payments.doc) j LIMIT 3;
SELECT json_object('id',1,'tags',json_array('a','b'));
-- 5 DDL/DML
CREATE TEMP TABLE demo (id INTEGER PRIMARY KEY, name TEXT NOT NULL, price REAL CHECK (price >= 0), cat INTEGER);
INSERT INTO demo (name, price) VALUES ('a', 1.5), ('b', 2.5);
INSERT INTO demo (name, price) SELECT product_name, CAST(list_price AS REAL) FROM dim_product LIMIT 3;
UPDATE demo SET price = price * 2 WHERE name = 'a';
DELETE FROM demo WHERE id = 2;
INSERT INTO demo (id, name, price) VALUES (1, 'a-new', 9) ON CONFLICT(id) DO UPDATE SET name = excluded.name, price = excluded.price;
SELECT * FROM demo;
BEGIN; DELETE FROM demo; SELECT count(*) AS inside_tx FROM demo; ROLLBACK; SELECT count(*) AS after_rollback FROM demo;
INSERT INTO demo (name, price) VALUES ('bad', -1);
INSERT INTO demo (name) VALUES (NULL);
-- 6 views / CTAS
CREATE TEMP VIEW v_completed AS SELECT * FROM fact_orders WHERE status='completed';
SELECT count(*) FROM v_completed;
CREATE TEMP TABLE t_city_counts AS SELECT city_id, count(*) AS n FROM dim_customer GROUP BY city_id;
SELECT * FROM t_city_counts LIMIT 2;
DROP VIEW v_completed; DROP TABLE t_city_counts;
-- 7 explain
EXPLAIN QUERY PLAN SELECT * FROM fact_orders WHERE customer_id = '5';
CREATE TEMP TABLE o2 AS SELECT * FROM fact_orders; CREATE INDEX temp.ix ON o2(customer_id);
EXPLAIN QUERY PLAN SELECT * FROM o2 WHERE customer_id = '5';
EXPLAIN QUERY PLAN SELECT * FROM o2 WHERE CAST(customer_id AS INT) = 5;
-- 10 assertion
SELECT order_id, count(*) FROM fact_orders GROUP BY order_id HAVING count(*) > 1;
SELECT count(*) AS orphans FROM fact_order_items i LEFT JOIN fact_orders o ON o.order_id=i.order_id WHERE o.order_id IS NULL;
-- 5b foreign keys: OFF by default in SQLite. Run separately, in a fresh in-memory db (FKs must not point into TEMP):
--   sqlite3 :memory:
--   PRAGMA foreign_keys = ON;
--   CREATE TABLE p (id INTEGER PRIMARY KEY);
--   CREATE TABLE c (id INTEGER PRIMARY KEY, p_id INTEGER REFERENCES p(id));
--   INSERT INTO c (p_id) VALUES (1);   -- FOREIGN KEY constraint failed
