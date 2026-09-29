-- Run interactively:  sqlite3 scratch.db
--   .read explore.sql          (or paste blocks one by one)
.headers on
.mode column

-- 0. What do we have?
.tables
.schema fact_orders
PRAGMA table_info(fact_order_items);

-- 1. Everything was imported as TEXT. Empty CSV fields are '' not NULL.
SELECT typeof(discount_pct) t, count(*) FROM fact_order_items GROUP BY t;
SELECT count(*) AS empty_discount FROM fact_order_items WHERE discount_pct = '';
SELECT count(*) AS null_discount  FROM fact_order_items WHERE discount_pct IS NULL;
-- NULLIF turns '' into a real NULL; avg() ignores NULLs, so compare:
SELECT avg(discount_pct) AS avg_text_as_is,
       avg(NULLIF(discount_pct, '')) AS avg_nullif,
       avg(COALESCE(NULLIF(discount_pct, ''), 0)) AS avg_missing_is_zero
FROM fact_order_items;

-- 2. Casting: text sorts alphabetically ('9' > '10')!
SELECT order_id FROM fact_orders ORDER BY order_id DESC LIMIT 3;                  -- '999', '998'...
SELECT order_id FROM fact_orders ORDER BY CAST(order_id AS INT) DESC LIMIT 3;     -- 1500, 1499...

-- 3. Profile each table: rows, distinct keys, min/max
SELECT count(*) n, count(DISTINCT order_id) distinct_ids,
       min(order_ts) first_order, max(order_ts) last_order FROM fact_orders;
SELECT status, count(*) n, round(100.0 * count(*) / (SELECT count(*) FROM fact_orders), 1) pct
FROM fact_orders GROUP BY status ORDER BY n DESC;

-- 4. Orphan checks (LEFT JOIN ... WHERE right IS NULL): FKs are not enforced yet
SELECT count(*) AS orders_without_customer
FROM fact_orders o LEFT JOIN dim_customer c ON c.customer_id = o.customer_id
WHERE c.customer_id IS NULL;
SELECT count(*) AS items_without_product
FROM fact_order_items i LEFT JOIN dim_product p ON p.product_id = i.product_id
WHERE p.product_id IS NULL;
SELECT count(*) AS orders_without_items
FROM fact_orders o LEFT JOIN fact_order_items i ON i.order_id = o.order_id
WHERE i.order_id IS NULL;

-- 5. Walk the snowflake: revenue by country and top-level category
SELECT co.country_name, sum(CAST(i.quantity AS INT) * CAST(i.unit_price AS REAL)) AS gross
FROM fact_order_items i
JOIN fact_orders o    ON o.order_id = i.order_id
JOIN dim_customer cu  ON cu.customer_id = o.customer_id
JOIN dim_city ci      ON ci.city_id = cu.city_id
JOIN dim_country co   ON co.country_id = ci.country_id
WHERE o.status = 'completed'
GROUP BY co.country_name ORDER BY gross DESC;

SELECT COALESCE(NULLIF(parent.category_name, ''), c.category_name) AS top_category,
       count(*) AS items
FROM fact_order_items i
JOIN dim_product p   ON p.product_id = i.product_id
JOIN dim_category c  ON c.category_id = p.category_id
LEFT JOIN dim_category parent ON parent.category_id = NULLIF(c.parent_category_id, '')
GROUP BY top_category ORDER BY items DESC;

-- 6. Dates are strings: check formats, then use strftime
SELECT length(order_ts) len, count(*) FROM fact_orders GROUP BY len;
SELECT strftime('%Y-%m', order_ts) AS month, count(*) orders
FROM fact_orders GROUP BY month ORDER BY month;

-- 7. JSON payments: json_extract on the raw doc
SELECT json_extract(doc, '$.order_id') order_id,
       json_extract(doc, '$.method')   method,
       json_extract(doc, '$.amount')   amount,
       json_extract(doc, '$.card.brand') card_brand      -- NULL when card is null
FROM raw_payments LIMIT 5;

SELECT json_extract(doc, '$.method') method, count(*) n,
       round(sum(json_extract(doc, '$.amount')), 2) total
FROM raw_payments GROUP BY method;

-- 8. Reconcile: does payment amount == sum of items after discount?
WITH items_total AS (
  SELECT order_id,
         round(sum(CAST(quantity AS INT) * CAST(unit_price AS REAL)
                   * (1 - COALESCE(NULLIF(discount_pct, ''), 0) / 100.0)), 2) AS total
  FROM fact_order_items GROUP BY order_id
), pay AS (
  SELECT json_extract(doc, '$.order_id') AS order_id,
         json_extract(doc, '$.amount')   AS amount
  FROM raw_payments
)
SELECT count(*) AS orders, sum(abs(pay.amount - t.total) < 0.01) AS matching
FROM pay JOIN items_total t ON t.order_id = pay.order_id;

-- 9. Log lines: separate good from junk with LIKE / GLOB, then parse with substr/instr
SELECT CASE WHEN line LIKE '%INFO user=%event=%' THEN 'ok'
            WHEN line = '' THEN 'blank' ELSE line END AS kind, count(*) n
FROM raw_log GROUP BY kind ORDER BY n DESC;

-- parse "user=73" out of the line (SQLite has no regex: substr + instr)
SELECT substr(line, 1, 20) AS ts,
       CAST(substr(line, instr(line, 'user=') + 5,
            instr(substr(line, instr(line, 'user=') + 5), ' ') - 1) AS INT) AS user_id,
       line
FROM raw_log WHERE line LIKE '%INFO user=%' LIMIT 5;

-- 10. Your turn (ideas):
--  a. Top 5 customers by completed-order revenue
--  b. Monthly revenue and month-over-month change (window function: LAG)
--  c. Sellers with no sale at all
--  d. Conversion: users with add_to_cart events who never ordered (needs the log parsed)
--  e. Duplicated emails in dim_customer? (GROUP BY email HAVING count(*) > 1, mind the '')
