.headers on
.mode column

-- SELECT and FROM
SELECT name AS customer_name, city_id FROM dim_customer LIMIT 5;

-- WHERE
SELECT * FROM dim_product WHERE category_id = 5;
SELECT * FROM dim_product WHERE list_price > 800;

-- aggregates and ORDER BY
SELECT count(*) FROM fact_orders WHERE status = 'refunded' 
AND order_ts >= '2025-05-01';
SELECT count(*) FROM fact_orders WHERE status = 'cancelled' 
AND order_ts >= '2025-05-01';

SELECT * FROM fact_orders WHERE status IN ('refunded', 'cancelled');
SELECT * FROM dim_product WHERE list_price BETWEEN 100 AND 200;
SELECT * FROM dim_customer WHERE name LIKE 'Ana%';

SELECT product_name, list_price FROM dim_product ORDER BY list_price LIMIT 5;
SELECT product_name, list_price FROM dim_product ORDER BY list_price DESC LIMIT 5;

SELECT * FROM fact_orders ORDER BY status, order_ts DESC LIMIT 10;

-- aggregates and gotchas

SELECT count(*) AS n_orders, min(order_ts) AS first_order,
max(order_ts) AS last_order FROM fact_orders;

SELECT sum(CAST(quantity AS INT)) AS units,
       avg(CAST(unit_price AS REAL)) AS avg_price
FROM fact_order_items;

SELECT count(*), count(NULLIF(email, '')),
count(DISTINCT city_id) FROM dim_customer;

-- GROUP BY

SELECT status, count(*) AS n
FROM fact_orders
GROUP BY status
ORDER BY n DESC;

-- analog bash
-- tail -n +2 fact_orders.csv | cut -d, -f4 | sort | uniq -c | sort -rn

SELECT category_id, count(*) AS n_products,
       min(CAST(list_price AS REAL)) AS cheapest,
       max(CAST(list_price AS REAL)) AS priciest
FROM dim_product
GROUP BY category_id
ORDER BY category_id;

SELECT status, strftime('%Y-%m', order_ts) AS month, count(*) AS n
FROM fact_orders
WHERE month <= '2025-03'
GROUP BY status, month
ORDER BY status, month;

SELECT order_id,
       count(*)                     AS n_items,
       sum(CAST(quantity AS INT))   AS units
FROM fact_order_items
GROUP BY order_id
ORDER BY units DESC, order_id
LIMIT 5;

-- HAVING
-- pipeline FROM -> WHERE -> GROUP BY -> HAVING -> ORDER BY -> LIMIT

SELECT city_id, count(*) AS n
FROM dim_customer
GROUP BY city_id
HAVING count(*) >= 20
ORDER BY n DESC
LIMIT 10;

SELECT customer_id, count(*) AS n_orders
FROM fact_orders
WHERE status = 'completed'
GROUP BY customer_id
HAVING count(*) >= 8
ORDER BY n_orders DESC, customer_id
LIMIT 10;

SELECT product_id, count(*) AS n FROM fact_order_items
GROUP BY product_id HAVING n >= 25 ORDER BY n DESC LIMIT 10;

SELECT email, count(*) AS n
FROM dim_customer
WHERE email <> ''
GROUP BY email
HAVING count(*) > 1;


-- INNER JOIN

SELECT * FROM dim_customer LIMIT 3;   -- has city_id
SELECT * FROM dim_city LIMIT 3;       -- city_id is its key

SELECT c.name, ci.city_name
FROM dim_customer AS c
JOIN dim_city AS ci ON ci.city_id = c.city_id
LIMIT 5;

SELECT o.order_id, o.customer_id, i.product_id, i.quantity,
FROM fact_orders AS o
JOIN fact_order_items AS i ON i.order_id = o.order_id
LIMIT 10;

SELECT c.name, ci.city_name, co.country_name
FROM dim_customer AS c
JOIN dim_city AS ci     ON ci.city_id = c.city_id
JOIN dim_country AS co  ON co.country_id = ci.country_id
LIMIT 10;

SELECT co.country_name, count(*) AS n
FROM dim_customer AS c
JOIN dim_city AS ci     ON ci.city_id = c.city_id
JOIN dim_country AS co  ON co.country_id = ci.country_id
GROUP BY co.country_name ORDER BY n DESC LIMIT 10;

SELECT p.product_id, p.product_name, c.category_name, s.seller_name
FROM dim_product AS p
JOIN dim_category AS c ON p.category_id = c.category_id
JOIN dim_seller AS s ON p.seller_id = s.seller_id
LIMIT 10;

SELECT count(*) FROM fact_orders AS o
JOIN fact_order_items AS i ON i.order_id = o.order_id;

SELECT count(*) FROM fact_orders;

-- LEFT JOIN

SELECT c.category_name,
       count(*)            AS n_rows,
       count(p.product_id) AS n_products
FROM dim_category AS c
LEFT JOIN dim_product AS p ON p.category_id = c.category_id
GROUP BY c.category_id, c.category_name;

SELECT c.category_id, c.category_name
FROM dim_category AS c
LEFT JOIN dim_product AS p ON p.category_id = c.category_id
WHERE p.product_id IS NULL; -- anti-join

SELECT c.customer_id, c.name, o.order_id FROM dim_customer AS c
LEFT JOIN fact_orders AS o ON o.customer_id = c.customer_id
WHERE o.order_id IS NULL;

-- NULL
.nullvalue NULL

SELECT NULL = NULL AS eq, NULL <> 1 AS ne, NULL + 1 AS plus, 'a' || NULL AS concat;
SELECT NULL IS NULL AS is_null; -- 1: the only way to test for NULL

SELECT NULL AND 0 AS a1, NULL AND 1 AS a2, NULL OR 1 AS o1, NULL OR 0 AS o2, NOT NULL AS n;
--                 0             NULL             1             NULL          NULL

SELECT count(*) FROM dim_customer;                                                
SELECT count(*) FROM dim_customer WHERE NULLIF(email,'') = 'x' OR NULLIF(email,'') <> 'x'; 
SELECT count(*) FROM dim_customer WHERE NULLIF(email,'') IS NULL;         

SELECT COALESCE(NULL, NULL, 'x');       -- first non-NULL argument -> 'x'
SELECT ifnull(NULL, 'y');               -- SQLite two-argument shorthand (not portable)
SELECT NULLIF(5, 5), NULLIF(5, 6);      -- NULL if equal, else the first value -> NULL, 5


-- CTEs (Common Table Expressions)

WITH completed AS (
    SELECT * FROM fact_orders WHERE status = 'completed'
)
SELECT count(*) FROM completed;


-- 1. order_totals: one row per order (aggregate the many side).
-- 2. completed: the orders we care about.
-- 3. The final SELECT joins the two and aggregates per customer.

WITH order_totals AS (
    SELECT order_id,
           sum(CAST(quantity AS INT) * CAST(unit_price AS REAL)
               * (1 - COALESCE(NULLIF(discount_pct, ''), 0) / 100.0)) AS total
    FROM fact_order_items
    GROUP BY order_id
),
completed AS (
    SELECT order_id, customer_id
    FROM fact_orders
    WHERE status = 'completed'
)
SELECT c.customer_id,
       count(*)              AS n_orders,
       round(sum(t.total),2) AS revenue
FROM completed AS c
JOIN order_totals AS t ON t.order_id = c.order_id
GROUP BY c.customer_id
ORDER BY revenue DESC
LIMIT 5;


-- average per ORDER (right, from the CTE): 964.09
WITH order_totals AS (
    SELECT order_id, sum(CAST(quantity AS INT) * CAST(unit_price AS REAL)
           * (1 - COALESCE(NULLIF(discount_pct,''), 0) / 100.0)) AS total
    FROM fact_order_items GROUP BY order_id
)
SELECT count(*) AS n_orders, round(avg(total),2) AS avg_order_value FROM order_totals;

-- average per ITEM (a different question): 547.16
SELECT round(avg(CAST(quantity AS INT) * CAST(unit_price AS REAL)
       * (1 - COALESCE(NULLIF(discount_pct,''), 0) / 100.0)), 2) AS avg_per_item
FROM fact_order_items;


-- WINDOW FUNCTION

WITH order_totals AS (
    SELECT order_id,
           sum(CAST(quantity AS INT) * CAST(unit_price AS REAL)
               * (1 - COALESCE(NULLIF(discount_pct,''), 0) / 100.0)) AS total
    FROM fact_order_items GROUP BY order_id
),
o AS (
    SELECT o.order_id, o.customer_id, t.total
    FROM fact_orders AS o JOIN order_totals AS t ON t.order_id = o.order_id
)
SELECT order_id, customer_id,
       round(total, 2)                                          AS total,
       round(sum(total) OVER (PARTITION BY customer_id), 2)     AS cust_total,
       round(100 * total / sum(total) OVER (PARTITION BY customer_id), 1) AS pct_of_customer
FROM o
ORDER BY customer_id, order_id
LIMIT 5;


WITH t(x) AS (VALUES (10), (20), (20), (30))
SELECT x,
       ROW_NUMBER() OVER (ORDER BY x DESC) AS rn,
       RANK()       OVER (ORDER BY x DESC) AS rk,
       DENSE_RANK() OVER (ORDER BY x DESC) AS dr
FROM t;

WITH ranked AS (
    SELECT customer_id, order_id, order_ts,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_ts DESC)
        AS rn
    FROM fact_orders
)
SELECT customer_id AS id, order_id, order_ts FROM ranked
ORDER BY CAST(customer_id AS INT), CAST(order_id AS INT) 
LIMIT 20;

WITH ranked AS (
    SELECT customer_id, order_id, order_ts,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_ts DESC)
        AS rn
    FROM fact_orders
)
SELECT customer_id AS id, order_id, order_ts FROM ranked
WHERE rn = 1
ORDER BY CAST(customer_id AS INT)
LIMIT 20;

SELECT order_id, order_ts,
       LAG(order_ts) OVER (PARTITION BY customer_id ORDER BY order_ts) AS prev_ts,
       round(julianday(order_ts)
             - julianday(LAG(order_ts) OVER (PARTITION BY customer_id ORDER BY order_ts)), 1) AS days_since_prev
FROM fact_orders
WHERE customer_id = 1
ORDER BY order_ts;

WITH order_totals AS (
    SELECT order_id,
           sum(CAST(quantity AS INT) * CAST(unit_price AS REAL)
               * (1 - COALESCE(NULLIF(discount_pct,''), 0) / 100.0)) AS total
    FROM fact_order_items GROUP BY order_id
),
monthly AS (
    SELECT strftime('%Y-%m', o.order_ts) AS month,
           round(sum(t.total), 2) AS revenue
    FROM fact_orders AS o
    JOIN order_totals AS t ON t.order_id = o.order_id
    WHERE o.status = 'completed'
    GROUP BY month
)
SELECT month, revenue,
       LAG(revenue) OVER (ORDER BY month) AS prev_revenue,
       round(revenue - LAG(revenue) OVER (ORDER BY month), 2) AS delta,
       round(100.0 * (revenue - LAG(revenue) OVER (ORDER BY month))
             / NULLIF(LAG(revenue) OVER (ORDER BY month), 0), 1) AS pct_change
FROM monthly
ORDER BY month;


WITH order_totals AS (
    SELECT order_id,
           sum(CAST(quantity AS INT) * CAST(unit_price AS REAL)
               * (1 - COALESCE(NULLIF(discount_pct,''), 0) / 100.0)) AS total
    FROM fact_order_items GROUP BY order_id
),
monthly AS (
    SELECT strftime('%Y-%m', o.order_ts) AS month,
           round(sum(t.total), 2) AS revenue
    FROM fact_orders AS o
    JOIN order_totals AS t ON t.order_id = o.order_id
    WHERE o.status = 'completed'
    GROUP BY month
)
SELECT month,
SUM(revenue) OVER (ORDER BY month
                   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total,
AVG(revenue) OVER (ORDER BY month
                   ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)AS moving_avg_3m
FROM monthly;


-- CASE WHEN

SELECT product_id, list_price,
  CASE WHEN CAST(list_price AS REAL) < 100 THEN 'cheap'
       WHEN CAST(list_price AS REAL) < 500 THEN 'mid'
       ELSE 'premium' END AS tier
FROM dim_product LIMIT 5;

SELECT count(*) AS n,
  sum(CASE WHEN status = 'refunded' THEN 1 ELSE 0 END) AS refunded, 
  count(*) FILTER (WHERE status = 'cancelled')          AS cancelled, 
  round(100.0 * avg(status IN ('cancelled','refunded')), 1) AS pct_lost  
FROM fact_orders;

-- DISTINCT

SELECT count(DISTINCT customer_id) FROM fact_orders;  
SELECT DISTINCT status FROM fact_orders;