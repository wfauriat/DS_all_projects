# SQL cheatsheet (grows as we practice)

Examples use the marketplace dataset (`scratch.db`, raw-loaded, all columns TEXT).

## SQLite CLI

```sql
.headers on          -- show column names
.mode box            -- pretty output (also: column, table, markdown, line, csv)
.tables              -- list tables
.schema dim_customer -- CREATE TABLE statement
PRAGMA table_info(dim_customer);                 -- column names/types/pk
SELECT name FROM pragma_table_info('dim_customer');  -- same, queryable
.read file.sql       -- run a script
```

Terminal: `sqlite3 -box -header scratch.db "SELECT ..."`. `Ctrl+L` clears the screen.
A statement ends with `;`. If you see a `...>` prompt, you forgot it.

## 1. SELECT and FROM

`FROM` picks the table, `SELECT` picks (or computes) the columns.

```sql
SELECT * FROM dim_country;                          -- all columns (exploration only)
SELECT country_name, region FROM dim_country;       -- chosen columns, chosen order
SELECT name AS customer_name FROM dim_customer;     -- AS renames in the output only
SELECT product_name, list_price * 2 AS double_price FROM dim_product;  -- computed column
SELECT customer_id, name FROM dim_customer LIMIT 5; -- first 5 rows
```

- Without `ORDER BY`, row order is not guaranteed.
- Keywords UPPERCASE, identifiers lower_snake_case (convention).
- Avoid `SELECT *` outside exploration.

## 2. WHERE

Filters rows; runs right after `FROM`, before `SELECT`. Only rows where the condition is true survive.

```sql
SELECT * FROM fact_orders WHERE status = 'cancelled';          -- strings: single quotes
SELECT * FROM dim_product WHERE list_price > 800;              -- = <> < <= > >=  (single "=")
SELECT * FROM fact_orders WHERE status = 'refunded' AND order_ts >= '2025-05-01';
SELECT * FROM fact_orders WHERE status IN ('refunded', 'cancelled');   -- shorthand for OR chain
SELECT * FROM dim_product WHERE list_price BETWEEN 100 AND 200;        -- inclusive both ends
SELECT * FROM dim_customer WHERE name LIKE 'Ana%';             -- % = any chars, _ = one char
SELECT * FROM dim_customer WHERE email IS NULL;                -- NULL needs IS / IS NOT, never =
```

- `AND` binds tighter than `OR`: always parenthesize mixed conditions.
  `WHERE (category_id = 4 OR category_id = 5) AND list_price > 500`
- Single quotes = string values, double quotes = identifiers.
- Filtering on a raw-imported column: it is TEXT, so compare with a cast.
  `WHERE CAST(list_price AS REAL) > 800`. Text comparison is alphabetical ('9' > '10').
  The real fix is declaring column types at table creation.
- Empty string and NULL are different things: `email = ''` and `email IS NULL` match different rows.
  SQLite's CSV `.import` yields `''` for empty fields, never NULL. To get NULLs:
  `UPDATE t SET col = NULL WHERE col = '';` or `NULLIF(col, '')` in a query, or convert
  `'' -> None` in a Python loader (`sqlite3` module maps `None` to NULL).

## 3. ORDER BY and aggregates

`ORDER BY` sorts the result; it runs after `SELECT`, so it can use column aliases (`WHERE` cannot).

```sql
SELECT product_name, list_price FROM dim_product ORDER BY list_price DESC LIMIT 5;  -- ASC is default
SELECT * FROM fact_orders ORDER BY status, order_ts DESC;      -- several keys: ties broken by the next
SELECT list_price * 2 AS dbl FROM dim_product ORDER BY dbl;    -- alias OK here
ORDER BY CAST(list_price AS REAL) DESC                         -- TEXT columns sort alphabetically otherwise
```

Aggregates collapse rows into one value (one output row unless `GROUP BY`):

```sql
SELECT count(*), min(order_ts), max(order_ts) FROM fact_orders;
SELECT sum(CAST(quantity AS INT)), avg(CAST(unit_price AS REAL)) FROM fact_order_items;
SELECT count(*) FROM fact_orders WHERE status = 'cancelled';   -- WHERE filters first, then aggregate
```

| Expression | Counts |
|---|---|
| `count(*)` | rows |
| `count(col)` | rows where `col` is not NULL (`''` counts!) |
| `count(DISTINCT col)` | distinct non-NULL values |

- Aggregates skip NULLs. `avg(x)` is `sum(x) / count(x)`, so what is NULL changes the denominator.
- Missing-value handling changes the answer (discount example: 6.18 vs 5.56):
  ```sql
  avg(NULLIF(discount_pct, ''))                   -- ignore missing -> 6.18
  avg(COALESCE(NULLIF(discount_pct, ''), 0))      -- missing means 0 -> 5.56
  ```
  `NULLIF(x, '')`: `''` becomes NULL. `COALESCE(x, 0)`: NULL becomes 0.
- On a raw-imported column, `avg(discount_pct)` silently treats `''` as 0 (same as the second form).
- `CAST(x AS INT)` truncates decimals; use `REAL` for prices.
- `LIMIT` on a single-row aggregate does nothing.

## 4. GROUP BY

Splits rows into groups (one per distinct value or combination) and aggregates each group: one output row per group.

```sql
SELECT status, count(*) AS n FROM fact_orders GROUP BY status ORDER BY n DESC;   -- SQL's "sort | uniq -c"
SELECT category_id, count(*) AS n, min(CAST(list_price AS REAL)) AS cheapest
FROM dim_product GROUP BY category_id;
SELECT status, strftime('%Y-%m', order_ts) AS month, count(*) AS n
FROM fact_orders GROUP BY status, month;                      -- one row per (status, month) combination
```

- **Rule:** every `SELECT` column is either in `GROUP BY` or inside an aggregate. SQLite does not enforce
  it (returns an arbitrary value silently); Postgres/BigQuery/DuckDB raise an error. Don't rely on it.
- `count(*)` = rows per group (items per order); `sum(col)` = total of a column (units per order). Not the same thing.
- `WHERE` filters rows *before* grouping.
- Ties + `LIMIT`: add a tiebreaker (`ORDER BY units DESC, order_id`) or the result is arbitrary.
- `GROUP BY` puts all NULLs into one group (even though `NULL = NULL` is not true).
  The same goes for `''`: `GROUP BY email` gives one group of 10 "missing" customers that is not a real email.
  Filter (`WHERE email <> ''`) or convert to NULL first.
- Alias in `WHERE`: SQLite allows it (lenient); standard SQL/Postgres does not. Repeat the expression or use a CTE.

## 5. HAVING and the pipeline order

Logical execution order (not the order you write it):

```
FROM (+JOIN) → WHERE → GROUP BY → HAVING → SELECT → DISTINCT → ORDER BY → LIMIT
```

`WHERE` filters **rows** before grouping. `HAVING` filters **groups** after aggregation, so it can use aggregates.

```sql
SELECT city_id, count(*) AS n FROM dim_customer
GROUP BY city_id
HAVING count(*) < 15;                       -- groups with fewer than 15 customers

SELECT customer_id, count(*) AS n_orders FROM fact_orders
WHERE status = 'completed'                  -- row filter first
GROUP BY customer_id
HAVING count(*) >= 8                        -- then group filter
ORDER BY n_orders DESC, customer_id;
```

- `WHERE count(*) > 1` is an error: the groups do not exist yet at the `WHERE` stage.
- If a condition can go in `WHERE`, put it there (runs earlier, less work). `HAVING` only for aggregate conditions.
- Aliases in `HAVING` work in SQLite but not portably: write `HAVING count(*) >= 15`.
- Check the direction of the comparison (`<` vs `>=` return complements) and check the distribution first
  (`ORDER BY n LIMIT 3`), or a threshold may filter nothing.

**Assertion / duplicate finder** (returns zero rows when all is well):

```sql
SELECT email, count(*) AS n
FROM dim_customer
WHERE email <> ''            -- without this, the 10 missing emails show up as a false "duplicate"
GROUP BY email
HAVING count(*) > 1;
```

## 6. INNER JOIN

Glues rows of two tables where a condition holds (usually foreign key = primary key). `JOIN` alone means `INNER JOIN`: rows without a match on either side are dropped.

```sql
SELECT c.name, ci.city_name
FROM dim_customer AS c                        -- alias defined in FROM, usable everywhere
JOIN dim_city AS ci ON ci.city_id = c.city_id;

-- chain joins to walk the snowflake, one hop per JOIN
SELECT co.country_name, count(*) AS n
FROM dim_customer AS c
JOIN dim_city AS ci     ON ci.city_id = c.city_id
JOIN dim_country AS co  ON co.country_id = ci.country_id
GROUP BY co.country_name ORDER BY n DESC;
```

- Always qualify columns with the alias (`c.name`, not `name`): readable, and no ambiguity.
- **Cardinality decides the row count.**
  - many-to-one (customer -> city): row count unchanged.
  - one-to-many (order -> its items): the "one" side is repeated once per match.
    `fact_orders JOIN fact_order_items` = 2643 rows = the many side (`fact_order_items`), not 1500.
  - Consequence: summing or counting a column that lives on the "one" side after such a join multiplies it.
    Aggregate the many side first, or count `DISTINCT`.
- Sanity habit: compare `count(*)` before and after each join.

## 7. LEFT JOIN

Keeps every row of the left table; right-side columns are NULL when nothing matches.

```sql
SELECT c.category_name, p.product_id
FROM dim_category AS c
LEFT JOIN dim_product AS p ON p.category_id = c.category_id;   -- parent categories appear with NULL product

-- count matches: count a RIGHT-side column, not *
SELECT c.category_name, count(*) AS n_rows, count(p.product_id) AS n_products
FROM dim_category AS c
LEFT JOIN dim_product AS p ON p.category_id = c.category_id
GROUP BY c.category_id, c.category_name;                       -- parents: n_rows = 1, n_products = 0
```

**Anti-join** (rows with no match): `LEFT JOIN` then `WHERE right.key IS NULL`.

```sql
SELECT c.customer_id, c.name                 -- customers with no orders at all
FROM dim_customer AS c
LEFT JOIN fact_orders AS o ON o.customer_id = c.customer_id
WHERE o.order_id IS NULL;
```

**Where the filter goes decides the meaning:**

```sql
-- customers with no COMPLETED order (includes those with only shipped orders, and those with none)
SELECT c.customer_id, c.name
FROM dim_customer AS c
LEFT JOIN fact_orders AS o
       ON o.customer_id = c.customer_id
      AND o.status = 'completed'             -- defines what "a match" means -> ON
WHERE o.order_id IS NULL;                    -- the "no match" test -> WHERE
```

- A right-side filter in `WHERE` (`WHERE o.status = 'completed'`) discards the unmatched rows (their value is NULL,
  and `NULL = 'completed'` is not true), so the query becomes an `INNER JOIN` in disguise.
  Test: 1051 rows, identical to the inner join.
- Rule: conditions that define a match go in `ON`; `WHERE` on a right-side column is only for `IS NULL`
  (deliberate anti-join) or when you truly want inner-join behaviour.
- Equivalent anti-join forms: `NOT EXISTS (SELECT 1 ...)` and `NOT IN (...)` (careful: `NOT IN` returns nothing if the list contains a NULL).
- Orphan checks (`explore.sql` section 4) use this exact pattern.

## 8. NULLs

NULL = "unknown / no value" (not 0, not `''`). Core rule: **any comparison with NULL yields NULL (unknown), and `WHERE` keeps only rows where the condition is true.**

```sql
.nullvalue NULL                          -- CLI: display NULL instead of blank (blank looks like '')

SELECT NULL = NULL, NULL <> 1, NULL + 1, 'a' || NULL;   -- all NULL
SELECT NULL IS NULL;                     -- 1: the only way to test for NULL (IS NULL / IS NOT NULL)
```

**Three-valued logic:** `NULL AND 0` = 0, `NULL AND 1` = NULL, `NULL OR 1` = 1, `NULL OR 0` = NULL, `NOT NULL` = NULL.
So a filter and its complement don't cover everything:

```sql
-- 200 customers, 190 with an email, 10 NULL
WHERE NULLIF(email,'') = 'x' OR NULLIF(email,'') <> 'x'     -- returns 190, not 200
```
`WHERE col <> 'x'` silently drops the NULL rows.

**Converting:**

```sql
COALESCE(a, b, 'x')      -- first non-NULL argument
ifnull(a, 'y')           -- SQLite-only two-argument shorthand
NULLIF(a, '')            -- NULL if a = '', else a      (empty -> NULL)
COALESCE(NULLIF(a,''), 0)                                -- empty/NULL -> 0
```

| Situation | Behaviour |
|---|---|
| `col = NULL`, `col <> NULL` | never true; use `IS [NOT] NULL` |
| `WHERE col <> 'x'` | drops NULL rows |
| `count(col)`, `sum`, `avg`, `min`, `max` | skip NULLs |
| aggregate over no non-NULL values | `sum`/`max`/`avg` = NULL, `count` = 0 (SQLite `total()` = 0.0) |
| `GROUP BY`, `DISTINCT` | all NULLs form one group |
| `ORDER BY` | SQLite: NULLs first in ASC, last in DESC (Postgres is the opposite); explicit: `NULLS FIRST` / `NULLS LAST` (SQLite 3.30+) |
| join key is NULL | never matches |
| CSV `.import` in SQLite | `''`, not NULL: convert deliberately (`NULLIF`, `UPDATE`, or `None` in a Python loader) |

**IN / NOT IN trap:**

```sql
SELECT 2 IN (1, NULL);          -- NULL (unknown), not 0
SELECT 2 NOT IN (1, NULL);      -- NULL -> row not kept
```
A `NOT IN (subquery)` whose result contains one NULL matches **no rows**, silently. Prefer `NOT EXISTS`
or `LEFT JOIN ... WHERE right.key IS NULL`.

**NULL-safe equality** (when a key can legitimately be NULL):

```sql
a IS b                                   -- SQLite: null-safe equality (NULL IS NULL -> 1, NULL IS 1 -> 0)
a IS NOT DISTINCT FROM b                 -- standard SQL (SQLite 3.39+, Postgres, DuckDB)
```

## 9. CTEs (WITH)

A named intermediate result that lives only for one statement (not a table, not a view). Each CTE is one stage
of the pipeline; the final `SELECT` reads from them.

```sql
WITH order_totals AS (                         -- one row per order (aggregate the many side first)
    SELECT order_id,
           sum(CAST(quantity AS INT) * CAST(unit_price AS REAL)
               * (1 - COALESCE(NULLIF(discount_pct, ''), 0) / 100.0)) AS total
    FROM fact_order_items
    GROUP BY order_id
),                                             -- comma between CTEs, a single WITH
completed AS (                                 -- later CTEs can use earlier ones
    SELECT order_id, customer_id FROM fact_orders WHERE status = 'completed'
)
SELECT c.customer_id, count(*) AS n_orders, round(sum(t.total), 2) AS revenue
FROM completed AS c
JOIN order_totals AS t ON t.order_id = c.order_id
GROUP BY c.customer_id
ORDER BY revenue DESC
LIMIT 5;
```

- Name a CTE for what it *is* (`completed_orders`), one job per CTE (filter, join, aggregate or window).
- Inspect any stage: swap the final `SELECT` for `SELECT * FROM order_totals LIMIT 10;`.
- Decide the **grain** (one row per what?) at each stage. Average order value is 964.09 (over `order_totals`);
  averaging over item rows gives 547.16, a different question with no error raised.
- Aggregate the many side in a CTE *before* joining so a join cannot multiply it.
- Same result as a subquery in `FROM`, but read top-to-bottom.

**Recursive CTE** (hierarchies of arbitrary depth): base case `UNION ALL` recursive step referring to itself.

```sql
WITH RECURSIVE tree(category_id, category_name, depth, path) AS (
    SELECT category_id, category_name, 0, category_name
    FROM dim_category WHERE NULLIF(parent_category_id, '') IS NULL          -- base: top level
  UNION ALL
    SELECT c.category_id, c.category_name, t.depth + 1, t.path || ' > ' || c.category_name
    FROM dim_category AS c JOIN tree AS t ON c.parent_category_id = t.category_id  -- children
)
SELECT depth, path FROM tree ORDER BY path;
```

## 10. Window functions, part 1

`GROUP BY` collapses rows to one per group; a window function computes over a group of rows but **keeps every row**
(row's own data + group-level value side by side, no self-join).

```sql
<function>(...) OVER (
    [PARTITION BY <cols>]     -- independent groups, like GROUP BY without collapsing
    [ORDER BY <cols>]         -- order within each group (needed for ranking, LAG, running totals)
    [frame clause]            -- which rows the function sees (part 2)
)
```

**Aggregate as window:**

```sql
SELECT order_id, customer_id,
       count(*) OVER (PARTITION BY customer_id) AS customer_n_orders     -- every order keeps its row
FROM fact_orders;

-- row vs its group: share of the customer's total (o = orders joined to order_totals CTE)
SELECT order_id, customer_id, round(total, 2) AS total,
       round(100 * total / sum(total) OVER (PARTITION BY customer_id), 1) AS pct_of_customer
FROM o;
```

**Ranking** (differ only on ties; values 30, 20, 20, 10 ordered DESC):

| function | result | ties |
|---|---|---|
| `ROW_NUMBER()` | 1, 2, 3, 4 | unique, ties broken arbitrarily |
| `RANK()` | 1, 2, 2, 4 | ties share a rank, gap after |
| `DENSE_RANK()` | 1, 2, 2, 3 | ties share a rank, no gap |

**Top N per group** (most recent order per customer; `rn <= 3` for the top 3):

```sql
WITH ranked AS (
    SELECT customer_id, order_id, order_ts,
           ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_ts DESC) AS rn
    FROM fact_orders
)
SELECT customer_id, order_id, order_ts FROM ranked WHERE rn = 1;      -- 199 rows: customer 180 has no orders
```

- Windows are computed at the `SELECT` stage (after `WHERE`/`GROUP BY`/`HAVING`), so they cannot be filtered in
  the same query's `WHERE` (`misuse of window function`). Compute in a CTE/subquery, filter outside.
  Snowflake/BigQuery/Databricks have `QUALIFY` for this; SQLite and Postgres do not.
- `PARTITION BY` is optional: `OVER (ORDER BY x)` treats the whole result as one group.
- The `ORDER BY` inside `OVER` orders the calculation only, not the output: keep a normal `ORDER BY` at the end.
- Add a tiebreaker to `ORDER BY` inside `ROW_NUMBER` if the result must be reproducible.

## 11. Window functions, part 2: order, neighbours, frames

**LAG / LEAD** (previous / next row in the window's `ORDER BY`):

```sql
SELECT order_id, order_ts,
       LAG(order_ts) OVER (PARTITION BY customer_id ORDER BY order_ts) AS prev_ts,
       round(julianday(order_ts)
             - julianday(LAG(order_ts) OVER (PARTITION BY customer_id ORDER BY order_ts)), 1) AS days_since_prev
FROM fact_orders WHERE customer_id = 1 ORDER BY order_ts;
```

- `LAG(x, n, default)`: n rows back (default 1), `default` instead of NULL when there is no such row.
- First row of each partition gets NULL. `PARTITION BY` resets the neighbours; without it, customer 2's first
  order would see customer 1's last as "previous".
- `julianday(ts)` turns a date string into a number of days (subtract to get gaps).

**Month-over-month change** = aggregate first (CTE), then window over the aggregated rows:

```sql
WITH order_totals AS (...), monthly AS (
    SELECT strftime('%Y-%m', o.order_ts) AS month, round(sum(t.total), 2) AS revenue
    FROM fact_orders AS o JOIN order_totals AS t ON t.order_id = o.order_id
    WHERE o.status = 'completed' GROUP BY month
)
SELECT month, revenue,
       round(revenue - LAG(revenue) OVER (ORDER BY month), 2) AS delta,
       round(100.0 * (revenue - LAG(revenue) OVER (ORDER BY month))
             / NULLIF(LAG(revenue) OVER (ORDER BY month), 0), 1) AS pct_change   -- NULLIF: avoid /0
FROM monthly ORDER BY month;
```

**Frame clause** = which rows of the partition each row sees:

```sql
SUM(revenue) OVER (ORDER BY month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)   -- running total
AVG(revenue) OVER (ORDER BY month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)           -- 3-row moving average
```

- Bounds: `UNBOUNDED PRECEDING`, `n PRECEDING`, `CURRENT ROW`, `n FOLLOWING`, `UNBOUNDED FOLLOWING`.
- Moving averages use fewer rows at the start (first values are averages over 1, 2 rows...).
- **ROWS vs RANGE:** `OVER (ORDER BY m)` with no frame defaults to `RANGE ... UNBOUNDED PRECEDING AND CURRENT ROW`,
  which treats rows with equal `m` as one unit: tied rows all show the same running total (10, 60, 60, 65 vs
  10, 30, 60, 65 with `ROWS`). Write `ROWS` explicitly for running totals and add a unique tiebreaker to the `ORDER BY`.
- **LAST_VALUE gotcha:** the default frame stops at the current row, so `LAST_VALUE(x) OVER (ORDER BY t)` returns the
  current row's value. Use `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`. `FIRST_VALUE` is fine without it.
- Name a repeated window once: `SELECT LAG(x) OVER w, SUM(x) OVER w FROM t WINDOW w AS (ORDER BY month)`.

| You want | Use |
|---|---|
| previous / next row | `LAG(x)` / `LEAD(x)` with `PARTITION BY` + `ORDER BY` |
| change vs last period | `x - LAG(x) OVER (ORDER BY t)`; `%`: divide by `NULLIF(LAG(x) OVER (...), 0)` |
| running total | `SUM(x) OVER (ORDER BY t ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)` |
| n-row moving average | `AVG(x) OVER (ORDER BY t ROWS BETWEEN n-1 PRECEDING AND CURRENT ROW)` |
| overall last value | `LAST_VALUE(x) OVER (... ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)` |

## TODO: SQL topics not yet covered (pick up in a later session)

Status after step 11: the query-reading core is done (SELECT through window functions). Remaining, by day-to-day priority:

1. [ ] **`CASE WHEN`** (biggest gap): bucketing/labels, conditional aggregation (`sum(CASE WHEN ... THEN 1 ELSE 0 END)`, `FILTER (WHERE ...)`), manual pivot/unpivot. Used in `explore.sql` but never taught.
2. [ ] **`DISTINCT` and subqueries**: scalar subqueries, `IN (SELECT ...)`, `EXISTS` / `NOT EXISTS` (safe anti-join), correlated subqueries.
3. [ ] **Set operations**: `UNION` / `UNION ALL`, `INTERSECT`, `EXCEPT` (diff two versions of a table = cheap regression test).
4. [ ] **Scalar functions**: strings (`substr`, `instr`, `replace`, `trim`, `lower`, `||`), dates (`strftime`, `julianday`, date arithmetic), numbers (`round`, integer vs real division), casting. Differ a lot between engines.
5. [ ] **DDL / DML and transactions**: `CREATE TABLE` (types, PK, FK, `CHECK`), `INSERT` (bulk, `INSERT ... SELECT`), `UPDATE`, `DELETE`, upsert (`ON CONFLICT`), `BEGIN`/`COMMIT`/`ROLLBACK`. Links directly to the planned Python ingestion.
6. [ ] **Views and `CREATE TABLE AS`**: persisting a query result (what a dbt model does underneath).
7. [ ] **`EXPLAIN QUERY PLAN` and sargable predicates** (indexes deliberately deprioritised for now).
8. [ ] **Dialect differences**: `QUALIFY`, `ILIKE`, `DATE_TRUNC`, `FILTER`, `IS NOT DISTINCT FROM`, `PIVOT` across SQLite / Postgres / DuckDB / BigQuery.
9. [ ] **JSON functions** (`json_extract` only touched briefly in `explore.sql`).
10. [ ] **Assertion queries** (module1_DE section 12) and **sessionization on the web log** (needs the log parsed first).

Suggested order: 1 -> 2 -> 3, then 5 together with the Python ingestion, the rest as needed.

<!-- Next sections, added as we go: set operations, EXPLAIN/indexes, assertion queries, sessionization on the web log -->
