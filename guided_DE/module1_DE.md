---
title: "Data Engineering for Data Scientists"
subtitle: "Module 01 — SQL for Analytics"
date: "2026-04-11"
author: "Claude Opus / Sonnet (writing) - W. Fauriat (supervision)"
toc: true
toc-depth: 3
numbersections: true
colorlinks: true
geometry: "margin=1in"
fontsize: "11pt"
---

# Module 01 — SQL for Analytics

**In this module:** [Why SQL won](#1-why-sql-won-and-what-that-means-for-you) · [Execution model](#2-the-execution-model-think-in-pipelines-not-text) · [Joins & NULL traps](#3-joins-in-detail-and-with-the-null-traps) · [CTEs](#4-common-table-expressions-ctes-and-the-structure-of-a-query) · [Window functions](#5-window-functions-the-main-event) · [Lateral joins](#6-lateral-joins-and-the-correlated-subquery) · [Set operations](#7-set-operations-union-intersect-except) · [EXPLAIN](#8-query-planning-and-explain) · [Indexes](#9-indexes-when-they-help-when-they-dont) · [Readable SQL](#10-writing-sql-another-human-can-read) · [Patterns to know](#11-a-few-patterns-every-data-scientist-should-know-by-heart) · [Testing SQL](#12-testing-your-sql-assertions-as-queries) · [What to remember](#what-to-remember) · [Cheatsheet](#cheatsheet) · [Where this module fits](#where-this-module-fits)

SQL is the most important programming language in data engineering, and by extension the single most important programming language for a working data scientist in 2026. Not because it is beautiful — it is emphatically not — but because it is the one language that every warehouse, every lakehouse engine, every BI tool, every notebook, and every modern data transformation framework speaks natively. If you can write fluent analytical SQL, you can operate in any data stack on earth, from a Snowflake at a Fortune 500 bank to a DuckDB file on your laptop. If you cannot, you will spend your career routing around the tool that could have answered your question in twenty lines, and your colleagues will quietly learn not to trust your numbers.

This module is not a SQL introduction. It assumes you can already write `SELECT`, `JOIN`, `GROUP BY`, `HAVING`, subqueries, and basic aggregates. What it covers is the much smaller, much more important subset of SQL that you need for *analytical* work as opposed to *transactional* work: window functions, CTEs, lateral joins, set operations, the query execution model, how to read an EXPLAIN plan, when indexes do and do not help, and how to structure a 300-line SQL query so you can still read it in six months. By the end of the module you should be comfortable writing the kind of SQL that ships as a dbt model in production, not the kind that survives only in a notebook cell.

---

## 1. Why SQL won (and what that means for you)

Twenty years ago, people argued that SQL would be replaced by the next-generation thing — ORMs, NoSQL, Spark DataFrames, pandas, MapReduce, Hadoop pig scripts, XPath, graph queries, whatever. Every one of those arrived, became popular for a while, and then either died or settled into a niche. SQL meanwhile kept growing: window functions (standardized 2003), common table expressions (2003), recursive CTEs (2003), grouping sets (1999), lateral joins (2008), MERGE (1999), JSON functions (2016), pattern matching (2016), polymorphic tables (2023). The 2026 ANSI SQL is an enormously capable language for analytical work, and every serious warehouse implements most of it.

The reason SQL won is structural: it is a **declarative** language for the operation that almost every analytical question boils down to, which is "take one or more sets of rows and combine, filter, aggregate, and reshape them". You specify *what* you want; the query planner figures out *how*. That means when the data grows, or the storage format changes, or a new index gets added, the same SQL keeps working and just gets faster. The trade-off is that you have to trust the planner, and when the planner gets it wrong, debugging is awkward. Much of this module is about writing SQL the planner will treat well and knowing what to do when it doesn't.

For a data scientist, "SQL fluency" is a specific skill set:

1. **Composing non-trivial queries without losing the thread.** CTEs, well-named subqueries, a mental model of how each stage passes rows to the next.
2. **Using window functions without guessing.** Ranking, running totals, lag/lead, session-ization, top-N-per-group.
3. **Joining correctly in the presence of duplicates, NULLs, and one-to-many relationships.** Most wrong-number bugs live here.
4. **Reading a query plan well enough to know when a query will be expensive.** You do not need to be a DBA, but you need to recognize a full scan from an index seek and a broadcast join from a shuffle join.
5. **Writing SQL that another human can read.** A 300-line query with no CTEs, no aliases, and a magic number in the middle is not fluent — it is a crime scene.

The rest of this module teaches these in that order.

---

## 2. The execution model: think in pipelines, not text

The biggest single misconception beginners have about SQL is that queries are executed in the order you write them. They are not. Every query is a **pipeline of logical operators** that the engine rewrites and reorders before execution. Understanding the pipeline is the foundation for understanding why window functions live where they live, why `WHERE` happens before `GROUP BY`, and why you cannot reference an aggregate in `WHERE`.

The canonical logical order is:

```
FROM        (and JOIN)       — identify source rows
WHERE                         — filter rows
GROUP BY                      — aggregate into groups
HAVING                        — filter groups
SELECT       (and window fns) — compute output columns, windows
DISTINCT                      — deduplicate
ORDER BY                      — sort
LIMIT / OFFSET                — truncate
```

You *write* `SELECT ... FROM ... WHERE ... GROUP BY ... HAVING ... ORDER BY ... LIMIT ...`, but the engine *thinks* `FROM → WHERE → GROUP BY → HAVING → SELECT → DISTINCT → ORDER BY → LIMIT`. That ordering explains almost every "why can't I do X" question beginners ask:

- **"Why can't I filter on a `SUM()` in `WHERE`?"** Because `WHERE` happens before `GROUP BY`, and `SUM()` doesn't exist yet. You need `HAVING`.
- **"Why can't I reference a column alias in `WHERE`?"** Because aliases are assigned in `SELECT`, which happens after `WHERE`. (Some engines are lenient and let you; do not rely on this.)
- **"Why does `ORDER BY` let me use aliases?"** Because `ORDER BY` happens after `SELECT`.
- **"Why does my window function not see the `WHERE` filter?"** It does — `WHERE` runs first. But it does not see `HAVING` filters on aggregates, because those run after window functions are computed. Think about this one carefully before writing a window query with `HAVING`.

Modern engines do not literally execute in this logical order. They rewrite the query through many passes: predicate pushdown (push `WHERE` clauses as close to the source as possible), join reordering (pick the cheapest join order), aggregate pushdown (compute partial aggregates early). But the logical order is the *semantics*: whatever the plan ends up doing, the result must be as if that order had been followed. Keeping it in mind prevents a large class of errors.

### 2.1. A mental model: every stage is a set of rows

A useful way to picture a SQL query is as a conveyor belt of rows flowing through stages. Each stage takes rows in, does something, and passes rows out. The output type might change: `GROUP BY` takes individual rows and emits one row per group; `WHERE` takes rows and emits a subset of rows; a window function takes rows and emits the same rows with a new column attached. When you write a query, ask yourself: "at this point in the pipeline, what are the rows, and what are their columns?". If you can't answer that, the query is a guess and you will be debugging it for the next hour.

This is the single mental model that separates people who write SQL from people who write *good* SQL. It is why the CTE pattern we'll meet in Section 4 is so powerful: each CTE makes the conveyor belt explicit, giving you a place to stop and ask "what's on the belt now?".

---

## 3. Joins, in detail and with the NULL traps

You know the five basic joins: `INNER`, `LEFT`, `RIGHT`, `FULL OUTER`, `CROSS`. You probably use `INNER` and `LEFT` in 95% of your queries. The non-obvious parts of joining are:

**A join with a one-to-many relationship duplicates rows on the "one" side.** If you `INNER JOIN orders ON customers.id = orders.customer_id`, and a customer has 7 orders, they appear 7 times in the result. Every aggregate after this join on a customer-level column will be multiplied by 7. This is the single most common source of wrong-number bugs in SQL. The fix is one of:

- **Aggregate the many-side first**, then join on the pre-aggregated result (one row per customer).
- **Use a window function** over the original join to pick one row per customer.
- **Use `COUNT(DISTINCT)`** if you're only counting, to deduplicate on the fly (but it is much slower than normal `COUNT`).

```sql
-- WRONG: customers counted 1x per order; total revenue inflated if double-counting
SELECT c.country, SUM(c.lifetime_value) AS total_ltv
FROM customers c
LEFT JOIN orders o ON o.customer_id = c.id
GROUP BY c.country;

-- RIGHT: aggregate orders first (or simply don't join at all if you don't need it)
SELECT c.country, SUM(c.lifetime_value) AS total_ltv
FROM customers c
GROUP BY c.country;
```

**NULLs in join keys never match.** `NULL = NULL` is `NULL`, not `TRUE`. Two rows with NULL `customer_id` will not join together. This is usually the right behavior but sometimes bites when one side has legitimate NULLs that a naive reader expects to join. If you need NULL-safe equality, use `IS NOT DISTINCT FROM` (ANSI; supported by Postgres, BigQuery) or `NULL-safe join` extensions.

**`LEFT JOIN` with a filter on the right side is an `INNER JOIN` in disguise.** If you write:

```sql
SELECT *
FROM customers c
LEFT JOIN orders o ON o.customer_id = c.id
WHERE o.order_date > '2026-01-01';
```

then any customer with no orders at all is removed by the `WHERE` (because all their right-side columns are NULL, and `NULL > '2026-01-01'` is `NULL`, which the `WHERE` rejects). If you meant "customers plus any of their 2026 orders", put the date filter in the `ON` clause, not the `WHERE`:

```sql
SELECT *
FROM customers c
LEFT JOIN orders o
  ON o.customer_id = c.id AND o.order_date > '2026-01-01';
```

This is subtle and catches people who assume outer joins "stay outer" regardless of where the filter lives. They do not.

**Self-joins are the default tool for comparing a row to nearby rows.** When you want "the previous row" or "the row 7 days earlier" or "the row with the maximum value per group", you are often tempted to self-join. Do not. Window functions (Section 5) are almost always cleaner and faster. Self-join the same table only when the relationship is genuinely many-to-many (employee-manager hierarchies, graph edges) or when the comparison is non-positional.

---

## 4. Common table expressions (CTEs) and the structure of a query

A common table expression is a named intermediate result. Syntactically:

```sql
WITH
active_customers AS (
    SELECT * FROM customers WHERE status = 'active'
),
recent_orders AS (
    SELECT * FROM orders WHERE order_date > CURRENT_DATE - INTERVAL '30 days'
),
customer_order_stats AS (
    SELECT
        c.id AS customer_id,
        c.country,
        COUNT(o.id) AS orders_last_30d,
        SUM(o.amount) AS revenue_last_30d
    FROM active_customers c
    LEFT JOIN recent_orders o ON o.customer_id = c.id
    GROUP BY c.id, c.country
)
SELECT country,
       AVG(orders_last_30d)   AS avg_orders,
       AVG(revenue_last_30d)  AS avg_revenue
FROM customer_order_stats
GROUP BY country
ORDER BY avg_revenue DESC;
```

The structural argument for CTEs is that every meaningful query has an implicit pipeline of stages, and CTEs give you a name for each stage. When you write a 200-line query without CTEs, the reader has to reconstruct the pipeline from indentation and subqueries. With CTEs, the pipeline is right there in the query's skeleton. Every analytics engineering codebase lives on this pattern.

A few practical rules that come from years of production use:

**Name CTEs for what they *are*, not what you are doing to them.** `active_customers`, not `filter_customers`; `customer_order_stats`, not `aggregate_orders`. The name should describe the set of rows, because the next person who reads the query will be tracking "what is on the conveyor belt at this point" — and the CTE name is the label on the belt.

**Keep each CTE doing one thing.** If a CTE has `SELECT ... FROM ... WHERE ... GROUP BY ... WITH a window function inside a subquery`, split it. The per-CTE cost in a modern warehouse is essentially zero, and the readability payoff is enormous. A good rule of thumb: each CTE is a verb in the pipeline (filter, join, aggregate, window).

**CTEs are not views and not temp tables.** In most engines, a CTE is a compile-time hint — the planner inlines it into the final query and re-plans the whole thing. If you reference the same CTE twice, it may or may not be materialized; you cannot rely on it. Postgres used to force materialization, and still does if you write `WITH foo AS MATERIALIZED (...)`, but BigQuery, Snowflake, and modern Postgres inline by default. This means you usually get the performance of an inlined subquery with the readability of a named step — best of both worlds.

**Recursive CTEs exist and you will need them once a year.** Recursive CTEs let you traverse hierarchies (employee-manager trees, bill-of-materials, ancestry graphs, transitive closures). They have a specific syntax (`WITH RECURSIVE t AS (base_case UNION ALL recursive_case)`) and a set of restrictions (no left-recursive references, no aggregates in the recursive part in most engines). When you need them, they are the correct tool. When you do not, they are a weird-looking trap.

### 4.1. Subqueries vs CTEs vs window functions: when to use which

For a given analytical question, you often have three ways to express it:

1. **As a subquery in `FROM`.** Old-school, still works, hard to read beyond one level.
2. **As a CTE.** Preferred for clarity when you have multiple stages or reuse.
3. **As a window function.** Preferred when you need to compare a row to other rows in the same set, without flattening the row count.

Example: "give me each customer's most recent order". Three ways:

```sql
-- subquery (awkward)
SELECT c.*, o.*
FROM customers c
JOIN orders o
  ON o.customer_id = c.id
  AND o.order_date = (
      SELECT MAX(order_date) FROM orders WHERE customer_id = c.id
  );

-- CTE with GROUP BY then join back (two passes over orders)
WITH latest AS (
    SELECT customer_id, MAX(order_date) AS last_order
    FROM orders
    GROUP BY customer_id
)
SELECT c.*, o.*
FROM customers c
JOIN latest l ON l.customer_id = c.id
JOIN orders o ON o.customer_id = c.id AND o.order_date = l.last_order;

-- window function (one pass, most readable)
SELECT c.*, o.*
FROM customers c
JOIN (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC) AS rn
    FROM orders
) o ON o.customer_id = c.id
WHERE o.rn = 1;
```

The window-function version wins on most engines for two reasons: it scans `orders` once, and it handles ties (multiple orders on the same day) deterministically via `ROW_NUMBER()` instead of returning duplicate joins. This "top-N per group" pattern is the single most useful window-function idiom; once you see it, you will start recognizing opportunities for it everywhere.

---

## 5. Window functions: the main event

Window functions are the single biggest productivity win you can get from learning modern SQL. They let you compute something *per row* using information from other rows, without collapsing the row count. If you can write fluent window functions, you can express 80% of analytical questions in a fraction of the SQL a beginner would need.

The syntax is:

```sql
<function>(<args>) OVER (
    [PARTITION BY <partition_cols>]
    [ORDER BY <order_cols>]
    [<frame_clause>]
)
```

- `PARTITION BY` splits the rows into independent groups. The window function resets between partitions.
- `ORDER BY` defines the order within the partition. Required for ranking and for frames.
- The frame clause — `ROWS BETWEEN ... AND ...` or `RANGE BETWEEN ... AND ...` — defines which rows in the partition the function sees.

The family of window functions splits into three groups:

### 5.1. Ranking and row-numbering

```sql
ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC)
RANK()       OVER (PARTITION BY customer_id ORDER BY order_date DESC)
DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY order_date DESC)
NTILE(4)     OVER (PARTITION BY country     ORDER BY lifetime_value)
```

- `ROW_NUMBER` gives every row a unique position. Ties are broken arbitrarily unless you add more `ORDER BY` columns.
- `RANK` gives tied rows the same rank, and leaves a gap: `1, 2, 2, 4`.
- `DENSE_RANK` gives tied rows the same rank without a gap: `1, 2, 2, 3`.
- `NTILE(n)` buckets the partition into `n` roughly-equal groups and labels them `1..n`. Useful for quantile-ish analyses, though for exact quantiles you want `PERCENTILE_CONT` or `PERCENTILE_DISC` (see 5.4).

The canonical use of `ROW_NUMBER` is the top-N-per-group pattern from 4.1. Always prefer it to self-joins on `MAX` when the per-group logic might have ties or when you want "the top 3 per customer" instead of just the top 1.

### 5.2. Aggregates as windows

Any aggregate function can be used as a window function. Instead of collapsing rows, it computes the aggregate over a window and attaches it to each row:

```sql
SELECT
    order_id,
    customer_id,
    amount,
    SUM(amount) OVER (PARTITION BY customer_id)                AS customer_total,
    AVG(amount) OVER (PARTITION BY customer_id)                AS customer_avg,
    amount - AVG(amount) OVER (PARTITION BY customer_id)       AS deviation_from_avg,
    COUNT(*)    OVER (PARTITION BY customer_id)                AS customer_order_count
FROM orders;
```

Note: you get both the row-level data (`amount`) and the group-level aggregate (`customer_total`) in the same row, without self-joining. This is a constant source of satisfaction every time you use it.

**Running totals** are the frame-clause variant:

```sql
SUM(amount) OVER (
    PARTITION BY customer_id
    ORDER BY order_date
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
) AS cumulative_spend
```

The frame clause `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` means "every row from the start of the partition up to and including this one". That gives a running total. The default frame when you use `ORDER BY` in a window is `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`, which is *almost* the same — the difference is how ties in `ORDER BY` are handled. `ROWS` compares by position; `RANGE` compares by value. For running totals, you usually want `ROWS` to be explicit and safe.

**Sliding windows** are the other obvious variant:

```sql
AVG(amount) OVER (
    PARTITION BY customer_id
    ORDER BY order_date
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
) AS rolling_7_order_avg
```

Seven-row rolling average of `amount`. Swap `ROWS` for `RANGE BETWEEN INTERVAL '7' DAY PRECEDING AND CURRENT ROW` (support varies by engine) to get a seven-*day* rolling average regardless of how many orders landed in those days.

### 5.3. Offset functions: LAG, LEAD, FIRST_VALUE, LAST_VALUE

These let you refer to other rows in the partition by position.

```sql
SELECT
    customer_id,
    order_date,
    amount,
    LAG(amount,  1) OVER (PARTITION BY customer_id ORDER BY order_date) AS prev_amount,
    LEAD(amount, 1) OVER (PARTITION BY customer_id ORDER BY order_date) AS next_amount,
    FIRST_VALUE(order_date) OVER (
        PARTITION BY customer_id ORDER BY order_date
    ) AS first_order_date,
    LAST_VALUE(order_date) OVER (
        PARTITION BY customer_id ORDER BY order_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS last_order_date
FROM orders;
```

Two gotchas:

- **`LAST_VALUE` needs an explicit frame.** Without one, the default frame is "up to the current row", and `LAST_VALUE` returns the current row's value, which is never what you want. Always add `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`.
- **`LAG`/`LEAD` have a second argument for the offset and a third for the default when there is no such row.** `LAG(x, 1, 0)` returns 0 instead of NULL for the first row.

`LAG` is the canonical tool for "compare this row to the previous row" questions: growth rates, session gaps, time-since-last-event, reorder intervals. `LEAD` is the canonical tool for "compare this row to the next" — useful for sessionization.

### 5.4. Distribution functions

```sql
PERCENT_RANK() OVER (ORDER BY score)                    -- 0 to 1
CUME_DIST()    OVER (ORDER BY score)                    -- 0 to 1, cumulative distribution
PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY score)      -- median, interpolated
PERCENTILE_DISC(0.9) WITHIN GROUP (ORDER BY score)      -- 90th percentile, discrete
APPROX_PERCENTILE(score, 0.5)                            -- approximate, fast; engine-specific
```

`PERCENTILE_CONT` and `PERCENTILE_DISC` are ordered-set aggregates, not window functions — syntactically different but in the same conceptual territory. For large datasets, approximate percentile functions (e.g., BigQuery's `APPROX_QUANTILES`, Snowflake's `APPROX_PERCENTILE`, Postgres's `percentile_cont` versus `percentile_disc`) are 10–100× faster and accurate enough for all but the most safety-critical calculations.

### 5.5. Sessionization: the killer application

The textbook window-function application is **sessionization**: given a stream of user events, group them into sessions where a session breaks after N minutes of inactivity. Classic pattern:

```sql
WITH events_with_gap AS (
    SELECT
        user_id,
        event_time,
        LAG(event_time) OVER (PARTITION BY user_id ORDER BY event_time) AS prev_time,
        CASE
            WHEN event_time - LAG(event_time) OVER (PARTITION BY user_id ORDER BY event_time)
                 > INTERVAL '30 minutes'
            THEN 1 ELSE 0
        END AS is_new_session
    FROM events
),
sessionized AS (
    SELECT
        user_id,
        event_time,
        SUM(is_new_session) OVER (
            PARTITION BY user_id ORDER BY event_time
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS session_id
    FROM events_with_gap
)
SELECT user_id, session_id, MIN(event_time) AS session_start, COUNT(*) AS n_events
FROM sessionized
GROUP BY user_id, session_id;
```

The trick is the double window: first compute a "new session" flag by comparing each row to the previous, then compute a running sum of that flag to generate session IDs. The entire session-ization is two windows and one aggregate — no joins, no self-joins, no procedural code. Every data scientist should be able to write this from memory. (Some engines now expose a native `SESSIONIZATION_ID()`; use it when available.)

---

## 6. Lateral joins and the correlated subquery

A **lateral join** (sometimes called `CROSS APPLY` in SQL Server) lets the right-hand side of a join reference columns from the left-hand side. It's the SQL way of saying "for each row on the left, compute this query that depends on the row, and glue the result back on".

```sql
SELECT
    c.id, c.name,
    last_orders.*
FROM customers c
LEFT JOIN LATERAL (
    SELECT order_id, order_date, amount
    FROM orders o
    WHERE o.customer_id = c.id
    ORDER BY order_date DESC
    LIMIT 3
) last_orders ON TRUE;
```

This returns each customer paired with their three most recent orders. Without LATERAL, you'd do it with a window function and a filter:

```sql
SELECT c.id, c.name, o.*
FROM customers c
JOIN (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC) AS rn
    FROM orders
) o ON o.customer_id = c.id
WHERE o.rn <= 3;
```

Both work; the window-function version usually performs better on columnar engines because it does a single pass. `LATERAL` wins when the inner query is more complex than a row-number can express (e.g., depends on multiple non-trivial filters that reference the outer row), or when it calls a set-returning function (`generate_series`, `unnest`). Know it exists; reach for window functions first.

---

## 7. Set operations: UNION, INTERSECT, EXCEPT

Three set operations, each with `ALL` and non-`ALL` variants:

- `UNION` combines two result sets and removes duplicates; `UNION ALL` keeps duplicates. **`UNION ALL` is almost always what you want** — it is an order of magnitude cheaper, and if you have reason to expect duplicates you should probably know and handle them explicitly.
- `INTERSECT` returns rows present in both inputs; `INTERSECT ALL` keeps duplicates per the minimum count.
- `EXCEPT` (sometimes `MINUS`) returns rows in the left but not the right; useful for "what's in `customers_new` that isn't in `customers_old`" kinds of diffs.

A practical use for `EXCEPT`: regression testing transformations. If you change a dbt model and want to check that the output still matches:

```sql
-- anything present in old but not new?
SELECT * FROM old_output
EXCEPT
SELECT * FROM new_output;

-- anything in new but not old?
SELECT * FROM new_output
EXCEPT
SELECT * FROM old_output;
```

If both return zero rows, the transformations produce identical output. This is a cheap smoke test you should run every time you touch a model's logic.

---

## 8. Query planning and EXPLAIN

When a query is slow, the right first step is to ask the engine how it plans to run it. Every major engine supports some form of `EXPLAIN` (Postgres, Snowflake, BigQuery, Databricks SQL all have their own syntax and UI). The plan is a tree of operators: scans at the leaves, joins and aggregates in the middle, a final step at the root. Each operator has a cost estimate and, after execution with `EXPLAIN ANALYZE`, an actual time.

The operators you should recognize:

**Scans.** How the engine reads a table.

- **Full table scan / sequential scan**: read every row. Fast on columnar warehouses for small-to-medium tables; slow on OLTP databases for large tables.
- **Index scan / index seek**: use a B-tree index to find specific rows. Fast for point lookups and small ranges; slow if you're actually reading most rows anyway.
- **Partition scan / partition pruning**: the engine skips entire partitions because the `WHERE` clause rules them out. A well-partitioned query should scan orders-of-magnitude fewer bytes than an unpartitioned one. On warehouses this is the single most important physical optimization.

**Joins.** How the engine combines two inputs.

- **Nested-loop join**: for each row on the left, scan the right. Good for tiny right sides (with an index), catastrophic for large ones.
- **Hash join**: build an in-memory hash table on the smaller side, stream the larger side through it. The default for most analytical joins in modern engines.
- **Merge join** (sort-merge): sort both sides, walk them in parallel. Good when inputs are already sorted by the join key (rare).
- **Broadcast join** (distributed engines): send the smaller table to every node, then local-join with the larger table on its partition. The right choice when one side is small.
- **Shuffle join** (distributed engines): repartition both sides by the join key, then local-join. Necessary when both sides are large. Expensive — the shuffle is the bottleneck.

**Aggregates.**

- **Hash aggregate**: build a hash map keyed by the group columns, accumulate into it. The default.
- **Sort-then-group**: sort by the group columns, then emit groups. Good when output is needed in group order anyway.
- **Streaming aggregate / partial aggregate**: compute partial aggregates per partition, then combine at the root. Used in distributed engines to reduce shuffle.

Reading a plan is a learned skill. Three habits that help:

1. **Start at the leaves.** The scans tell you how much data you're reading. If that number is huge, the rest of the plan barely matters — the query will be slow no matter what.
2. **Look at the cardinalities.** A join that takes 1M × 10K = 10B rows is going to be slow regardless of the join type. Many "my query is slow" problems are "my join produces too many rows" problems, not join-algorithm problems.
3. **Find the biggest operator and ask what it's doing.** If most of the time is in a shuffle, the fix is partition-aligned data. If most of the time is in a full scan, the fix is partition pruning or an index. If most of the time is in a sort, the fix is to avoid the sort.

### 8.1. Partition pruning and the shape of fast warehouse queries

On columnar warehouses (Snowflake, BigQuery, Databricks SQL), the most important performance property of a query is **how much data it scans**. Compute is fast; storage I/O is the bottleneck. The engine tries to scan as little data as possible by (a) reading only the columns you reference (columnar storage pays off here; this is free), and (b) skipping partitions/micro-partitions/row groups where the `WHERE` clause proves no match is possible.

For (b) to work, your `WHERE` clauses have to be on partition columns and have to be sargable — that is, expressed in a way the engine can reason about statically. `WHERE order_date BETWEEN '2026-04-01' AND '2026-04-07'` will prune partitions. `WHERE DATE_PART('week', order_date) = 15` will not, because the engine does not know that `DATE_PART('week', ...)` is monotone. This is why dbt and analytics engineers are pedantic about writing date filters in sargable form: it can change a query from scanning a day's worth of data to scanning a year's.

The practical rule: **always filter on partition columns with simple comparisons**, and when you need a derived value (like "the week of the year"), compute it after the filter, not inside it.

---

## 9. Indexes: when they help, when they don't

On an OLTP database, indexes are critical. On a columnar warehouse, indexes barely exist — the storage layout and partition/cluster keys play a similar role. Because data scientists touch both, you need to know the basics of both.

**OLTP indexes (Postgres, MySQL, SQL Server).**

- A **B-tree index** on a column is a sorted secondary structure that lets the engine find matching rows in O(log n) instead of O(n). The default index type.
- Indexes help `WHERE col = value`, `WHERE col IN (...)`, `WHERE col BETWEEN a AND b`, and `ORDER BY col LIMIT k` queries. They do *not* help `WHERE UPPER(col) = 'X'` unless you create a function index.
- **Composite indexes** on `(a, b, c)` help `WHERE a = ... AND b = ... AND c = ...` and `WHERE a = ... AND b = ...` but not `WHERE b = ... AND c = ...` (the leading column is necessary). The order of columns matters and is chosen with query patterns in mind.
- **Indexes make writes slower.** Every insert/update/delete has to maintain every index. OLTP databases are tuned for read-heavy workloads when you add many indexes; for write-heavy tables you have to be stingy.

**Warehouse "indexes" and their equivalents.**

- **Partition keys**: like an index on an OLTP database, but coarser. `PARTITION BY date_trunc('day', event_time)` in BigQuery lets a query filter on `event_time` to scan only the matching day. You cannot have many partition keys; pick one (usually date).
- **Cluster keys / sort keys**: ordering within a partition. `CLUSTER BY customer_id` in BigQuery or `CLUSTER BY (customer_id)` in Snowflake sorts (or approximately sorts) within each partition, so a query filtering on `customer_id` reads only the relevant micro-partitions. Cluster keys can be multi-column, and you typically pick them to match common filters.
- **Z-order / space-filling curves**: Delta Lake and Iceberg offer `OPTIMIZE ... ZORDER BY`, which sorts files along a space-filling curve so multiple filter columns benefit simultaneously. Useful when queries filter on several non-date columns.
- **Bloom filters**: some engines (BigQuery, Snowflake, Databricks) automatically maintain bloom filters per micro-partition so that a query can cheaply prove "no row with this key is in this micro-partition". You do not usually control these explicitly.
- **Materialized views**: pre-computed aggregates stored as a table, automatically refreshed. The "index" for aggregate queries.

A useful mental model: on a warehouse, the knob that controls "how fast is a query on this filter" is the partitioning and clustering scheme, not an index. Ask your DBA or lead data engineer how the tables you query are partitioned and clustered, and write your `WHERE` clauses to match.

### 9.1. Sargable queries: the #1 performance rule

A **sargable** (search-ARGument-able) predicate is one the engine can evaluate without touching every row. The rule is simple and very valuable:

- **Sargable:** `WHERE order_date >= '2026-04-01'`
- **Not sargable:** `WHERE DATE_TRUNC('month', order_date) = '2026-04-01'`

Both compute the same thing, but the second wraps the column in a function, which hides the comparison from the engine's partition-pruning machinery. The same applies to casts, arithmetic on columns, string functions — any transformation on the *column side* of a comparison blocks index and partition use. Move the transformation to the *literal side* whenever possible:

```sql
-- bad: function wraps the column
WHERE UPPER(email) = 'ALICE@EXAMPLE.COM'

-- good: fold the transformation into the literal
WHERE email = 'alice@example.com'

-- bad: date arithmetic on the column
WHERE order_date + INTERVAL '7 days' > CURRENT_DATE

-- good: move the arithmetic to the literal
WHERE order_date > CURRENT_DATE - INTERVAL '7 days'
```

This is the single most valuable rule in query optimization. It will save you more time than any other single piece of SQL wisdom.

---

## 10. Writing SQL another human can read

A 400-line SQL query is a feature of analytics engineering life. If you cannot come back to one in three months and understand what it does, you have a maintenance bomb. Some rules that keep big SQL readable:

**Use CTEs for every logical stage.** Even if the CTE is a single `SELECT *` — naming it makes the pipeline explicit.

**Pick one casing convention and stick to it.** The most common modern convention: lowercase `snake_case` for identifiers, UPPERCASE for SQL keywords. `SELECT customer_id FROM orders WHERE order_date > '2026-01-01'`. Mixing cases ("selectCustomerId") is fine in application code and terrible in SQL because SQL identifiers are case-insensitive in most engines and case-sensitive in others, and the inconsistency will break somewhere.

**Align columns for readability.** A wide `SELECT` list with three columns per line is much easier to scan than a 30-column single line. Your diff tool will thank you.

```sql
SELECT
    c.id                       AS customer_id,
    c.name                     AS customer_name,
    c.country                  AS customer_country,
    COUNT(o.id)                AS order_count,
    SUM(o.amount)              AS total_revenue,
    AVG(o.amount)              AS avg_order_value,
    MAX(o.order_date)          AS last_order_date
FROM customers c
LEFT JOIN orders o ON o.customer_id = c.id
GROUP BY c.id, c.name, c.country
```

**Qualify columns with table aliases.** Once a query has two or more tables, unqualified column names become ambiguous to the reader even if they're unambiguous to the engine. Always write `c.id` and `o.customer_id`, never `id` and `customer_id`.

**Use short but meaningful aliases.** `customers AS c` is fine; `customers AS customerstable` is noise; `customers AS x` is hostile to the next reader.

**Comment the *why*, not the *what*.** `-- exclude test accounts introduced during Q4 incident` is valuable. `-- select customers` is not.

**Prefer `WHERE` to `HAVING` when you can.** `HAVING` is for filtering on aggregates. If you can put the filter in `WHERE`, do, because it runs before the aggregate and is cheaper.

**Avoid `SELECT *` in production queries.** You rely on specific columns; when the source table gets a new column, `SELECT *` silently changes the output shape, and some downstream consumer breaks. Always list columns explicitly. (The exception is an inner CTE you're about to select from; there, `SELECT *` is fine because the outer CTE pins the columns.)

**Avoid magic numbers.** `WHERE status_code = 7` leaves the reader wondering what 7 means. `WHERE status_code = 7 -- 7 = 'shipped'` is better. Best is a CTE that turns it into a name:

```sql
WITH shipped_orders AS (
    SELECT * FROM orders WHERE status_code = 7 -- 'shipped'
)
```

**Use `qualify` where supported.** Snowflake, BigQuery, and Databricks SQL support `QUALIFY`, which is like `WHERE` but runs *after* window functions and lets you filter on them directly:

```sql
SELECT *
FROM orders
QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC) = 1;
```

This replaces the subquery-with-filter pattern and is noticeably more readable. It's not ANSI, though, so it's a portability compromise. dbt projects targeting one of the supporting warehouses use it freely.

---

## 11. A few patterns every data scientist should know by heart

**Top-N per group:**
```sql
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY g ORDER BY score DESC) AS rn
    FROM t
) x
WHERE rn <= 3;
```

**Deduplicate by taking the latest:**
```sql
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY pk ORDER BY updated_at DESC) AS rn
    FROM t
) x
WHERE rn = 1;
```

**Gaps and islands (find contiguous runs):**
```sql
WITH numbered AS (
    SELECT *,
           ROW_NUMBER() OVER (ORDER BY event_time)                      AS rn_all,
           ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_time) AS rn_user
    FROM events
)
SELECT user_id, MIN(event_time) AS run_start, MAX(event_time) AS run_end, COUNT(*) AS run_len
FROM numbered
GROUP BY user_id, rn_all - rn_user;
```
The `rn_all - rn_user` trick gives the same value for every row in a contiguous run of the same user, so grouping by it collapses runs.

**Pivot manually:**
```sql
SELECT
    customer_id,
    SUM(CASE WHEN product = 'A' THEN amount ELSE 0 END) AS revenue_a,
    SUM(CASE WHEN product = 'B' THEN amount ELSE 0 END) AS revenue_b,
    SUM(CASE WHEN product = 'C' THEN amount ELSE 0 END) AS revenue_c
FROM orders
GROUP BY customer_id;
```
Most engines also have a dedicated `PIVOT` operator; the manual version is portable and readable.

**Unpivot manually:**
```sql
SELECT customer_id, 'revenue_a' AS metric, revenue_a AS value FROM agg
UNION ALL
SELECT customer_id, 'revenue_b' AS metric, revenue_b AS value FROM agg
UNION ALL
SELECT customer_id, 'revenue_c' AS metric, revenue_c AS value FROM agg;
```

**Day-over-day change with LAG:**
```sql
SELECT
    day,
    revenue,
    revenue - LAG(revenue) OVER (ORDER BY day) AS delta,
    (revenue - LAG(revenue) OVER (ORDER BY day))
        / NULLIF(LAG(revenue) OVER (ORDER BY day), 0) AS pct_change
FROM daily_revenue;
```

**Funnel (ordered event counts per user):**
```sql
WITH funnel AS (
    SELECT user_id,
           MAX(CASE WHEN event = 'signup'   THEN event_time END) AS signup_at,
           MAX(CASE WHEN event = 'activate' THEN event_time END) AS activate_at,
           MAX(CASE WHEN event = 'convert'  THEN event_time END) AS convert_at
    FROM events
    WHERE event IN ('signup', 'activate', 'convert')
    GROUP BY user_id
)
SELECT
    COUNT(*)                                                          AS signed_up,
    COUNT(activate_at) FILTER (WHERE activate_at > signup_at)         AS activated,
    COUNT(convert_at)  FILTER (WHERE convert_at  > activate_at)       AS converted
FROM funnel;
```
The `FILTER (WHERE ...)` clause is ANSI SQL for conditional aggregation; if your engine doesn't support it, use `COUNT(CASE WHEN ... THEN ... END)`.

These patterns together cover a surprising fraction of analytical questions you get asked. Memorize the shape of each one, and you'll find yourself writing fluent analytical SQL without having to think about it.

---

## 12. Testing your SQL: assertions as queries

One of the most useful habits you can build is writing **assertion queries** — queries that return rows only when something is wrong. They are the SQL equivalent of unit tests, and they are how dbt's test framework works under the hood.

```sql
-- unique primary key
SELECT customer_id, COUNT(*)
FROM dim_customers
GROUP BY customer_id
HAVING COUNT(*) > 1;

-- no NULL primary key
SELECT *
FROM dim_customers
WHERE customer_id IS NULL;

-- referential integrity
SELECT DISTINCT o.customer_id
FROM fct_orders o
LEFT JOIN dim_customers c ON c.customer_id = o.customer_id
WHERE c.customer_id IS NULL;

-- row counts within expected range
SELECT 'daily row count out of range' AS check
WHERE (SELECT COUNT(*) FROM fct_orders WHERE order_date = CURRENT_DATE - 1)
      NOT BETWEEN 10000 AND 1000000;

-- no duplicates after dedup logic
SELECT natural_key, COUNT(*)
FROM stg_events
GROUP BY natural_key
HAVING COUNT(*) > 1;
```

The convention is: *the query should return zero rows when the assertion holds.* Any result is a failure. This is the pattern dbt tests compile to, and it is the foundation of data quality checking (Module 08). You can run these manually as part of code review, check them into your dbt project as tests, or have your orchestrator run them as a post-task step.

---

## What to remember

- **SQL is the most important language in data engineering.** Fluent analytical SQL is the highest-leverage skill a working data scientist can acquire in 2026.
- **Think in logical pipeline order: FROM → WHERE → GROUP BY → HAVING → SELECT → DISTINCT → ORDER BY → LIMIT.** Aggregates exist only after GROUP BY; aliases exist only after SELECT.
- **CTEs make big queries readable.** Name every logical stage. Let each CTE do one thing. Treat them like paragraphs.
- **Window functions are the main event.** `ROW_NUMBER` for top-N-per-group, aggregate windows for running totals, `LAG`/`LEAD` for adjacent comparisons, the sessionization pattern for event grouping.
- **Joins duplicate rows on the one-to-many side and NULLs never match.** Aggregate before joining when the many side isn't needed row-by-row. `LEFT JOIN` + `WHERE` on the right side silently becomes `INNER JOIN`; put the predicate in the `ON` clause.
- **Performance is dominated by how much data you scan.** Partition pruning and sargable predicates matter far more than join algorithms. `WHERE col = x`, not `WHERE f(col) = x`.
- **EXPLAIN is your friend.** Start at the leaves, look at the cardinalities, find the biggest operator. Scans that should use partition pruning but don't are the #1 performance bug in analytical SQL.
- **Columnar warehouses don't really have indexes; they have partition keys and cluster keys.** Knowing how your main tables are partitioned and writing queries that match those keys is the whole game.
- **Memorize the core patterns: top-N-per-group, deduplicate-by-latest, gaps-and-islands, pivot/unpivot, day-over-day, funnel.** 80% of analytical questions fit one of them.
- **Test your SQL with assertion queries.** Zero rows means success; anything else is a bug. This is the foundation of dbt tests and data quality.

---

## Cheatsheet

**Logical execution order.**
```
FROM → JOIN → WHERE → GROUP BY → HAVING → SELECT → window → DISTINCT → ORDER BY → LIMIT
```

**Window function skeleton.**
```sql
<fn>(...) OVER (
    [PARTITION BY ...]
    [ORDER BY ...]
    [ROWS BETWEEN ... AND ...]
)
```

**Canonical windows.**
```sql
-- top N per group
ROW_NUMBER() OVER (PARTITION BY g ORDER BY s DESC)

-- running total
SUM(x) OVER (PARTITION BY g ORDER BY t
             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)

-- 7-row moving average
AVG(x) OVER (PARTITION BY g ORDER BY t
             ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)

-- previous / next row
LAG(x)  OVER (PARTITION BY g ORDER BY t)
LEAD(x) OVER (PARTITION BY g ORDER BY t)
```

**Sessionization.**
```sql
WITH flagged AS (
  SELECT *,
         CASE WHEN t - LAG(t) OVER (PARTITION BY u ORDER BY t) > INTERVAL '30 min'
              THEN 1 ELSE 0 END AS new_sess
  FROM events
)
SELECT *,
       SUM(new_sess) OVER (PARTITION BY u ORDER BY t
                           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS sess_id
FROM flagged;
```

**Top-N-per-group.**
```sql
SELECT * FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY g ORDER BY s DESC) AS rn
  FROM t
) x WHERE rn <= N;
-- or with QUALIFY (Snowflake/BQ/Databricks):
SELECT * FROM t
QUALIFY ROW_NUMBER() OVER (PARTITION BY g ORDER BY s DESC) <= N;
```

**Assertion query (dbt-style test).**
```sql
-- returns zero rows if the assertion holds
SELECT key, COUNT(*) FROM t GROUP BY key HAVING COUNT(*) > 1;
```

**Common join mistakes and fixes.**
```
Many-to-many inflation        → aggregate one side first
LEFT JOIN turns into INNER     → move predicate from WHERE into ON
NULL-keyed rows don't match    → IS NOT DISTINCT FROM or explicit coalesce
Forgot to dedup on composite   → add ROW_NUMBER() deduplication
```

**Performance checklist.**
```
☐ WHERE on partition column with simple comparison
☐ No function wrap on column side of comparison
☐ Column list explicit (no SELECT *)
☐ Aggregate the many side before joining when possible
☐ Limit to needed columns early in CTEs
☐ UNION ALL, not UNION, unless dedup is the point
☐ QUALIFY instead of subquery-plus-where for windows (if supported)
☐ EXPLAIN run on slow queries
```

**Style rules.**
```
Keywords UPPER, identifiers lower_snake_case
Every stage a named CTE
Always qualify columns with table aliases
Comment the why, not the what
Never SELECT * in production queries
Named constants beat magic numbers
```

---

## Where this module fits

You now have the SQL chops to express almost any analytical question cleanly and to recognize when a query is going to be expensive. The next module takes the natural next step: how do you *organize* a hundred SQL queries into a coherent, tested, version-controlled transformation project? That tool is dbt, and it has quietly become the most important piece of non-warehouse software in the modern data stack.
