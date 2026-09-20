# SQL exploration cheatsheet — mediaElection27

Run inside `docker compose exec postgres psql -U media27 -d media27` (or `make psql`).
Each section introduces one new concept, building on the last.

---

## 0. psql meta-commands (not SQL, but you'll use them constantly)

```
\dt                      -- list tables
\d articles              -- describe a table (columns, indexes, constraints)
\l                       -- list databases
\x                       -- toggle expanded vertical display (for wide rows)
\timing                  -- show query execution time after each query
\q                       -- quit
```

---

## 1. SELECT, COUNT, LIMIT

```sql
-- How much data do we have?
SELECT COUNT(*) FROM articles;
SELECT COUNT(*) FROM sources;
SELECT COUNT(*) FROM candidates;

-- Eyeball a few rows (LIMIT keeps you from drowning in output):
SELECT id, title FROM articles LIMIT 5;

-- Pick specific columns instead of SELECT * — habit-forming for performance:
SELECT outlet, slug, lean FROM sources LIMIT 5;
```

---

## 2. ORDER BY

```sql
-- 10 most recent articles (DESC = descending):
SELECT id, published_at, title
FROM articles
ORDER BY published_at DESC
LIMIT 10;

-- 10 oldest articles:
SELECT id, published_at, title
FROM articles
ORDER BY published_at ASC
LIMIT 10;
```

---

## 3. WHERE — filters

```sql
-- Exact match:
SELECT slug, outlet FROM sources WHERE lean = 'left';

-- IN — multi-value match:
SELECT slug, outlet FROM sources WHERE lean IN ('left', 'hard-left', 'centre-left');

-- Date filter:
SELECT id, title, published_at
FROM articles
WHERE published_at >= '2026-05-15'
ORDER BY published_at DESC LIMIT 10;

-- Combine with AND/OR/NOT:
SELECT slug, outlet, lean
FROM sources
WHERE is_active = true AND lean IN ('right', 'hard-right');

-- Pattern matching with LIKE (case-sensitive) and ILIKE (case-insensitive):
SELECT id, title FROM articles WHERE title ILIKE '%macron%' LIMIT 10;
SELECT id, title FROM articles WHERE title ILIKE '%mélenchon%' LIMIT 10;
SELECT id, title FROM articles WHERE title ILIKE '%bardella%' OR title ILIKE '%le pen%' LIMIT 10;

-- IS NULL / IS NOT NULL — for nullable columns:
SELECT slug, declared_at FROM candidates WHERE declared_at IS NULL;
```

---

## 4. GROUP BY + aggregates

```sql
-- Articles per source:
SELECT source_id, COUNT(*) AS n
FROM articles
GROUP BY source_id
ORDER BY n DESC;

-- Sources per political lean:
SELECT lean, COUNT(*) AS n_sources
FROM sources
GROUP BY lean
ORDER BY n_sources DESC;

-- Aggregate functions: MIN, MAX, AVG, SUM, COUNT, COUNT(DISTINCT ...)
SELECT
    MIN(published_at) AS oldest,
    MAX(published_at) AS newest,
    COUNT(*) AS total,
    COUNT(DISTINCT source_id) AS distinct_sources
FROM articles;

-- HAVING — like WHERE, but for groups:
SELECT source_id, COUNT(*) AS n
FROM articles
GROUP BY source_id
HAVING COUNT(*) > 50
ORDER BY n DESC;
```

---

## 5. JOIN — combining tables

```sql
-- Articles with their outlet name (most common JOIN you'll write):
SELECT s.outlet, a.published_at, LEFT(a.title, 70) AS title
FROM articles a
JOIN sources s ON s.id = a.source_id
ORDER BY a.published_at DESC
LIMIT 10;

-- Same thing with explicit table aliases:
--   `articles a` means "table articles, refer to it as a"
--   `sources s` means "table sources, refer to it as s"
-- Aliases let you write s.outlet instead of sources.outlet.

-- JOIN + GROUP BY — articles per outlet:
SELECT s.outlet, COUNT(*) AS n
FROM articles a
JOIN sources s ON s.id = a.source_id
GROUP BY s.outlet
ORDER BY n DESC;

-- JOIN + WHERE — Macron-mention count per outlet
-- (Week 2 will replace this with the proper mentions table):
SELECT s.outlet, COUNT(*) AS n_macron
FROM articles a
JOIN sources s ON s.id = a.source_id
WHERE a.title ILIKE '%macron%'
GROUP BY s.outlet
ORDER BY n_macron DESC;
```

---

## 6. Multiple JOINs

```sql
-- Candidates with their alias count:
SELECT c.display_name, COUNT(ca.id) AS n_aliases
FROM candidates c
LEFT JOIN candidate_aliases ca ON ca.candidate_id = c.id
GROUP BY c.id, c.display_name
ORDER BY n_aliases DESC;
-- LEFT JOIN: keep candidates even if they have zero aliases.
-- INNER JOIN (the default just `JOIN`) would drop them.

-- For each candidate, list the aliases as a comma-separated string:
SELECT c.display_name, STRING_AGG(ca.alias, ', ' ORDER BY ca.alias) AS aliases
FROM candidates c
JOIN candidate_aliases ca ON ca.candidate_id = c.id
GROUP BY c.display_name
ORDER BY c.display_name;
```

---

## 7. Date functions

```sql
-- Articles per day (DATE_TRUNC chops a timestamp down to a day/hour/etc.):
SELECT DATE_TRUNC('day', published_at) AS day, COUNT(*) AS n
FROM articles
GROUP BY day
ORDER BY day DESC;

-- Articles per day, in Paris time (Plan-spec'd analytics window):
SELECT DATE_TRUNC('day', published_at AT TIME ZONE 'Europe/Paris') AS day, COUNT(*) AS n
FROM articles
GROUP BY day
ORDER BY day DESC;

-- Articles in the last 24 hours:
SELECT COUNT(*) FROM articles WHERE published_at > NOW() - INTERVAL '24 hours';

-- Articles per outlet per day (the shape of the dashboard data):
SELECT s.outlet,
       DATE_TRUNC('day', a.published_at AT TIME ZONE 'Europe/Paris')::date AS day,
       COUNT(*) AS n
FROM articles a
JOIN sources s ON s.id = a.source_id
GROUP BY s.outlet, day
ORDER BY day DESC, n DESC;
```

---

## 8. CASE — inline conditional

```sql
-- Bucket articles by political lean of the source:
SELECT
    CASE
        WHEN s.lean IN ('hard-left', 'left', 'centre-left') THEN 'left-of-centre'
        WHEN s.lean IN ('hard-right', 'right', 'centre-right', 'sovereigntist') THEN 'right-of-centre'
        ELSE 'other'
    END AS bucket,
    COUNT(*) AS n
FROM articles a
JOIN sources s ON s.id = a.source_id
GROUP BY bucket
ORDER BY n DESC;
```

---

## 9. DISTINCT

```sql
-- Distinct outlets that have published in the DB:
SELECT DISTINCT s.outlet
FROM articles a
JOIN sources s ON s.id = a.source_id
ORDER BY s.outlet;

-- Distinct article count per outlet (DISTINCT inside aggregate):
-- (each cross-feed-republished article counted once per outlet)
SELECT s.outlet, COUNT(DISTINCT a.content_hash) AS n_unique_articles
FROM articles a
JOIN sources s ON s.id = a.source_id
GROUP BY s.outlet
ORDER BY n_unique_articles DESC;
```

---

## 10. Subqueries and CTEs

```sql
-- Outlets that have published more than 50 articles:
SELECT outlet
FROM sources
WHERE id IN (
    SELECT source_id FROM articles
    GROUP BY source_id HAVING COUNT(*) > 50
);

-- Same idea using a CTE (Common Table Expression — easier to read for big queries):
WITH busy_sources AS (
    SELECT source_id, COUNT(*) AS n
    FROM articles GROUP BY source_id HAVING COUNT(*) > 50
)
SELECT s.outlet, bs.n
FROM busy_sources bs
JOIN sources s ON s.id = bs.source_id
ORDER BY bs.n DESC;
```

---

## 11. A real "share of voice" preview

```sql
-- Which outlets mention which candidates (by title pattern, before
-- Week 2's proper extractor uses the mentions table):
SELECT s.outlet,
       COUNT(*) FILTER (WHERE a.title ILIKE '%le pen%')   AS n_le_pen,
       COUNT(*) FILTER (WHERE a.title ILIKE '%bardella%') AS n_bardella,
       COUNT(*) FILTER (WHERE a.title ILIKE '%macron%')   AS n_macron,
       COUNT(*) FILTER (WHERE a.title ILIKE '%mélenchon%') AS n_melenchon,
       COUNT(*) AS n_total
FROM articles a
JOIN sources s ON s.id = a.source_id
GROUP BY s.outlet
ORDER BY n_total DESC;
-- COUNT(*) FILTER (WHERE …) is "count of rows in this group matching the condition".
-- Equivalent to SUM(CASE WHEN ... THEN 1 ELSE 0 END) but cleaner.
```


```sql
  SELECT s.outlet,
         COUNT(*) FILTER (WHERE a.title ILIKE '%le pen%')   AS n_le_pen,
         COUNT(*) FILTER (WHERE a.title ILIKE '%bardella%') AS n_bardella,
         COUNT(*) FILTER (WHERE a.title ILIKE '%macron%')   AS n_macron,
         COUNT(*) FILTER (WHERE a.title ILIKE '%mélenchon%') AS n_melenchon,
         COUNT(*) AS n_total
  FROM articles a JOIN sources s ON s.id = a.source_id
  GROUP BY s.outlet
  ORDER BY n_macron DESC;            -- "which outlets cover Macron most"
```


```sql
  SELECT s.lean, s.outlet,
         COUNT(*) FILTER (WHERE a.title ILIKE '%macron%')   AS n_macron,
         COUNT(*) FILTER (WHERE a.title ILIKE '%mélenchon%') AS n_melenchon,
         COUNT(*) FILTER (WHERE a.title ILIKE '%bardella%') AS n_bardella,
         COUNT(*) AS n_total
  FROM articles a JOIN sources s ON s.id = a.source_id
  GROUP BY s.lean, s.outlet
  ORDER BY 
    CASE s.lean
      WHEN 'hard-left'    THEN 1
      WHEN 'left'         THEN 2
      WHEN 'centre-left'  THEN 3
      WHEN 'centre'       THEN 4
      WHEN 'public'       THEN 5
      WHEN 'centre-right' THEN 6
      WHEN 'right'        THEN 7
      WHEN 'hard-right'   THEN 8
      WHEN 'sovereigntist'THEN 9
      ELSE 99
    END;
```



That last query is essentially what the Week 2 keyword extractor + `/timeseries`
endpoint will do properly — using the `mentions` table instead of `ILIKE` on titles.
Worth running both before and after, to see why the proper schema (with aliases +
per-mention rows + `requires_context`) is more rigorous than naive title-grep.

Try modifying these — change the candidate, change the time window, swap `outlet`
for `lean`. SQL gets fluent through tinkering.





-- For each outlet, the % of its candidate-mentioning articles that name each candidate
```sql
SELECT s.outlet, c.display_name,
        COUNT(DISTINCT m.article_id) AS n_articles,
        ROUND(100.0 * COUNT(DISTINCT m.article_id) / NULLIF(SUM(COUNT(DISTINCT
m.article_id))
            OVER (PARTITION BY s.outlet), 0), 1) AS pct_share
FROM mentions m
JOIN articles a ON a.id = m.article_id
JOIN sources s ON s.id = a.source_id
JOIN candidates c ON c.id = m.candidate_id
GROUP BY s.outlet, c.display_name
ORDER BY s.outlet, n_articles DESC;
```