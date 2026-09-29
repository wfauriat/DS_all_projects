# CLAUDE.md

Personal sandbox for **hands-on practice** in data science, data engineering and Python. The user drives step by step; Claude supplies commands, idioms and small scripts on request.

## How to work here

- Short interactions. Answer what was asked, then suggest at most one sensible next step.
- Don't over-engineer: no frameworks, packaging, tests or abstractions unless asked. Plain scripts and SQL files are the norm.
- Give things the user can run and poke at (bash one-liners, `.sql` files, small Python scripts). Test them before handing them over when feasible.
- Point out what the output shows (gotchas, dirty data findings), because the goal is learning, not just working code.
- Exercises: offer ideas, but let the user try first. Give a skeleton or solution only when asked.
- Explore first, formalize later: throwaway exploration, then proper ingestion with explicit schema.

## Reference material (large: read by headings only, then the relevant section)

- `module1_DE.md`: Module 01, SQL for Analytics (execution order, joins and NULL traps, CTEs, window functions, lateral joins, set ops, EXPLAIN, indexes, readable SQL, patterns, assertion-query testing). Later modules cover dbt.
- `module1_DS.md`: Module 01, Python for Data Science (idioms, collections/itertools/functools, type hints, dataclass/NamedTuple/TypedDict, pathlib, environments with uv, profiling, logging).

Use `grep -n '^#\{1,2\} ' module1_*.md` to get the outline.

## Data

Everything in `data/` is **synthetic**, produced by `generator.py` (seeded, stdlib only, `python generator.py`). It's a marketplace with a snowflake schema:

- Dimensions (CSV): `dim_country` <- `dim_city` <- `dim_customer` / `dim_seller`; `dim_category` (self-referencing parent); `dim_product`.
- Facts (CSV): `fact_orders`, `fact_order_items`.
- Semi-structured: `payments.jsonl` (nested nullable `card`), `web_events.log` (text log with ~40 junk lines).
- Deliberately dirty: empty strings for missing values (emails, seller ratings, discounts), dates as strings, junk log lines.

Regenerating overwrites `data/`. Row counts: 200 customers, 25 sellers, 120 products, 1500 orders, ~2640 items, 6000 good log lines.

## Files so far

- `explore.sh`, `bash_cheatsheet.md`: bash exploration of `data/` (head/cut/awk/grep/sort/jq).
- `load_raw.sql`, `explore.sql`: throwaway SQLite load (all TEXT) into `scratch.db` and exploration queries. Recreate with `rm scratch.db; sqlite3 scratch.db < load_raw.sql`.

## Current learning track: SQL fundamentals, step by step

The user is building `custom_explore.sql` by hand while Claude walks through one concept per step, with a small "your turn" exercise each time. After each step is done, Claude adds it to `sql_cheatsheet.md` (which has a trailing comment listing planned sections).

- Covered and in `sql_cheatsheet.md`: SQLite CLI basics, SELECT/FROM, WHERE, ORDER BY and aggregates (NULL handling in avg/count), GROUP BY, HAVING and the logical pipeline order, INNER JOIN (cardinality, row multiplication), LEFT JOIN (anti-join, filter in ON vs WHERE), NULLs (three-valued logic, NOT IN trap, null-safe equality), CTEs (incl. recursive), window functions part 1 (OVER/PARTITION BY, aggregate windows, ROW_NUMBER/RANK/DENSE_RANK, top-N-per-group via CTE since SQLite has no QUALIFY).
- Also covered: window functions part 2 (LAG/LEAD, month-over-month, running total and moving average frames, ROWS vs RANGE, LAST_VALUE gotcha).
- In progress: nothing pending.
- **TODO (pick up next session)**: the full checklist of remaining SQL topics is in `sql_cheatsheet.md` under "TODO: SQL topics not yet covered". Suggested order: `CASE WHEN` + conditional aggregation (biggest gap), then subqueries/`EXISTS`/`DISTINCT`, then set operations (`UNION`/`INTERSECT`/`EXCEPT`); then DDL/DML + transactions together with the Python ingestion into SQLite. Scalar functions, views/`CREATE TABLE AS`, `EXPLAIN`, dialect differences, JSON functions, assertion queries and web-log sessionization follow as needed. Indexes are deliberately deprioritised. Tick the boxes in the cheatsheet as topics are done.
- Session style so far: user asks for the next step, Claude gives concept plus testable snippets (verified by running against `scratch.db` first); user sometimes says "no exercise, I'll just test". Cheatsheet is updated only when the user says so.
- Recurring gotchas already met: all columns are TEXT after raw import (cast for numbers, text sorts alphabetically); `''` is not NULL (CLI `.import` never produces NULL); SQLite is lenient where other engines are strict (aliases in WHERE/HAVING, non-aggregated columns with GROUP BY), so flag non-portable habits.
- When the user pastes results, check them against the question actually asked (direction of comparisons, count vs sum) and say plainly when a query answers a different question.

## Stack

- Now: bash + Python 3 + SQLite (`sqlite3` CLI 3.45).
- Possibly later, for more involved analytics: DuckDB, dbt.

## Next steps (planned)

Proper ingestion into SQLite: explicit `CREATE TABLE` (types, PK/FK, CHECK), Python loader with rejects file for bad rows, JSONL and log parsers. Known open finding: 2 of 1500 payments don't reconcile with item totals.
