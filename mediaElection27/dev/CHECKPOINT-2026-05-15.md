# Checkpoint — 2026-05-15

End-of-session notes after the Week 1 → Week 3 push. Captures observations
that are easy to forget but useful to have on hand when picking the project
back up.

---

## Bugs caught during verification (and what they teach)

1. **Cross-source `content_hash` UNIQUE blocked dual-feed publishing.**
   France Info publishes the same article in both `politique` and `élections`
   feeds. The original schema had `UNIQUE (content_hash)` which rolled back
   the second insert — and corrupted the whole `_record_run` transaction
   alongside it. Two fixes in migration `0002` + `app/ingest/run.py`:
   - dropped the unique, kept a non-unique index (cross-source republication
     is now a separate row by design — see `cheatsheet_sql.md` query 4 of §11
     for "wire detection")
   - split persist + record into two transactions so a row-level failure in
     persist can't poison the audit log write
   *Lesson:* schema constraints encode an editorial choice. "Same content,
   different outlet" is a design question, not a duplication bug.

2. **`articles.py` had `select(article_id, distinct(candidate_id))`** —
   emits invalid Postgres syntax (`SELECT mentions.article_id, DISTINCT …`).
   Fixed to `.distinct()` on the select. Caught by the API smoke tests, not
   by mypy.
   *Lesson:* type-checks don't see SQL semantics. Integration tests do.

3. **`/timeseries` had inconsistent registry filtering** — returned only
   referenced candidates when data existed, ALL candidates when empty. Caught
   by `test_timeseries_no_data_window_is_well_formed`. Now always returns
   the full registry as a stable lookup table.
   *Lesson:* the empty-data path is its own design surface.

4. **`pydantic-settings` `.env` silently masks new defaults.** Updated the
   browser-prefixed `INGEST_USER_AGENT` default in `app/config.py` after
   Marianne started 403'ing — but the user's `.env` (copied from the OLD
   `.env.example` earlier) still had the old value, and `.env` wins over
   defaults. Symptom: Marianne kept 403'ing even though the test in isolation
   showed the new UA worked.
   *Lesson — and this generalises to AWS:* settings precedence is
   `defaults < .env < real env vars < secrets injection`. When a config
   change "doesn't take effect", **inspect the runtime `settings.X` value
   directly**, not the source default.

5. **asyncpg + FastAPI TestClient = `Event loop is closed` on teardown.**
   Each TestClient call spins up a new event loop; the cached async engine's
   connection pool is created in one loop but its asyncpg `_terminate_graceful_close`
   fires against another. Fix: `asyncio_default_test_loop_scope = "session"`
   in `pyproject.toml` so all tests share one loop, plus `httpx.AsyncClient`
   over `ASGITransport` instead of `TestClient` for proper async semantics.
   *Lesson:* async test fixtures need explicit loop scope; the default
   (`function`) is wrong for any test that touches a pooled async resource.

---

## Real-world RSS surprises

| Outlet | Behaviour | Workaround |
|---|---|---|
| **Le Parisien** | 100 items per fetch, **zero per-item dates** | Channel `lastBuildDate` fallback in `parser.py`. URLs literally embed `dd-mm-yyyy` — a per-source date extractor would give per-item precision. |
| **Marianne** | Discriminates on UA pattern | Identifying-but-Mozilla-prefixed UA passes (`Mozilla/5.0 (compatible; mediaElection27/0.1; +…)`). Bare custom UAs get 403. |
| **Les Échos** | Akamai blocks all paths from server IPs | Even a real Chrome UA gets 403. Truly needs a headless browser on a residential IP. Disabled (`is_active: false`). |
| **La Croix** | Removed public RSS entirely | `/RSS/une` 302's to `/choix_redac/rss` which is 404. Disabled. |
| **AFP** | Closed public RSS years ago | Skipped from day one. |

Pattern: anti-scraping is now standard, even for syndication endpoints. The
identifying-Mozilla UA gets you most of the way; the remaining ~10% need a
real browser.

---

## Design decisions that already paid off

1. **`mentions.attributes` JSONB + `extractor_version` in the UNIQUE
   constraint.** We haven't built NER yet, but the table is shaped so a
   second extractor can write rows alongside `keyword v1` with no
   migration. The schema already proves the design — `attributes`
   correctly carries `{alias, match_kind}` for keyword-v1 rows; NER will add
   `{entity_type, surrounding_context, model_logits}` to the same column.

2. **`requires_context` declarative disambiguation.** "Le Pen" alias only
   fires when "Marine" appears in the same field. Verified by query 4 of
   the eval session: every `le-pen-marine` mention had "Marine Le Pen" or
   "Marine" co-occurring. Marion-Maréchal references correctly excluded.
   The whole disambiguation logic is in YAML, not code.

3. **Tri-state URL filter semantics** (`null` / `[]` / `[ids]`). The
   simpler "always a list, empty means show-all" model breaks the
   "deselect all → show none" UX. The tri-state matches the user's mental
   model with three URL shapes:
   - `?candidates` missing → show all
   - `?candidates=` → show none
   - `?candidates=2,14` → show those

4. **No-NAT architecture (planned, not yet built).** Local `run.py` already
   collapses both halves (fetch + load) into one process — the prod CDK stack
   in Week 4 will split them across two Lambdas (one outside VPC for internet,
   one inside VPC for RDS) without changing the application logic. The
   abstraction is the right one.

---

## Quantitative observations from the corpus (596 articles, 275 mentions)

- **Extractor v keyword vs naive title-ILIKE** (cheatsheet §11):
  Mélenchon 15 vs 15, Bardella 34 vs 34 — perfect agreement for unambiguous
  surnames. Le Pen 7 vs 10 — extractor 30% more conservative, correctly
  rejecting Marion-Maréchal references. This is the Phase 2 eval harness's
  payoff already partially visible.
- **Top of leaderboard, May 2026**: Bardella > Macron > Attal > Philippe.
  Real picture of who actually drives campaign-period coverage.
- **Macron at #2** despite `eligible=false` — validates the "track
  reference figures" decision. He commands almost as much coverage as
  Bardella (61 / 64 mentions) because he's still the sitting president.
- **5 cross-source `content_hash` collisions** out of 591 distinct hashes —
  early evidence that the wire-detection use case is real. Will grow as
  the official campaign accelerates.
- **Le Parisien dominates "most recent" lists** because all 100 items share
  the same `lastBuildDate`. Daily-bucket analytics correct; "what's hot
  right now" needs the per-URL date extractor.

---

## Operational gotchas worth a sticky note

- **`docker compose down` keeps volumes; `-v` is the destructive variant.**
  Trick to remember: there is no recovery from `down -v`.
- **`usermod -aG docker $USER` doesn't propagate to existing shells.**
  `newgrp docker` activates without logout, or `exec newgrp docker` to
  replace the current shell.
- **Cron's `@reboot` fires before Docker is necessarily up.** The wrapper
  `wait_for_postgres()` (60s grace) handles the race. Same pattern will
  apply when we move to Lambda + RDS in Week 4.
- **The dev DB has 596 articles + 275 mentions on May 15.** Growth rate
  visible via `SELECT date_trunc('day', fetched_at), COUNT(*) FROM articles
  GROUP BY 1 ORDER BY 1;`. Useful baseline if anything looks off later.

---

## Things likely to bite later (ordered by horizon)

| When | What | Why |
|---|---|---|
| Week 4 (CDK) | NAT Gateway accidentally provisioned via VPC defaults | Audit the CDK stack carefully; ~$32/mo silently. |
| Week 4 (CDK) | CloudWatch logs default to infinite retention | Set `logRetention=ONE_WEEK` on every log group at construct time. |
| Week 4 (CDK) | Forgotten Elastic IPs / unused EBS volumes | Tag everything `project=media27`, set up Cost Anomaly Detection. |
| Week 6 (eval) | Macron mentions are noise-heavy (Brigitte rumours, foreign policy) | Sentiment + topic tagging eventually warranted; not yet. |
| Anytime | Le Parisien same-timestamp clustering skews "what's recent" | Per-source URL date extractor. |
| Anytime | Smart quotes / NBSPs in titles produce different `content_hash` | Unicode-normalize before hashing. Wire-detection accuracy improves. |
| Anytime | Share-of-voice double-counts when an article matches multiple candidates | Document carefully which definition each chart uses. |

---

## Pedagogical observations (about how the project is structured)

The `keyword vs ILIKE` comparison query (cheatsheet §11) is the single most
useful artefact in the project so far — it visibly demonstrates why a
proper schema (aliases + `requires_context` + per-mention rows) beats naive
title-grep. Worth keeping handy for write-ups and demo conversations.

`PLAN-v1.md` has held up well — almost every load-bearing decision has
either been validated or proved still-the-right-call. Two minor deviations
worth noting:

1. The plan said TanStack Router for the frontend; chose `react-router-dom`
   instead because the dashboard is a single page and doesn't benefit from
   file-based routing or typed search params. Easy to swap if multi-route
   navigation arrives.
2. The plan said shadcn/ui; using plain Tailwind utilities for v1 and
   deferring shadcn until the dashboard has more UI surface area.

Both deviations documented in the Week 3 commit message.

---

## Status one-liner for the future

> Local end-to-end works: cron-driven RSS ingest → Postgres → keyword
> extractor → FastAPI → React+ECharts dashboard with French UI. 596
> articles / 275 mentions in the dev DB as of 2026-05-15. Next chunk:
> AWS CDK (Week 4) to put the whole thing on the public internet.
