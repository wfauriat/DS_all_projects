# French Media Election Tracker — Implementation Plan

## Context

A portfolio project that ingests RSS feeds from major French media outlets, stores headline metadata in Postgres, exposes a FastAPI backend, and serves an interactive dashboard tracking how often each 2027 presidential candidate is mentioned in each outlet over time. Goal: end-to-end training across data ingestion, SQL, analytics, data viz, and AWS deployment, plus a live, datable resume artifact ("I tracked the 2027 French campaign").

Today is 2026-05-15. First round is constitutionally scheduled for ~April 2027 (~11 months out). Sweet spot to build now on lower-volume pre-campaign data, scale through autumn, and harvest the official campaign Jan–Apr 2027.

### Already-decided constraints

- **Hosting**: AWS Free Tier through the election; migrate to a hybrid free stack (Vercel/Netlify + Neon Postgres) post-campaign so the dashboard stays online indefinitely.
- **Backend**: Python + FastAPI (Lambda + API Gateway HTTP API via Mangum).
- **Mention detection**: keyword matching MVP, eval harness in Phase 2, NER (spaCy/CamemBERT) only if eval shows it's needed. Schema designed to accommodate NER without migration.
- **Frontend**: interactive dashboard (filters, drilldown) for MVP; sentiment/extras as Phase 3 stretch.

---

## Architecture

```
                 EventBridge (cron 4×/day)
                          │
                          ▼
              ┌──────────────────────┐
              │  ingest-lambda       │   no VPC → has internet egress
              │  (fetches 20 feeds   │
              │   in parallel)       │
              └──────────┬───────────┘
                         │  raw XML
                         ▼
              ┌──────────────────────┐
              │  S3: media27-raw     │
              │  feeds/{date}/{src}  │   30-day Glacier, 365-day delete
              └──────────┬───────────┘
                         │  S3:ObjectCreated trigger
                         ▼
              ┌──────────────────────┐
              │  loader-lambda       │   in VPC → reaches RDS
              │  parse → dedup →     │   no internet needed
              │  upsert → extract    │
              └──────────┬───────────┘
                         │
                         ▼
              ┌──────────────────────┐
              │  RDS Postgres 16     │   db.t3.micro, single-AZ
              │  (Free Tier 12 mo)   │   in VPC, private
              └──────────▲───────────┘
                         │
              ┌──────────┴───────────┐
              │  api-lambda (FastAPI │
              │  via Mangum)         │   in VPC
              └──────────▲───────────┘
                         │
              API Gateway HTTP API
                         │
                         ▼
              ┌──────────────────────┐
              │  CloudFront ◀─ S3    │   static SPA build
              │  (frontend bundle)   │
              └──────────────────────┘
```

The **no-NAT split** is the key design choice. NAT Gateway (~$32/mo) is the single biggest "free tier killer." Splitting ingest (internet, no VPC) from loader (DB access, in VPC) removes the need.

---

## AWS service choices

| Concern | Choice | Why not the alternative |
|---|---|---|
| DB | RDS Postgres `db.t3.micro`, single-AZ, 20 GB gp3 | Aurora Serverless v2 has 0.5-ACU floor (~$43/mo), no Free Tier |
| API runtime | Lambda + API Gateway HTTP API + Mangum | EC2/Fargate add ops overhead; Lambda Free Tier is permanent (1M req + 400k GB-s/mo) |
| Cron | EventBridge Scheduler → ingest Lambda | trivial, free at this scale |
| Static frontend | S3 + CloudFront with OAC | CloudFront 1 TB egress free perpetually |
| Logs | CloudWatch Logs, **7-day retention** | default is infinite — silent cost growth |
| Alerts | Billing alarm at $1 and $5 (`us-east-1`) + AWS Budgets monthly cap + Cost Anomaly Detection | hard requirement; Free Tier surprises happen |
| Secrets | Secrets Manager for DB creds (~$0.40/mo) | small accepted spend; cleaner than env vars |
| IaC | AWS CDK in Python | one language across infra + app |
| Region | `eu-west-3` (Paris) for low latency to French users; billing alarms still in `us-east-1` | |

After Free Tier expires (~May 2027): Postgres dump → Neon free tier (autoscale-to-zero); frontend → Vercel; API → either Vercel serverless functions or a $5 Hetzner VM running uvicorn.

---

## Database schema (Postgres 16, all timestamps `TIMESTAMPTZ` UTC)

```
sources              (id SMALLINT PK, slug, outlet, section, feed_url,
                      lean, is_active, created_at)

articles             (id BIGSERIAL PK, source_id FK,
                      guid, url, title, summary, published_at, fetched_at,
                      lang, raw JSONB, content_hash BYTEA,
                      UNIQUE (source_id, guid),
                      UNIQUE (content_hash))
                      -- idx: (published_at DESC), (source_id, published_at DESC)

candidates           (id SMALLINT PK, slug, display_name, party, lean,
                      declared_at DATE, eligible BOOL, notes)

candidate_aliases    (id SERIAL PK, candidate_id FK, alias TEXT,
                      match_kind TEXT,           -- exact | wholeword | regex
                      requires_context TEXT,     -- e.g. "Marine" near "Le Pen"
                      is_active BOOL,
                      UNIQUE (candidate_id, alias))

mentions             (id BIGSERIAL PK, article_id FK, candidate_id FK,
                      field TEXT,                -- title | summary
                      match_text, start_offset INT, end_offset INT,
                      extractor TEXT,            -- keyword-v1 | spacy-... | ner-v1
                      extractor_version TEXT,
                      confidence REAL,
                      attributes JSONB,          -- NER extras: no migration needed
                      created_at,
                      UNIQUE (article_id, candidate_id, field, start_offset,
                              extractor, extractor_version))
                      -- idx: (candidate_id), (article_id),
                      --      GIN (attributes) for Phase 2

ingest_runs          (id BIGSERIAL PK, started_at, finished_at, source_id FK,
                      status, feed_http_status,
                      n_items_seen, n_articles_inserted, n_articles_skipped_dup,
                      n_mentions_inserted, error TEXT, meta JSONB)
```

**Phase 2** adds materialized view `mention_daily_counts (day, source_id, candidate_id, n_mentions, n_articles)` refreshed nightly with `REFRESH MATERIALIZED VIEW CONCURRENTLY`.

**Schema design notes worth keeping in mind**:
- `mentions.attributes` JSONB lets NER write rich metadata (entity type, surrounding context, model logits) without ALTER TABLE.
- `extractor_version` in the UNIQUE constraint allows reprocessing the same article with a new model version side-by-side.
- `articles.raw` JSONB holds the full parsed feed entry → can reprocess everything from the DB without re-fetching feeds.
- `candidate_aliases.requires_context` handles the Le Pen ambiguity (Marine vs Marion vs Jean-Marie) and similar cases declaratively.

---

## RSS feed sources (~20 feeds, 14 outlets, full political spectrum)

| Outlet | Lean | Feed URL |
|---|---|---|
| Le Monde (politique) | centre-left | `https://www.lemonde.fr/politique/rss_full.xml` |
| Le Figaro (politique) | centre-right | `https://www.lefigaro.fr/rss/figaro_politique.xml` |
| Libération (politique) | left | `https://www.liberation.fr/arc/outboundfeeds/rss/category/politique/?outputType=xml` |
| Mediapart | left/investigative | `https://www.mediapart.fr/articles/feed` |
| Le Point (politique) | centre-right | `https://www.lepoint.fr/arc/outboundfeeds/rss/category/politique/` |
| L'Express (politique) | centre | `https://www.lexpress.fr/arc/outboundfeeds/rss/politique.xml` |
| France Info (élections) | public-service | `https://www.franceinfo.fr/elections.rss` |
| France Info (politique) | public-service | `https://www.franceinfo.fr/politique.rss` |
| BFM TV (politique) | centre-right | `https://www.bfmtv.com/rss/politique/` |
| Les Échos (France) | business/centre | `https://syndication.lesechos.fr/rss/rss_france.xml` |
| La Croix | catholic/centre | `https://www.la-croix.com/RSS/une` |
| Marianne | sovereigntist | `https://www.marianne.net/rss.xml` |
| L'Humanité (politique) | hard-left | `https://www.humanite.fr/sections/politique/feed` |
| Le Parisien (politique) | centre | `https://feeds.leparisien.fr/leparisien/rss/politique` |
| 20 Minutes (politique) | centre | `https://www.20minutes.fr/feeds/rss-politique.xml` |
| RFI (politique française) | public/intl | `https://www.rfi.fr/fr/tag/politique-fran%C3%A7aise/rss` |
| Le Nouvel Obs (politique) | left | `https://www.nouvelobs.com/politique/rss.xml` |

**Unavailable**: AFP (closed public RSS), France Inter (podcast-oriented only), La Tribune (autodiscovery only — inspect HTML for `<link rel="alternate" type="application/rss+xml">`).

Each URL must be re-validated by the ingest code on first run; outlets sometimes change paths. The `sources` table allows hot-swapping a feed URL without a deploy.

---

## Python project layout

```
mediaElection27/
  pyproject.toml          # uv + ruff + mypy --strict + pytest
  Makefile                # dev, test, deploy targets
  .env.example
  app/
    config.py             # pydantic-settings
    db/
      engine.py           # SQLAlchemy async engine
      models.py           # ORM mirroring schema above
      migrations/         # alembic
    models/               # pydantic API schemas (separate from ORM)
    sources/
      registry.py         # canonical source list
      seed.py             # idempotent loader (reads sources.yaml, candidates.yaml)
    ingest/
      lambda_handler.py   # entry: fetch in parallel, write raw to S3
      fetcher.py          # async httpx with etag/if-modified-since
      parser.py           # feedparser + dateutil for pubDate normalisation
      loader_handler.py   # S3-event triggered: parse → dedup → upsert → extract
      dedup.py            # canonical URL + content_hash
    extract/
      base.py             # Extractor protocol → list[MentionDraft]
      keyword.py          # alias matcher with requires_context (v1)
      eval_runner.py      # Phase 2
      ner.py              # Phase 3, only if eval requires it
    api/
      main.py             # FastAPI app
      mangum_handler.py   # Lambda adapter
      routers/
        candidates.py     # GET /candidates
        sources.py        # GET /sources
        timeseries.py     # GET /timeseries?candidate_id=&source_id=&from=&to=&group_by=
        articles.py       # GET /articles?candidate_id=&source_id=&date=
        meta.py           # /healthz, /version, /stats
  frontend/
    package.json
    vite.config.ts
    src/...
  infra/cdk/              # CDK in Python
    stacks/
      network_stack.py
      data_stack.py       # RDS + Secrets Manager
      api_stack.py        # API Lambda + API Gateway HTTP API
      ingest_stack.py     # Ingest + Loader Lambdas, EventBridge, S3
      observability_stack.py  # alarms, budgets, anomaly detection
      frontend_stack.py   # S3 + CloudFront + OAC
    seeds/
      candidates.yaml
      sources.yaml
  scripts/
    init_db.py
    backfill.py           # replay from S3 raw → DB
    run_eval.py           # Phase 2
  tests/
    unit/
    integration/          # testcontainers-postgres
    eval/
      labeled_samples.jsonl  # Phase 2 gold set
  .github/workflows/
    ci.yml                # ruff, mypy, pytest
    deploy.yml            # cdk deploy on merge to main
```

---

## Frontend

**Stack**: Vite + React 19 + TypeScript + TanStack Query + TanStack Router (URL-state filters) + shadcn/ui + Tailwind v4 + Zod + MSW (offline dev fixtures).

**Charting**: **ECharts** via `echarts-for-react`. Best in class for interactive time-series with brushing, multi-line legends, stacked areas, downsampling — handles 14 outlets × ~12 candidates × 365 days without choking. Recharts struggles past ~5k points; Plotly's bundle (~3 MB) hurts TTI; Chart.js lacks the multi-axis interactivity.

UI language: **French** (target audience is French-speaking media watchers). Single locale for v1; i18n is overkill.

---

## MVP timeline (evenings/weekends, individual)

Targets: **deployed dashboard by 2026-06-19 (5 weeks)**, **eval harness done by 2026-07-10 (8 weeks)**.

| Week | Dates | Milestones |
|---|---|---|
| 1 | May 18–24 | Local dev. `docker compose up postgres`. Schema → alembic init. Seed `candidates.yaml` (~12 candidates incl. Mélenchon, Bardella, Philippe, Retailleau, Glucksmann, Ruffin, Tondelier, Faure, Wauquiez, Darmanin, Attal; Le Pen marked `eligible=false` per Oct 2025 Cour de cassation ruling). Seed `sources.yaml`. CLI `python -m app.ingest.run --once` prints article counts. |
| 2 | May 25–31 | `app.extract.keyword` v1 with aliases + `requires_context`. End-to-end against ~7 days local data. FastAPI app + four routers. `/timeseries` query in chart-ready shape. ~30 unit tests. |
| 3 | Jun 1–7 | Frontend scaffold: Vite + React + ECharts. Single chart, two filter panels, URL-state router, MSW fixtures. End of week: full app works locally. |
| 4 | Jun 8–14 | AWS infra. CDK stacks. `cdk deploy` produces RDS + Lambdas + API Gateway + EventBridge + S3 + CloudFront. Billing alarms at $1 and $5. Manual ingest in prod; verify mentions populate. |
| 5 | Jun 15–19 | Public push. CloudFront URL live (custom domain optional). Short README and write-up posted to portfolio site / LinkedIn. |
| 6 | Jun 22–28 | Phase 2 prep: hand-label 300 articles (~20/outlet) from already-ingested data into `tests/eval/labeled_samples.jsonl`. |
| 7 | Jun 29–Jul 5 | `run_eval.py` computes per-candidate precision/recall/F1. Decide on NER per the rule below. |
| 8 | Jul 6–10 | `mention_daily_counts` materialized view + nightly refresh Lambda. `/stats` page showing extractor F1. |
| Aug–Sep 2026 | Slow burn | Stacked-area share-of-voice chart, per-source drilldown, CSV export. Backfill missed feeds from S3 raw. Update candidate list as the field firms up. |
| Oct 2026–Mar 2027 | Campaign ramp | Phase 3 stretch (sentiment via CamemBERT) only if dashboard already feels complete. Optional: weekly digest email, Mastodon bot. |
| Apr–May 2027 | Election + migration | Watch it work. Snapshot DB nightly to S3 Glacier. Post-second-round: migrate to Neon + Vercel. |

---

## Risks and mitigations

- **AWS cost surprises** — billing alarms at $1 and $5 in `us-east-1`; AWS Budgets; Cost Anomaly Detection enabled; tag every resource `project=media27`; CDK enforces `removalPolicy=DESTROY` and `logRetention=ONE_WEEK`; S3 lifecycle rule (Glacier 30d, delete 365d). Treat the no-NAT design as non-negotiable.
- **RSS feed reliability** — each feed fetched independently; failures logged to `ingest_runs`; daily report Lambda emails on >2 consecutive failures of the same source so you can find the new URL.
- **Candidate list churn** — Le Pen's ineligibility was upheld by the Cour de cassation 2025-10-15; Mélenchon declared 2026-05-03; more entrants and alliances coming. Candidates and aliases are config (`candidates.yaml`, `candidate_aliases.yaml`), reseeded by `python -m app.sources.seed`. Ineligible candidates shown in a separate dashboard sub-panel.
- **Copyright / droit voisin** — store and display only title + URL + outlet + `published_at`. No body, no images, no thumbnails. Always link out. Footer: "Données: flux RSS publics. Cliquez pour lire l'article original." This posture is safe for non-commercial portfolio use.
- **Dedup edge cases** — `(source_id, guid)` for in-source; `content_hash = sha256(lower(strip(title)) || lower(strip(summary)))` for cross-source. Canonicalise URLs (strip `utm_*`, `fbclid`, fragments) before storage.
- **Time zone handling** — parse `pubDate` with `dateutil.parser`, normalise to UTC; if no offset, assume `Europe/Paris`; reject items >7 days in the future or <1990. All aggregations use `date_trunc('day', published_at AT TIME ZONE 'Europe/Paris')` so day boundaries match the editorial day, not UTC midnight.
- **Solo-dev scope creep** — public deploy at Week 5 is a hard milestone; refuse new features before it ships. Ideas go in `BACKLOG.md`, not GitHub issues.

---

## Phase 2 — Eval harness

Hand-label 300 articles stratified by outlet (~20 each) and date (random over the most recent month). For each `(article, candidate)` pair, record `[]` or `[{candidate_slug, field, start, end, surface}]`. A "correct" mention is an explicit reference to the candidate as a political figure — including unambiguous surnames, full names, and uniquely-identifying titles. Excluded: family namesakes, historical references (Le Pen père), reported speech where the speaker is the topic.

For each candidate `c`: precision, recall, F1. Macro- and micro-averaged.

**NER decision rule**: ship keyword-only if macro-F1 ≥ 0.90 **and** every candidate's recall ≥ 0.85 **and** no candidate's precision < 0.80. Otherwise: if recall failures dominate, expand `candidate_aliases` and rerun before adding NER. If precision failures dominate, add a context-window check first. NER (spaCy `fr_core_news_md` or CamemBERT-NER) only justified when keyword + context windows still leave macro-F1 below 0.85.

---

## Verification (end-to-end)

1. **Local pipeline**: `docker compose up postgres && python scripts/init_db.py && python -m app.ingest.run --once` → `psql` shows >0 rows in `articles`, `mentions`, `ingest_runs`.
2. **API**: `uvicorn app.api.main:app` → `curl localhost:8000/timeseries?candidate_id=1&from=2026-05-01&to=2026-05-15` returns chart-ready JSON.
3. **Frontend**: `npm run dev` → dashboard loads, filters work, time-series re-renders on filter change.
4. **AWS deploy**: `cdk deploy` succeeds; CloudFront URL serves the SPA; `/healthz` on API Gateway returns 200; one manual EventBridge "Run rule" execution populates `articles` in RDS.
5. **Cost guardrails verified**: billing alarms visible in CloudWatch (`us-east-1`); AWS Budgets shows `media27` budget; Cost Anomaly Detection enabled.
6. **Phase 2**: `python scripts/run_eval.py` outputs per-candidate F1 table to `eval-report-{date}.md`.

---

## Critical files (target paths — all to be created)

- `app/db/models.py` — SQLAlchemy ORM mirroring the schema; foundation for everything.
- `app/ingest/loader_handler.py` — S3-triggered Lambda owning parse → dedup → upsert → extract; the most failure-prone piece.
- `app/extract/keyword.py` — alias matcher with `requires_context`; analytical heart of v1 and what Phase 2 evaluates.
- `app/api/routers/timeseries.py` — chart-feeding endpoint; shape and indexing here determine dashboard latency.
- `infra/cdk/stacks/ingest_stack.py` — wires EventBridge → ingest Lambda → S3 → loader Lambda → RDS, including the no-NAT split that keeps the project inside Free Tier.

---

## Assumptions (flag if any are wrong)

- Source code public on GitHub (portfolio piece).
- CI/CD via GitHub Actions (`cdk deploy` on merge to main).
- AWS region `eu-west-3` (Paris); billing alarms in `us-east-1` (only region that emits billing metrics).
- Custom domain optional — `*.cloudfront.net` is fine for v1.
- Dashboard UI in French only.
- A new AWS account exists or will be created (12-month Free Tier resets per account).
