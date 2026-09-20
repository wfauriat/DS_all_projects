# mediaElection27 — rebuild specification

A self-contained design document describing what was built. The goal is
to enable a re-implementation from scratch in a different repository,
with an AI assistant guiding decisions but **the engineer writing the
code**. The document deliberately specifies *contracts and rationale*
rather than supplying ready-to-paste code.

## 0. What this is

A French political-media analytics dashboard for the 2027 French
presidential election. It periodically ingests RSS feeds from ~15
French outlets, extracts candidate mentions from headlines, and serves
a public dashboard showing per-candidate / per-outlet trends over time.

End artefact: a public URL serving a React+TypeScript SPA backed by a
FastAPI service and Postgres database, fully hosted on AWS Free Tier,
costing < $5/month gross.

## 1. Load-bearing design decisions

Read these before writing any code. Every later decision depends on
these holding.

1. **Read-only public dataset, no user accounts.** No auth anywhere.
   Removes whole classes of complexity and lets CORS stay permissive
   during development.

2. **Strict cost discipline.** Target: stays free for ~12 months
   spanning the campaign. After 2027-04 election: migrate to
   Neon (Postgres) + Vercel (frontend) for indefinite portfolio
   uptime. AWS Free Tier (mid-2025 onwards) gives 6 months of
   time-limited free services + $200 credits for new accounts.

3. **No NAT Gateway in the VPC.** A NAT Gateway costs ~$32/month per
   AZ, more than every other component combined. The pipeline is
   structured so the only component needing public internet (the RSS
   ingest) runs *outside* the VPC, where Lambda gets free internet
   egress. Components needing RDS (loader, API) run *inside* a VPC
   with `PRIVATE_ISOLATED` subnets only — no IGW, no NAT.

4. **Configuration is data, not code.** Both the candidate registry
   and the source list live in YAML files reseeded into Postgres on
   change. Mid-campaign entrants, ineligibility rulings, and feed-URL
   changes don't require code deploys.

5. **Schema is forward-compatible with NER.** A `JSONB attributes`
   column on `mentions` and `(extractor, extractor_version)` in the
   uniqueness key let a future Named Entity Recognition extractor
   write rows alongside the keyword extractor with no migration.

6. **No article bodies stored.** Only `title + summary + url + outlet +
   published_at`. This is the *droit voisin* (neighbouring-rights)
   compliance posture for non-commercial portfolio use of public RSS
   feeds. Display always links out to the original article. Footer
   text mentions the data origin.

7. **Eligibility is a first-class flag.** The Cour de cassation
   upheld Marine Le Pen's ineligibility on 2025-10-15. She remains
   tracked (her mentions are journalistically significant) but shown
   in a separate "ineligible" panel in the UI.

8. **French-only UI; English code identifiers.** All visible strings
   are in French (the audience). All variable names, comments, commit
   messages, and code reviews are in English (the industry convention).
   No mixing.

9. **Dashboard is single-page.** All views (Dashboard, Articles,
   Leaderboard, Share-of-Voice, Source Drilldown) are routes within
   one SPA. URL state is bookmarkable for every filter combination.

10. **The keyword extractor is v1; an eval harness decides v2.**
    Phase 1 ships with regex+alias matching plus per-alias
    disambiguation tokens. Phase 2 hand-labels 300 articles and
    computes per-candidate F1. NER (spaCy `fr_core_news_md` or
    CamemBERT-NER) is only adopted if macro-F1 < 0.85 after alias
    expansion.

## 2. Tech stack

### Backend

| Concern | Choice | Why |
|---|---|---|
| Language | Python 3.12 | Type hints, async, broad library support |
| Package mgmt | uv + pyproject.toml | Fast, single-file deps spec |
| Lint / format | ruff | One tool, fast |
| Type-checker | mypy `--strict` | Catches whole-class bugs at edit time |
| Web framework | FastAPI | Pydantic-typed contracts, async-first |
| Async DB | SQLAlchemy 2.0 + asyncpg | Mature, native async, declarative ORM |
| Sync DB (alembic, seed) | SQLAlchemy + psycopg 3 | Alembic doesn't speak async cleanly |
| Schema migrations | Alembic | Reversible, branch-aware |
| HTTP client (ingest) | httpx | Async, sane defaults |
| RSS parser | feedparser | Forgiving on broken feeds |
| Config | pydantic-settings | Env-var + .env layered |
| Logging | structlog | JSON-shaped, contextvars-aware |
| Test framework | pytest + testcontainers-postgres | Real Postgres in CI, not mocks |

### Frontend

| Concern | Choice | Why |
|---|---|---|
| Build tool | Vite | Fast HMR, ES modules natively |
| Framework | React 19 + TypeScript | Industry default, mature ecosystem |
| Routing | react-router-dom | Light enough; TanStack Router would also work |
| Data fetching | TanStack Query | Cache, refetch, loading/error states |
| Schema validation | Zod | Mirror server contracts at runtime boundary |
| Mock service | MSW | Fixture-driven offline dev mode |
| Charts | ECharts | Stacked area, multi-line, locale-aware |
| Styling | Tailwind v4 | Utility-first, no runtime CSS engine |
| Component library | shadcn (deferred until needed) | Copy-paste primitives; no v1 dependency |

### Infrastructure

| Concern | Choice |
|---|---|
| Cloud | AWS, region `eu-west-3` (Paris) |
| IaC | AWS CDK in Python |
| Compute | Lambda (Python 3.12) |
| Database | RDS Postgres 16.x, `db.t3.micro`, single-AZ |
| Object storage | S3 (raw feeds + frontend bundle) |
| API gateway | API Gateway HTTP API v2 (cheaper than REST) |
| CDN | CloudFront with Origin Access Control |
| Scheduler | EventBridge cron |
| Secrets | Secrets Manager (CDK-injected at deploy time, not runtime-fetched) |
| Observability | CloudWatch logs + alarms, SNS topic, AWS Budgets |
| CI/CD | GitHub Actions with OIDC trust to AWS |

## 3. Project layout

```
project-root/
├── app/
│   ├── api/
│   │   ├── deps.py            # Async session dependency
│   │   ├── main.py            # FastAPI app, router includes, CORS
│   │   ├── mangum_handler.py  # AWS Lambda entry (FastAPI via Mangum)
│   │   └── routers/
│   │       ├── articles.py
│   │       ├── candidates.py
│   │       ├── meta.py        # /healthz, /version, /stats
│   │       ├── sources.py
│   │       └── timeseries.py
│   ├── config.py              # pydantic-settings (env-var driven)
│   ├── db/
│   │   ├── engine.py          # async + sync sessionmakers
│   │   ├── migrations/        # alembic env + versions
│   │   └── models.py          # SQLAlchemy ORM
│   ├── extract/
│   │   ├── base.py            # MentionDraft, AliasSpec dataclasses
│   │   ├── keyword.py         # KeywordExtractor with requires_context
│   │   └── run.py             # CLI / batch wrapper
│   ├── ingest/
│   │   ├── dedup.py           # content_hash canonicalisation
│   │   ├── fetcher.py         # async httpx fetcher, per-source
│   │   ├── lambda_handler.py  # AWS Lambda entry (ingest stage)
│   │   ├── loader_handler.py  # AWS Lambda entry (loader stage)
│   │   ├── parser.py          # feedparser wrapper, edge cases
│   │   └── run.py             # local end-to-end orchestrator
│   ├── models/                # Pydantic response models
│   │   ├── article.py
│   │   ├── candidate.py
│   │   ├── source.py
│   │   └── timeseries.py
│   └── sources/
│       └── seed.py            # YAML → Postgres upsert
├── frontend/
│   ├── src/
│   │   ├── App.tsx
│   │   ├── api/               # typed API clients (one file per resource)
│   │   ├── components/        # ChartX, FilterX, TableX
│   │   ├── lib/               # colors, lean, url-state
│   │   ├── routes/            # one per page
│   │   └── main.tsx
│   ├── package.json
│   └── tsconfig.json
├── infra/
│   ├── cdk/
│   │   ├── app.py             # entrypoint; instantiates all 7 stacks
│   │   ├── cdk.json
│   │   └── stacks/
│   │       ├── network_stack.py
│   │       ├── runtime_stack.py
│   │       ├── data_stack.py
│   │       ├── ingest_stack.py
│   │       ├── api_stack.py
│   │       ├── frontend_stack.py
│   │       └── observability_stack.py
│   └── lambda_layer/
│       └── requirements-lambda.txt
├── seeds/
│   ├── candidates.yaml
│   └── sources.yaml
├── scripts/
│   ├── cron_ingest.sh         # local-dev wrapper
│   └── media27.cron
├── tests/
│   ├── integration/           # FastAPI + real Postgres via testcontainers
│   └── unit/                  # extractor, parser, dedup
├── .github/workflows/
│   ├── ci.yml
│   └── deploy.yml
├── alembic.ini
├── docker-compose.yml         # local Postgres
├── Makefile                   # standard developer targets
└── pyproject.toml
```

## 4. Database schema

Postgres 16, all timestamps `TIMESTAMPTZ` stored in UTC.

### `sources`

```
id              SMALLINT  PK, hand-assigned
slug            TEXT      UNIQUE NOT NULL
outlet          TEXT      NOT NULL                       -- "Le Monde"
section         TEXT                                       -- "politique"
feed_url        TEXT      NOT NULL
lean            TEXT                                       -- left | centre | right | etc.
is_active       BOOLEAN   NOT NULL DEFAULT TRUE
created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
```

### `articles`

```
id              BIGINT    PK, autoincrement
source_id       SMALLINT  FK sources(id) NOT NULL
guid            TEXT      NOT NULL                        -- from RSS <guid> or <link>
url             TEXT      NOT NULL                        -- canonicalised (no utm_*, fbclid, fragments)
title           TEXT      NOT NULL
summary         TEXT
published_at    TIMESTAMPTZ NOT NULL                      -- UTC; fall back to lastBuildDate then now()
fetched_at      TIMESTAMPTZ NOT NULL DEFAULT now()
lang            TEXT      NOT NULL DEFAULT 'fr'
raw             JSONB                                       -- original RSS item attributes
content_hash    BYTEA     NOT NULL                        -- sha256(lower(strip(title)) || lower(strip(summary)))

UNIQUE (source_id, guid)                                    -- in-source dedup
INDEX (published_at)
INDEX (source_id, published_at)
INDEX (content_hash)                                        -- non-unique: cross-source republication is a feature, not a bug
```

`content_hash` is intentionally **not** unique. Two outlets publishing
the same wire story is editorially significant; storing both rows lets
a query later count cross-source republication frequency.

### `candidates`

```
id              SMALLINT  PK, hand-assigned
slug            TEXT      UNIQUE NOT NULL
display_name    TEXT      NOT NULL                        -- "Jean-Luc Mélenchon"
party           TEXT
lean            TEXT
declared_at     DATE
eligible        BOOLEAN   NOT NULL DEFAULT TRUE           -- false → shown in separate UI panel
notes           TEXT
```

### `candidate_aliases`

```
id                  INTEGER   PK, autoincrement
candidate_id        SMALLINT  FK candidates(id) ON DELETE CASCADE
alias               TEXT      NOT NULL
match_kind          TEXT      NOT NULL DEFAULT 'wholeword'  -- exact | wholeword | regex
requires_context    TEXT                                     -- token that must appear in same field
is_active           BOOLEAN   NOT NULL DEFAULT TRUE

UNIQUE (candidate_id, alias)
```

`requires_context` is the key disambiguation primitive. Alias
`"Le Pen"` with `requires_context: "Marine"` only fires when "Marine"
co-occurs in the same field. Without it, Marion-Maréchal references
would inflate Marine's count. The whole disambiguation policy lives in
YAML, not code.

### `mentions`

```
id                  BIGINT    PK, autoincrement
article_id          BIGINT    FK articles(id) ON DELETE CASCADE
candidate_id        SMALLINT  FK candidates(id)
field               TEXT      NOT NULL                     -- 'title' | 'summary'
match_text          TEXT      NOT NULL
start_offset        INTEGER   NOT NULL
end_offset          INTEGER   NOT NULL
extractor           TEXT      NOT NULL                     -- 'keyword' | 'ner' | ...
extractor_version   TEXT      NOT NULL                     -- 'v1', 'v2', ...
confidence          NUMERIC                                  -- 1.0 for keyword; model score for NER
attributes          JSONB     NOT NULL DEFAULT '{}'         -- {alias, match_kind} for keyword; {entity_type, ...} for NER
created_at          TIMESTAMPTZ NOT NULL DEFAULT now()

UNIQUE (article_id, candidate_id, field, start_offset, extractor, extractor_version)
INDEX (candidate_id)
INDEX (article_id)
INDEX (attributes) USING gin
```

Two extractors can co-exist on the same article — the uniqueness key
includes `(extractor, extractor_version)`. Switching extractors is a
matter of inserting new rows; you can A/B compare or evolve forward.

### `ingest_runs`

```
id                          BIGINT    PK, autoincrement
started_at                  TIMESTAMPTZ NOT NULL DEFAULT now()
finished_at                 TIMESTAMPTZ
source_id                   SMALLINT  FK sources(id)
status                      TEXT      NOT NULL              -- running | ok | partial | failed
feed_http_status            SMALLINT
n_items_seen                INTEGER
n_articles_inserted         INTEGER
n_articles_skipped_dup      INTEGER
n_mentions_inserted         INTEGER
error                       TEXT
meta                        JSONB     NOT NULL DEFAULT '{}'

INDEX (started_at)
```

Audit log. Written in a **separate transaction** from `_persist_articles`
so a row-level failure during persistence (e.g., duplicate-key) cannot
poison the audit-log write. Critical for debugging production.

## 5. Configuration as data

### `seeds/candidates.yaml`

```yaml
candidates:
  - id: 1
    slug: melenchon
    display_name: Jean-Luc Mélenchon
    party: LFI
    lean: hard-left
    declared_at: 2026-05-03
    eligible: true
    notes: "Declared 2026-05-03 (France 24)."
    aliases:
      - { alias: "Jean-Luc Mélenchon", match_kind: exact }
      - { alias: "Mélenchon",          match_kind: wholeword }
      - { alias: "Melenchon",          match_kind: wholeword }   # accent-stripped fallback
      - { alias: "JLM",                match_kind: wholeword }

  - id: 3
    slug: le-pen-marine
    display_name: Marine Le Pen
    party: RN
    lean: hard-right
    eligible: false
    notes: |
      Cour de cassation upheld 5-year ineligibility on 2025-10-15.
      Listed for tracking but flagged ineligible in the dashboard.
    aliases:
      - { alias: "Marine Le Pen", match_kind: exact }
      - { alias: "Le Pen",        match_kind: wholeword, requires_context: "Marine" }
```

`python -m app.sources.seed` is idempotent — UPSERTs candidates,
DELETEs+re-inserts aliases per candidate (so removed aliases disappear).

### `seeds/sources.yaml`

```yaml
sources:
  - id: 1
    slug: lemonde-politique
    outlet: Le Monde
    section: politique
    feed_url: https://www.lemonde.fr/politique/rss_full.xml
    lean: centre-left

  # 14-15 outlets total spanning the political spectrum
```

`is_active: false` on individual sources disables fetching without
deleting the row (so `ingest_runs` history stays interpretable).

## 6. Ingest pipeline

Two execution modes, one persistence path.

### 6a. Local pipeline (`app/ingest/run.py`)

Synchronous end-to-end for development. Reads `seeds/sources.yaml`,
fetches each feed with `httpx.AsyncClient`, parses with `feedparser`,
canonicalises URLs, computes `content_hash`, and inserts to Postgres
via SQLAlchemy. Writes one `ingest_runs` row per source.

CLI invoked via `make ingest-once` or `python -m app.ingest.run --once`.

### 6b. Production pipeline — the no-NAT split

The same persistence logic, but split into two Lambdas:

```
EventBridge cron (0 6 12 18 UTC, daily)
        │
        ▼
[Ingest Lambda] (NO VPC)         — has free internet egress
        │  fetches all feeds, writes raw XML to S3
        ▼
   S3 raw bucket  (feeds/YYYY-MM-DD/{slug}.xml)
        │  S3 ObjectCreated event per uploaded file
        ▼
[Loader Lambda] (IN VPC)         — only path is S3 (gateway endpoint) + RDS
        │  parses XML, dedups, INSERT articles, INSERT ingest_runs
        │  then runs the keyword extractor inline over pending articles
        ▼
        Postgres (RDS)
```

This split is **the** load-bearing infrastructure decision. Don't
collapse it. The ingest Lambda has no VPC config, so it lives on
AWS-managed Lambda networking with free public-internet egress. The
loader runs in `PRIVATE_ISOLATED` subnets and reaches RDS without
ever needing to leave the VPC.

The S3 bucket is owned by the **Ingest stack**, not the Data stack —
otherwise the S3-notification → Loader-Lambda wiring would create a
cross-stack dependency cycle.

### 6c. Loader Lambda admin actions

The same loader Lambda accepts non-S3 payloads for one-off ops:

| Payload | Action |
|---|---|
| `{"action": "migrate"}` | `alembic upgrade head` against RDS |
| `{"action": "seed"}` | `app.sources.seed.main()` from bundled YAML |
| `{"action": "extract", "reprocess_all": false}` | run keyword extractor over pending articles |
| `{"Records": [...]}` (S3 event) | normal article persistence + inline extract |

This avoids needing a separate "admin Lambda" — same IAM role, same VPC
config, same log group. The handler dispatches on event shape.

### 6d. Article persistence rules

- `guid` is taken from the RSS `<guid>` (most reliable) or falls back
  to the canonical `<link>` if guid is missing.
- `url` is canonicalised before storage: strip `utm_*` params, `fbclid`,
  URL fragments. (Avoids tracking-noise duplicates between social-share
  variants.)
- `published_at` parsing: prefer item `<pubDate>` via `dateutil.parser`;
  fall back to channel `<lastBuildDate>` (matters for Le Parisien which
  has zero per-item dates); reject items >7 days in the future or
  before 1990.
- Items missing both a date and `lastBuildDate` are rejected with an
  error logged to `ingest_runs.meta`.
- Day-bucket analytics use
  `date_trunc('day', published_at AT TIME ZONE 'Europe/Paris')` so day
  boundaries match the editorial day, not UTC midnight.

### 6e. RSS edge cases observed in practice

| Outlet | Behaviour | Workaround |
|---|---|---|
| Le Parisien | 100 items per fetch, zero per-item dates | Channel `lastBuildDate` fallback |
| Marianne | UA-pattern discrimination | Identifying-but-Mozilla-prefixed UA |
| Les Échos | Akamai blocks server IPs entirely | `is_active: false` |
| La Croix | Removed public RSS | `is_active: false` |
| AFP | Closed public RSS years ago | Not seeded |

The User-Agent that works for most: `Mozilla/5.0 (compatible;
mediaElection27/0.1; +https://github.com/<owner>/<repo>)`.

## 7. The keyword extractor

`app/extract/keyword.py`. Stateless given the alias set.

### Input

`AliasSpec(candidate_id, alias, match_kind, requires_context, is_active)`,
loaded from `candidate_aliases` table.

### Algorithm

1. Group aliases by candidate.
2. For each candidate, sort aliases longest-first so the full name
   claims its span before the surname does.
3. For each alias, compile its regex once:
   - `exact` → escaped literal
   - `wholeword` → `\b<escaped>\b`
   - `regex` → use as-is
   - All flags: `IGNORECASE | UNICODE`
4. For each alias, run `finditer` over title and summary independently.
5. **Span de-duplication per candidate**: skip a match if its span is
   fully contained in any span already accepted for this candidate.
6. **Context disambiguation**: if the alias has `requires_context`,
   skip the match unless the context token appears (anywhere) in the
   same field as a whole word.
7. Emit `MentionDraft(article_id, candidate_id, field, match_text,
   start_offset, end_offset, extractor='keyword', version='v1',
   confidence=1.0, attributes={alias, match_kind})`.

### Why "longest first per candidate"

Without it, "Jean-Luc Mélenchon" would emit two mentions for the
same candidate at overlapping positions (one for the full name, one
for the surname). The de-dup is per-candidate, so two *different*
candidates can still match overlapping text (rare but valid).

### Why "same field" for `requires_context`

If "Le Pen" appears in the title and "Marine" only in the summary,
that's not strong enough to claim a Marine-Le-Pen mention. Conservative
by design — false positives are worse than false negatives in a
political tracker.

## 8. Backend API surface

FastAPI app at `app/api/main.py` includes all routers. Async only.

### Endpoints

| Method | Path | Purpose |
|---|---|---|
| GET | `/healthz` | liveness; returns `{"status": "ok"}` |
| GET | `/version` | build info |
| GET | `/stats` | counts: articles, mentions, sources, candidates, ingest_runs |
| GET | `/sources` | list of all sources (active flag included) |
| GET | `/candidates` | list of all candidates with alias count |
| GET | `/articles` | paginated articles with optional candidate/source/window filters |
| GET | `/timeseries` | the chart-feeding endpoint (see below) |

### `/timeseries` — the analytical heart

Query params:

| Param | Type | Default | Notes |
|---|---|---|---|
| `candidate_id` | repeatable int | none → all | `?candidate_id=1&candidate_id=2` |
| `source_id` | repeatable int | none → all | same shape |
| `from` | ISO date | today - 30 days | inclusive |
| `to` | ISO date | tomorrow | exclusive |
| `tz` | IANA tz | `Europe/Paris` | day-boundary timezone |
| `extractor` | str | `keyword` | future-proofing for NER comparison |
| `extractor_version` | str | `v1` | same |

Returns:

```json
{
  "from": "2026-04-17",
  "to": "2026-05-18",
  "tz": "Europe/Paris",
  "extractor": "keyword",
  "extractor_version": "v1",
  "n_total_mentions": 318,
  "points": [
    {"day": "2026-04-17", "candidate_id": 1, "source_id": 5, "n_mentions": 1, "n_articles": 1},
    ...
  ],
  "candidates": [ /* full registry, always present */ ],
  "sources": [ /* active sources, always present */ ]
}
```

The full candidate + source registries are **always** included even
if the window is empty. This is the "stable lookup table" pattern: the
frontend can render labels/colours from the registry without depending
on whether the requested window happened to be non-empty.

Implementation: one parameterised SQL query (use `text()` + `bindparam`
for arrays) that groups by `(day, candidate_id, source_id)`. Don't try
to express it via the ORM — the timezone-aware date_trunc is much
clearer in raw SQL.

### Tri-state filter semantics

URL filters have three states:

| URL shape | Meaning |
|---|---|
| `?candidates` missing | show all |
| `?candidates=` (empty list) | show none |
| `?candidates=2,14` | show those two |

The frontend's filter state and the API's filter handling must agree
on this. The simpler "always a list, empty means show-all" model
breaks the "deselect everything" UX.

## 9. Frontend

Single-page application with the following routes, all sharing one
top-level `Layout` (header + footer):

| Route | Page | Anchored component |
|---|---|---|
| `/` | Dashboard | `TimeSeriesChart` (multi-line, candidates × days) + filter panels |
| `/articles` | Articles | `ArticlesTable` with candidate + window filters |
| `/leaderboard` | Leaderboard | `LeaderboardTable` (candidate, n_mentions, sparkline) |
| `/share-of-voice` | Share of Voice | `ShareOfVoiceChart` (stacked area or 100% stacked) |
| `/sources/:slug` | Source Drilldown | per-outlet view of candidate coverage |

### URL state

A `lib/url-state.ts` helper synchronises filter state with URL
search-params. Every filter change updates the URL (`history.replace`,
not `push`, so back/forward don't pollute) and every component reads
the source of truth from the URL on mount. Result: every URL is a
shareable snapshot.

### Data flow

```
React component
     │
     ▼
useQuery(['timeseries', filters], () => fetchTimeseries(filters))
     │
     ▼
api/timeseries.ts        — typed wrapper using fetch + Zod validation
     │
     ▼
HTTP API (FastAPI)
```

Each `src/api/*.ts` file owns one resource: typed input, typed output,
Zod schema for runtime validation at the boundary. Frontend never trusts
the server's shape — Zod catches drift.

### MSW fixtures

Mock service worker handlers return fixed JSON for offline development.
`npm run dev` works without any backend running. Updated when API
contracts change.

### Build-time config

The API base URL is baked at build time via `VITE_API_BASE_URL`. The
GitHub Actions deploy workflow reads the API Gateway URL from
CloudFormation outputs, exports it as that env var, then runs
`npm run build`. The frontend bundle is uploaded to S3 and served via
CloudFront.

## 10. AWS infrastructure — 7 CDK stacks

All in region `eu-west-3` (Paris). The `Environment` is shared via
`os.environ["CDK_DEFAULT_ACCOUNT"]`.

### `Media27Network`

The VPC and security groups. **No NAT, no IGW**, only
`PRIVATE_ISOLATED` subnets across 2 AZs (RDS DB Subnet Groups require
≥2 AZs even for single-AZ instances). One S3 gateway endpoint (free).

Two security groups:

- `RdsSg` — accepts 5432 only from `LambdaVpcSg`. `allow_all_outbound=False`.
- `LambdaVpcSg` — egress open (for AWS APIs). Used by the loader and API Lambdas.

**Critical**: SecurityGroup `description` is ASCII-only. AWS EC2 rejects
em-dashes, smart quotes, and accented characters with the cryptic message
"Character sets beyond ASCII are not supported", forcing a full stack
rollback.

### `Media27Runtime`

A single Lambda Layer containing all Python runtime dependencies
(fastapi, mangum, sqlalchemy[asyncio], alembic, asyncpg, psycopg,
pydantic, httpx, feedparser, pyyaml, structlog). Built via Docker
bundling against the official Python 3.12 Lambda runtime image so
the binary wheels match the target runtime.

### `Media27Data`

The RDS instance:

- engine: Postgres 16, latest patch
- instance class: `db.t3.micro`
- storage: gp3, 20 GB, encrypted
- multi-AZ: false (Free Tier)
- subnet group: the network stack's PRIVATE_ISOLATED subnets
- backup retention: 1 day
- deletion protection: false (this is portfolio)
- `removal_policy=SNAPSHOT` so accidental destruction takes a final snapshot first
- credentials: generated Secret in Secrets Manager
- security group: `RdsSg` from network stack

### `Media27Ingest`

The pipeline core:

- S3 raw bucket (block all public access, S3-managed encryption,
  lifecycle: Glacier after 30 days, expire after 365 days,
  `auto_delete_objects=True` for clean stack destruction)
- Ingest Lambda — **outside the VPC**, 5-min timeout, has internet
- Loader Lambda — **inside VPC** (PRIVATE_ISOLATED), 5-min timeout
- EventBridge cron rule fires the ingest Lambda 4×/day at 0/6/12/18 UTC
- S3 ObjectCreated event source on the raw bucket triggers the loader

The Loader Lambda's environment includes:

| Var | Value |
|---|---|
| `DB_HOST` | RDS endpoint address |
| `DB_PORT` | RDS endpoint port |
| `DB_NAME` | `media27` |
| `DB_USERNAME` | `db_secret.secret_value_from_json("username").unsafe_unwrap()` |
| `DB_PASSWORD` | `db_secret.secret_value_from_json("password").unsafe_unwrap()` |

This injects the secret value as a CloudFormation `{{resolve:secretsmanager:...}}`
dynamic reference. At deploy time, CFN resolves it and bakes the
plaintext into the Lambda's env vars. This **avoids a runtime Secrets
Manager call**, which would hang from PRIVATE_ISOLATED subnets without
a ~$15/month VPC interface endpoint.

### `Media27Api`

- API Lambda — inside VPC, runs FastAPI via Mangum, 30s timeout
- API Gateway HTTP API v2, one catch-all route `ANY /{proxy+}` plus
  `ANY /` so the Lambda receives everything
- CORS: `allow_origins=["*"]` for v1 (tighten to CloudFront origin
  before public launch)
- CloudFormation Output `ApiUrl` consumed by the frontend stack

### `Media27Frontend`

- S3 SPA bucket — private, OAC-restricted
- CloudFront distribution with the SPA bucket as origin via Origin
  Access Control (OAI is deprecated; don't use it)
- `PRICE_CLASS_100` (Europe + US edge locations only)
- Error responses 403 and 404 → `/index.html` with HTTP 200 — this is
  how SPAs handle deep links (React Router takes over client-side)
- `BucketDeployment` from `frontend/dist/` (conditional on the
  directory existing — local `cdk synth` works without a build)

### `Media27Observability`

- SNS topic with email subscriber (optional, gated on
  `cdk.json` context key `alert_email`)
- CloudWatch Alarms: one per Lambda for errors > 0 in a 5-minute window
- RDS CPU alarm (>80% for 5 minutes)
- AWS Budgets: $5/month with notifications at 80% and 100%

## 11. CI/CD

Two GitHub Actions workflows.

### `.github/workflows/ci.yml`

Runs on every push and PR:

- `ruff check` over `app/` and `tests/`
- `mypy --strict app/`
- `pytest tests/unit/` (testcontainers spins a real Postgres; ~30 sec)

### `.github/workflows/deploy.yml`

Runs on push to `main` (paths-ignore: `**.md`, `LICENSE`, `.gitignore`):

```yaml
permissions:
  id-token: write    # OIDC
  contents: read

env:
  AWS_REGION: eu-west-3
  NODE_VERSION: "22"
  PYTHON_VERSION: "3.12"

steps:
  - actions/checkout@v4
  - aws-actions/configure-aws-credentials@v4 (OIDC via AWS_DEPLOY_ROLE_ARN secret)
  - setup-python (3.12, pip cache)
  - install python deps incl. `[infra]` extras (cdk libs)
  - setup-node (22, npm cache)
  - install AWS CDK CLI
  - cdk synth --quiet                          # sanity
  - cdk deploy Media27Network Media27Runtime \
               Media27Data Media27Ingest \
               Media27Api Media27Observability \
               --require-approval never --concurrency 3
  - read API URL from CloudFormation
  - cd frontend && npm ci
  - VITE_API_BASE_URL=... npm run build
  - cdk deploy Media27Frontend --require-approval never
```

### One-time AWS setup before the workflow can run

1. Create an OIDC identity provider for `token.actions.githubusercontent.com`
2. Create IAM role `<project>-github-deploy` with trust policy scoped
   to `repo:<owner>/<repo>:ref:refs/heads/main`, permissions
   AdministratorAccess (or a tighter CDK-specific policy)
3. Save the role ARN as repo secret `AWS_DEPLOY_ROLE_ARN`
4. `cdk bootstrap aws://<account-id>/eu-west-3` from a workstation
   once (CDK needs a per-region toolkit stack)

## 12. Testing strategy

- **Unit tests** (`tests/unit/`): pure functions only — extractor logic,
  parser edge cases, dedup hash semantics. No I/O. Fast (<1s total).
- **Integration tests** (`tests/integration/`): real Postgres via
  `testcontainers`. Spin a fresh container per test session, run
  alembic upgrade against it, drive the FastAPI app via `httpx.AsyncClient`
  over `ASGITransport`. ~30s. Covers API contracts end-to-end.

**Async test loop scope** (`pyproject.toml`):

```toml
[tool.pytest.ini_options]
asyncio_default_test_loop_scope = "session"
```

Without this, asyncpg's pool dies between tests because each test
spins up its own event loop, leading to `Event loop is closed` errors.

**Use `httpx.AsyncClient` over `ASGITransport`, not `TestClient`.**
FastAPI's TestClient is synchronous and produces the same loop-mismatch
issues for async pooled connections.

## 13. Suggested rebuild order

The plan that actually worked, with timing for context (one engineer,
evenings/weekends):

| Week | Focus | Stops at |
|---|---|---|
| 1 | Local schema + ingest | `python -m app.ingest.run --once` prints counts; `psql` shows rows |
| 2 | Keyword extractor + FastAPI | All four/five routers; ~30 unit tests; `/timeseries` returns chart-ready JSON |
| 3 | Frontend scaffold | Vite + React + ECharts; one chart, two filters, URL state; MSW fixtures |
| 4 | AWS CDK deploy | `cdk deploy` produces RDS + Lambdas + API Gateway + CloudFront; one manual ingest populates RDS |
| 5 | Public push + write-up | Live URL; README; portfolio post |
| 6-8 | Phase 2 eval | 300 hand-labelled articles; `run_eval.py`; F1 table; NER decision |
| ongoing | Phase 3 stretches | Sentiment, drilldowns, materialized views |

Build vertically. Don't write the frontend scaffold in Week 1; don't
write the AWS stack in Week 2. The whole project rests on the local
pipeline working end-to-end — when that's solid, everything downstream
is a transformation of working code into a new context.

## 14. Anticipated gotchas to avoid

Six learned in production. Forewarned, you can skip the pain.

1. **EC2 SecurityGroup descriptions must be ASCII-only.** No em-dashes,
   no smart quotes, no accented characters. Other AWS resource
   descriptions are more permissive — EC2 is uniquely strict.

2. **RDS in PRIVATE_ISOLATED subnets cannot be made publicly accessible.**
   `publicly_accessible=True` is silently a no-op if no public subnet
   exists. The only way in is via something inside the VPC.

3. **Lambdas in PRIVATE_ISOLATED subnets cannot reach Secrets Manager
   at runtime.** Don't write a cold-start `boto3.client("secretsmanager")`
   call from a VPC Lambda unless you've also added the VPC interface
   endpoint (~$15/month). Instead: inject the secret value at deploy
   time via CDK SecretValue token resolution into env vars.

4. **CloudFormation refuses to update a cross-stack export while in use.**
   Replacing a shared LayerVersion (or any other exported resource) via
   `cdk deploy` will fail rollback if other stacks import the old value.
   The recovery is `delete-stack` on the consumer stacks first, then
   redeploy. Long-term cleaner: publish ARNs via SSM Parameter Store
   rather than CFN exports.

5. **VPC-attached Lambda deletion takes 15-30 minutes.** ENIs are slow
   to release. `delete-stack` looks stuck; it isn't. Plan accordingly.

6. **`ROLLBACK_COMPLETE` ≠ `UPDATE_ROLLBACK_COMPLETE`.** The first is
   terminal — you must `delete-stack` before retrying. The second is
   recoverable — `cdk deploy` will retry against it. The two-letter
   prefix is the only visible difference.

Additional gotchas worth a sticky note:

- `pydantic-settings` `.env` file silently overrides defaults. When a
  config change "doesn't take effect", inspect `settings.X` at runtime,
  not the source default.
- `docker compose down` keeps volumes; `-v` is destructive. No recovery.
- Cron `@reboot` fires before Docker is necessarily ready. Wrap with
  `wait_for_postgres()` if running locally on a cron.
- The AWS CLI `lambda invoke` default `--cli-read-timeout` is 60s.
  First-invoke admin calls (migrate, seed) need `--cli-read-timeout 300`.
- Smart quotes / NBSPs in titles produce different `content_hash`. If
  cross-source dedup matters, normalise Unicode before hashing.
- Lambda env vars are visible to anyone with `lambda:GetFunction`. Fine
  for portfolio; revisit for stricter threat models.

## 15. Out of scope (deliberately)

Things to **not** build in v1, even though they'd be tempting:

- **User accounts / auth.** Read-only public dataset. No auth surface.
- **Multi-language UI.** French only.
- **Live rotation of DB credentials.** Inject at deploy time; rotate by redeploying.
- **Real-time updates.** 4-hourly cron is enough. No websockets.
- **Article body storage.** *Droit voisin* posture.
- **Comments / social features.** Not what this is.
- **Generic admin dashboard.** Lambda admin invokes are the admin UI.
- **NAT Gateway anywhere in the architecture.** Free Tier depends on this.
- **Multi-AZ RDS, read replicas, automatic backups beyond 1 day.** Free Tier.
- **Custom domain (yet).** CloudFront default URL is fine for v1.
- **Phase 3 sentiment / NER.** Only after Phase 1 keyword + Phase 2 eval prove it's needed.

## 16. Closing notes

The project's hardest decisions are not the obvious technical ones
(framework choice, library picks). They're the **scoping** decisions —
what NOT to build, what NOT to store, what NOT to optimise. Most of
the "load-bearing decisions" in §1 are negative space: no NAT, no auth,
no article bodies, no Aurora, no NER until F1 demands it.

The reward for that discipline is a coherent project you can describe
in two sentences and demo in 30 seconds. Anything that makes you
unable to do those two things is probably worth cutting.

Good luck with the rebuild. The plan held up in production — almost
every load-bearing decision either validated or proved still-the-right-
call. The places where it bent (frontend router, deferred shadcn) were
in the framework-choice layer, where the cost of swapping is low.
