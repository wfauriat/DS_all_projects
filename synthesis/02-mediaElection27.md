# mediaElection27 — technical synthesis

> A production-deployed, cost-constrained analytics product: daily RSS ingest of ~15 French national
> outlets → Postgres → candidate-mention extraction → public React/ECharts dashboard on AWS.
> The controlling constraint is **gross spend < $5/month on Free Tier**, and it visibly shapes the
> architecture at every layer.

---

## 1. Identity

| Item | Value |
|---|---|
| Domain | media monitoring / share-of-voice for the 2027 French presidential election |
| Shape | classic 3-tier web product (ingest → RDBMS → API → SPA) deployed serverless on AWS |
| Backend | Python 3.12, FastAPI, SQLAlchemy 2.0 async + sync, asyncpg/psycopg, alembic, pydantic-settings, feedparser, httpx, structlog, boto3, Mangum |
| Frontend | Vite + React 19 + TypeScript, TanStack Query, ECharts, Tailwind v4, Zod, react-router 7 |
| Infra | AWS CDK (Python), 7 CloudFormation stacks, `eu-west-3`; GitHub Actions + OIDC |
| Tooling | pip + venv (`make install`), ruff, **mypy --strict**, pytest + pytest-asyncio + testcontainers |
| Size | ~113 files; Python ≈ 2.2k lines, TS/TSX ≈ 1.8k lines, CDK ≈ 700 lines, docs ≈ 2.5k lines |
| Docs | `dev/PLAN-v1.md`, `dev/MENTAL-MODELS.md` (generic IaC + DE mental models), two dated CHECKPOINTs, `dev/SPECIFICATION-REBUILD.md` (a self-contained re-implementation spec), `dev/cheatsheet_sql.md`, `dev/schema_db.md` |
| Status at time of writing | live: 605 articles / 318 mentions / 14 candidates / 15 sources, pipeline autonomous 4×/day |

---

## 2. Topology

```
EventBridge cron (0/6/12/18 UTC)
        ▼
ingest Lambda   ── NO VPC ──  free public egress → fetch ~15 RSS feeds (async, bounded)
        │                     reads seeds/sources.yaml bundled in the zip (no DB access)
        ▼  put_object feeds/YYYY-MM-DD/{slug}.xml
   S3 raw bucket (Glacier-IR @30d, expire @365d, SSE-S3, BPA, enforce_ssl)
        ▼  S3 ObjectCreated event, one record per file
loader Lambda   ── IN VPC (PRIVATE_ISOLATED) ──
        │  parse XML → canonical_url + content_hash → INSERT ... ON CONFLICT DO NOTHING
        │  write ingest_runs audit row → then run the keyword extractor inline
        ▼
   RDS Postgres 16 (db.t3.micro, single-AZ, private, SG-restricted)
        ▲
API Lambda (Mangum → FastAPI) ── IN VPC ──
        ▲
API Gateway HTTP API ($default, catch-all /{proxy+})
        ▲
CloudFront (OAC) ◀── private S3 bucket holding the built SPA
```

**The load-bearing decision — the "no-NAT split".** The VPC has *only*
`PRIVATE_ISOLATED` subnets and `nat_gateways=0`. A NAT gateway would cost ≈ $32/month, more than the
rest of the stack combined. Therefore:
- anything needing the public internet (RSS fetch) runs **outside** the VPC, where Lambda gets free egress;
- anything needing RDS runs **inside** the VPC and reaches S3 through a free **gateway endpoint**;
- Secrets Manager cannot be reached at runtime from isolated subnets, so credentials are injected as
  plaintext env vars resolved by CloudFormation at deploy time (`secret_value_from_json(...).unsafe_unwrap()`),
  an explicitly recorded security trade-off.

---

## 3. Entry points

| Entry point | Invoked by |
|---|---|
| `app/ingest/lambda_handler.handler` | EventBridge cron (prod) |
| `app/ingest/loader_handler.handler` | S3 ObjectCreated **and** manual admin payloads `{"action": "migrate"\|"seed"\|"extract"}` |
| `app/api/mangum_handler.handler` | API Gateway (prod); `Mangum(app, lifespan="off")` |
| `app/api/main:app` | `uvicorn` (local dev, `make api`) |
| `python -m app.ingest.run --once [--source slug]` | local dev / host cron (`scripts/cron_ingest.sh`) |
| `python -m app.extract.run [--all]` | local dev / loader admin action |
| `python -m app.sources.seed` | local dev / loader admin action |
| `alembic upgrade head` | local dev / loader admin action |
| `infra/cdk/app.py` | `cdk synth` / `cdk deploy` (discovered via `cdk.json`) |
| `frontend` npm scripts | `vite`, `tsc --noEmit && vite build` |

---

## 4. Data model (Postgres, SQLAlchemy 2.0 declarative)

`app/db/models.py` — `DeclarativeBase` with `type_annotation_map = {dict[str, Any]: JSONB}`, so
`Mapped[dict[str, Any]]` maps to JSONB automatically.

| Table | Key columns | Constraints / indexes |
|---|---|---|
| `sources` | `id SMALLINT` (hand-assigned, `autoincrement=False`), `slug` unique, `outlet`, `section`, `feed_url`, `lean`, `is_active` | seeded from YAML |
| `articles` | `id BIGINT`, `source_id`, `guid`, `url`, `title`, `summary`, `published_at TZ`, `fetched_at`, `lang`, `raw JSONB`, `content_hash BYTEA` | `UNIQUE(source_id, guid)`, indexes on `published_at`, `(source_id, published_at)`, `content_hash` |
| `candidates` | `id SMALLINT` hand-assigned, `slug`, `display_name`, `party`, `lean`, `declared_at`, `eligible`, `notes` | |
| `candidate_aliases` | `candidate_id`, `alias`, `match_kind` (`exact\|wholeword\|regex`), `requires_context`, `is_active` | `UNIQUE(candidate_id, alias)`, `ON DELETE CASCADE` |
| `mentions` | `article_id`, `candidate_id`, `field` (`title\|summary`), `match_text`, `start_offset`, `end_offset`, `extractor`, `extractor_version`, `confidence`, `attributes JSONB` | `UNIQUE(article_id, candidate_id, field, start_offset, extractor, extractor_version)` + **GIN index on `attributes`** |
| `ingest_runs` | `started_at/finished_at`, `source_id`, `status` (`running\|ok\|partial\|failed`), `feed_http_status`, `n_items_seen`, `n_articles_inserted`, `n_articles_skipped_dup`, `n_mentions_inserted`, `error`, `meta JSONB` | index on `started_at` |

**The central schema idea:** `mentions` is keyed by `(extractor, extractor_version)` as well as by
position. Several extractors (keyword v1 today, an NER model later) can therefore write into the same
table side by side without clobbering each other, and any query filters on the extractor version it
wants. This is *model-versioning inside the data model* rather than in a separate store.

**A recorded schema walk-back** (migration `98d3055b30f7`): the initial schema had
`UNIQUE(content_hash)` globally, which collapsed the same AFP wire republished by several outlets into
a single row. Since "each outlet's pick-up is a distinct data point", the constraint was dropped and
replaced with a plain index. Good example of *dedup scope being an analytical decision, not a technical one*.

Alembic: `env.py` wires `target_metadata = Base.metadata` and pulls the sync DSN from
`app.config.settings`, with `compare_type=True, compare_server_default=True`.

---

## 5. Ingest layer (`app/ingest/`)

| Module | Content |
|---|---|
| `fetcher.py` | `FetchSpec`/`FetchResult` dataclasses; `fetch_one` sets conditional-GET headers (`If-None-Match`, `If-Modified-Since`) and **never raises** — network errors become `status=0, error="..."` in the result; `fetch_all` = one `httpx.AsyncClient` + `asyncio.Semaphore(ingest_max_parallel)` + `asyncio.gather` |
| `parser.py` | `normalise_pubdate()` handles `struct_time` (feedparser, UTC by convention) and free-text dates (`dateutil`), assumes **Europe/Paris** for naive datetimes, converts to UTC, and applies a **sanity window** (reject < 1990 or > now+7 d). `parse_feed()` falls back to the channel-level `lastBuildDate` for feeds that omit per-item dates, and skips entries without URL or title |
| `dedup.py` | `canonical_url()` strips `utm_*` + a fixed set of tracking params and the fragment; `content_hash()` = sha256 of lowercased/stripped `title\nsummary` |
| `run.py` | the local CLI collapsing fetch + persist. `_persist_articles` uses `pg_insert(...).on_conflict_do_nothing(index_elements=[source_id, guid]).returning(Article.id)` and counts inserted vs skipped from whether a row came back. **Two independent transactions per feed**: one for the articles (can roll back), one that always records the `ingest_runs` audit row |
| `lambda_handler.py` | prod fetch stage; reads `seeds/sources.yaml` from the bundled zip (no DB), uploads each body to `feeds/{date}/{slug}.xml`, returns a per-feed report |
| `loader_handler.py` | prod persist stage; **reuses `_persist_articles` / `_record_run` from `run.py`** so persistence logic exists once. Notable mechanics below |

**`loader_handler` mechanics worth noting**
- A *cold-start hook* `_configure_db_url_from_env()` runs at import time, guarded by
  `"AWS_LAMBDA_FUNCTION_NAME" in os.environ`, and rewrites `settings.database_url_sync` from the
  injected env vars **before** any SQLAlchemy engine is constructed. All engine-touching imports are
  therefore placed after the call with `# noqa: E402` — a deliberate, documented import-order violation.
- The same function is the **in-VPC admin entrypoint**: `handler` dispatches on `event["action"]`
  (`migrate` runs `alembic.command.upgrade` programmatically against a `Config` built from the bundled
  `alembic.ini`; `seed`; `extract`). One IAM role, one VPC config, one log group for all one-off ops.
- After persisting S3 records it runs the extractor **inline** but wraps it in a try/except that never
  propagates: "persistence is the loader's primary job; matching can be backfilled."

---

## 6. Extraction layer (`app/extract/`)

A deliberately small, declarative NLP stage with a pluggable seam.

- `base.py` — `AliasSpec` and `MentionDraft` dataclasses (`slots=True`), decoupled from the ORM so the
  extractor is unit-testable with no DB; an `Extractor` **Protocol** (`extractor_id`, `version`,
  `extract(*, article_id, title, summary) -> list[MentionDraft]`) as the seam for a future NER model.
- `keyword.py` — `KeywordExtractor` (id `keyword`, version `v1`):
  - three match kinds compiled once at construction: `exact` (escaped substring), `wholeword`
    (`\b…\b`), `regex` (raw), all `IGNORECASE | UNICODE`;
  - **context disambiguation**: `requires_context` demands a whole-word co-occurrence *in the same field*
    (the canonical case: alias "Le Pen" requires "Marine", which keeps Marion Maréchal-Le Pen out of
    Marine's counts);
  - **span subsumption per candidate**: aliases sorted longest-first, and a match entirely inside an
    already-claimed span is dropped, so "Jean-Luc Mélenchon" emits one mention, not two;
  - accent-stripped variants are *data* (extra alias rows in YAML), not Unicode normalisation in code —
    "keeps the matcher simple and the data declarative".
- `run.py` — idempotent driver: loads active aliases, selects articles with a correlated
  `NOT EXISTS` on `(extractor, extractor_version)` (or all with `--all`), batches drafts (500) and
  UPSERTs with `on_conflict_do_nothing` on the positional unique key, counting real inserts via
  `.returning(...)`.

---

## 7. API layer (`app/api/`)

FastAPI, async SQLAlchemy, Pydantic response models with `from_attributes=True`.

- `deps.py` — `SessionDep = Annotated[AsyncSession, Depends(get_session)]`, the single injected dependency.
- `engine.py` — **dual engines**: async (asyncpg, `pool_pre_ping`, `pool_size=5`) for the API, sync
  (psycopg) for alembic/seed/ingest/extract scripts; both lazily created module-level singletons.
- Routers:
  | Router | Endpoint | Notable |
  |---|---|---|
  | `meta` | `/healthz`, `/version`, `/stats` | cheap aggregate counters |
  | `candidates` | `GET /candidates` | outer join + `count()` for alias counts, merged into the Pydantic model with `model_copy(update=...)` |
  | `sources` | `GET /sources` | `include_inactive` flag |
  | `timeseries` | `GET /timeseries` | the chart endpoint — see below |
  | `articles` | `GET /articles` | filter by candidate / `has_mention` / source / date window, `limit`/`offset` with a `select(func.count()).select_from(base.subquery())` total, then a second query to attach `candidate_ids` per article (avoids a row-multiplying join) |

**`/timeseries` is the interesting one.**
- Raw SQL via `text()` with `bindparam("candidate_ids", type_=ARRAY(Integer))`, using the
  `CARDINALITY(:ids) = 0 OR col = ANY(:ids)` idiom so one statement serves both "all" and "filtered".
- Day bucketing with `date_trunc('day', a.published_at AT TIME ZONE :tz)::date` — the "day" is the
  **editorial day in the requested IANA timezone** (default `Europe/Paris`), not a UTC day. This is the
  project's equivalent of DE_pipeline_stream's "three clocks" discipline.
- The response embeds the **full candidate and source registries** alongside the points, so the
  frontend can render labels and colours from one request even when the window is empty or filtered.
- `from_: date = Field(alias="from")` + `response_model_by_alias=True` to expose a Python-reserved word
  as a query/JSON key.

---

## 8. Frontend (`frontend/`)

- **Routing/state**: react-router 7 with five routes (Dashboard, Leaderboard, ShareOfVoice,
  SourceDrilldown, Articles). **All filter state lives in the URL** (`useSearchParams`), so refresh and
  link-sharing reproduce the view. `lib/url-state.ts` is the single parse/serialise module, including a
  documented **tri-state encoding**: param missing = "all", present-and-empty = "none", list = those ids.
- **Data fetching**: TanStack Query with `staleTime: 60_000`, `refetchOnWindowFocus: false`, `retry: 1`.
  Each `api/*.ts` module exports a Zod schema *and* the `useXxx()` hook; the response is
  `Schema.parse(data)` — **runtime validation at the API boundary**, with `z.infer` giving the TS type.
  `api/client.ts` is a ~30-line fetch wrapper with an `ApiError` class and array-aware query params.
- **Charts**: ECharts via `echarts-for-react`, `option` built inside `useMemo`. Computation is done in
  plain TS reducers before the chart config (`rollupByCandidate`, `enumerateDays`, `buildRows`), which
  keeps the chart component declarative and the aggregation testable in principle.
  Share-of-voice normalises each day to 100 % over a fully enumerated day axis (so gaps are zeros, not
  missing points); the leaderboard computes total / distinct outlets / latest day / per-day sparkline.
- **Domain conventions as small pure modules**: `lib/colors.ts` (stable palette slot by candidate id,
  so a candidate keeps its colour across reloads and filter changes) and `lib/lean.ts` (political-lean
  ordering used to sort candidates left-to-right, unknown values sorted last on purpose).
- **i18n**: one `as const` string table (`i18n.ts`) in fr-FR, including function-valued entries for
  interpolation — a pragmatic stand-in for a real i18n library.
- `VITE_API_BASE_URL` is **baked in at build time** by the deploy workflow after reading the API URL
  from CloudFormation outputs.

---

## 9. Infrastructure (`infra/cdk/`, 7 stacks)

| Stack | Contents | Notable choices |
|---|---|---|
| `Media27Network` | VPC `10.0.0.0/16`, `max_azs=2` (RDS subnet groups need ≥2 AZs even single-AZ), **`nat_gateways=0`**, only `PRIVATE_ISOLATED` subnets, free S3 gateway endpoint, two SGs (`rds_sg` ingress 5432 only from `lambda_vpc_sg`, `allow_all_outbound=False`) | the cost-driving stack |
| `Media27Runtime` | one shared Lambda `LayerVersion` built with Docker bundling against the official Python 3.12 Lambda image (`pip install -r requirements-lambda.txt -t /asset-output/python` + `.pyc`/`__pycache__` pruning) | keeps each function asset to just `app/` |
| `Media27Data` | Secrets Manager generated password + RDS Postgres 16.6 `db.t3.micro`, gp3 20 GB, private, `backup_retention=1d`, `RemovalPolicy.SNAPSHOT` | snapshot-on-destroy as a safety net |
| `Media27Ingest` | raw S3 bucket (BPA, SSE-S3, `enforce_ssl`, lifecycle Glacier-IR@30d/expire@365d), ingest Lambda (**no `vpc=`**), EventBridge cron, loader Lambda (in VPC), `S3EventSource` | bucket lives here, not in DataStack, to avoid a cross-stack cycle on the notification |
| `Media27Api` | API Lambda (Mangum) in VPC + HTTP API with a catch-all `/{proxy+}` ANY route + `/`, CORS preflight, `CfnOutput ApiUrl` | FastAPI does all dispatch; API Gateway is a dumb front door |
| `Media27Frontend` | private S3 bucket + CloudFront with **Origin Access Control**, `PRICE_CLASS_100`, SPA deep-link handling via 403/404 → `/index.html` 200, conditional `BucketDeployment` only when `frontend/dist` exists | synth always works without a built frontend |
| `Media27Observability` | SNS topic (+ optional email from `cdk -c alert_email=`), one CloudWatch alarm per Lambda on `metric_errors`, RDS CPU > 80 % for 15 min, `CfnBudget` at $5 with 80 %/100 % notifications | documents what it *deliberately defers* (billing alarms need `us-east-1`; cost anomaly detection is overkill at this scale) |

`app.py` composes them explicitly, passes constructs between stacks (which CDK compiles into
CloudFormation `Outputs` + `Fn::ImportValue`), adds explicit `add_dependency()` where no value
reference exists, and applies `Tags.of(app).add("project", "media27")` for Cost Explorer.

Both Lambda-bearing stacks share an identical `LAMBDA_EXCLUDES` list, with the asset rooted at the
project root so `seeds/` rides along with `app/`.

## 10. CI/CD

- `ci.yml` on every branch: `ruff check` → `mypy app` (strict) → `pytest tests/unit` only
  (unit tests must not need Postgres).
- `deploy.yml` on push to `main`, with `paths-ignore` for `**.md`/`LICENSE`/`.gitignore`:
  **OIDC federation** (`permissions: id-token: write`, `aws-actions/configure-aws-credentials` assuming
  `media27-github-deploy`) — no long-lived AWS keys. Then `cdk synth --quiet` (sanity) →
  `cdk deploy` of the six backend stacks `--concurrency 3` → read `ApiUrl` from CloudFormation via the
  AWS CLI into `$GITHUB_OUTPUT` → `npm ci && npm run build` with `VITE_API_BASE_URL` → deploy
  `Media27Frontend` (which invalidates CloudFront). The four one-time manual AWS setup steps are
  documented as a comment block in the workflow itself.

## 11. Tests

- `tests/unit/test_keyword.py` — 16 pure-Python tests that read as an executable specification of the
  matcher: whole-word vs substring, case-insensitivity, accent fallback as data, `requires_context`
  positive/negative/the Marion-Maréchal case, multiple matches, span subsumption, multi-candidate,
  title+summary, empty input, invalid `match_kind` raising at construction, regex kind.
- `tests/integration/` — `httpx.AsyncClient` + `ASGITransport` against the real app with a
  session-scoped event loop, and a module-level `pytestmark = pytest.mark.skipif(not _postgres_reachable())`
  so the suite stays green without Docker.
- pytest config: `asyncio_mode = "auto"`, session-scoped default loops (so the async engine's pool is
  reused).

## 12. Local workbench

- `Makefile` with the same self-documenting `help` idiom as the other projects (grep + awk over `## `),
  explicit `.venv/bin/*` tool paths rather than activation, and a `dev: db-up migrate seed` bootstrap target.
- `docker-compose.yml` is one service: Postgres 16-alpine with a healthcheck and a named volume.
  `make db-up` polls `pg_isready` in a shell loop before returning.
- `app/config.py`: a single `pydantic-settings` `Settings` instance with `.env` support, holding both
  DSNs, the seeds/raw dirs, the S3 bucket, log flags, and ingest tuning (timeout, parallelism, UA string).
- `scripts/cron_ingest.sh` + `scripts/media27.cron` — the pre-AWS host deployment, kept in the repo.
  The wrapper documents *why* a wrapper exists (cron's minimal environment), uses `set -uo pipefail`
  **without `-e`** so failures get logged rather than silently truncating the run, waits up to 60 s for
  Postgres on a `/dev/tcp` probe (for `@reboot` races with Docker), and prints scannable
  `===== <iso ts> ingest start/end (exit N) =====` markers.

---

## 13. Idioms worth extracting

1. **Config-as-data seeded into the DB.** `seeds/candidates.yaml` and `seeds/sources.yaml` are the
   editorial source of truth; `app/sources/seed.py` UPSERTs them (`on_conflict_do_update` with
   `stmt.excluded[...]`), and **aliases are delete-all + reinsert per candidate** so removing an alias
   from YAML removes it from the DB. Editing a feed URL or a disambiguation rule is a data change, not
   a code change. The same YAML is bundled into the ingest Lambda for the no-DB fetch stage.
2. **Idempotency by construction at every write.** `ON CONFLICT DO NOTHING` on the natural keys
   (`(source_id, guid)` for articles, the positional key for mentions), plus `.returning(id)` to
   distinguish "inserted" from "already there". Re-running any stage is always safe.
3. **Audit-row-per-run in the same database as the data** (`ingest_runs`), written in its own
   transaction so a persistence failure still produces an observability record.
4. **The extractor is versioned in the data.** `(extractor, extractor_version)` columns + a Protocol
   seam means adding an NER model is additive, and A/B comparison is a `GROUP BY`.
5. **One handler, many jobs, dispatched on event shape** — the loader Lambda as admin entrypoint. Saves
   an IAM role, a VPC config and a log group per operation.
6. **Cold-start env rewriting before engine construction**, with the import-order violation made
   explicit and justified in a docstring.
7. **Cost as a first-class architectural constraint**, with the consequences traced end-to-end: no NAT →
   isolated subnets → no Secrets Manager at runtime → deploy-time credential injection → an accepted,
   documented security trade-off.
8. **Runtime schema validation on both sides of the wire**: Pydantic response models in Python, Zod
   parsing in TypeScript. The two are hand-kept in sync (no codegen from OpenAPI — a noted simplification).
9. **URL as the canonical UI state store**, with an explicit tri-state encoding for "all / none / subset".
10. **Timezone-aware day bucketing** pushed into SQL with `AT TIME ZONE :tz`, parameterised per request.

## 14. Recorded operational lessons (from `dev/CHECKPOINT-2026-05-17.md`)

- EC2 SecurityGroup descriptions must be **ASCII-only** (an em-dash rolled back a whole stack).
- `publicly_accessible=True` on RDS is a no-op when the subnet group has no public subnet — "isolated"
  really means isolated, including from AWS's own public APIs.
- CloudFormation refuses to update a **cross-stack export while in use**; the producer rolls back and
  consumers stay pinned. Suggested long-term fix: publish ARNs through SSM Parameter Store instead of
  exports.
- Deleting a VPC-attached Lambda takes 15–30 min (ENI release) with no stack events in the meantime.
- Renaming a CDK construct changes its logical ID → CloudFormation destroys and recreates the resource.
  ("the #1 way to accidentally drop a production database", per `dev/MENTAL-MODELS.md`.)
- `--cli-read-timeout 300` is needed for the first Lambda invoke after a deploy (cold start > 60 s default).

## 15. Known gaps / next steps

- CORS is `allow_origins=["*"]` — flagged in the code as "tighten before any public deploy".
- Credentials are readable by anyone with `lambda:GetFunction` (accepted for a portfolio project).
- No OpenAPI→TS codegen; the Zod schemas duplicate the Pydantic models by hand.
- Conditional-GET (`ETag`/`Last-Modified`) is implemented in the fetcher but not yet persisted per
  source, so it never actually short-circuits a fetch.
- Per-candidate filtering is partly client-side ("cheap for v1; will matter as the corpus grows").
- Phase 2 planned: an **eval harness** for extractor precision/recall (the measurement discipline that
  `DE_pipeline_stream`'s scorecard already has).