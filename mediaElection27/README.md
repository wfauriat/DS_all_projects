# mediaElection27

**Live dashboard**: <https://d2h6mx4fzeshbf.cloudfront.net>

Tracking how often each 2027 French presidential candidate is mentioned
across the major French media outlets. A daily RSS ingest pipeline
pulls headlines from ~15 French national outlets covering the political
spectrum, normalises them into Postgres, runs candidate-mention
detection, and serves an interactive dashboard showing mention trends
per candidate per outlet over time.

Portfolio / training project: end-to-end ownership of an analytics
pipeline from RSS scraping to deployed React + ECharts dashboard, on
AWS Free Tier, under a hard $5/month gross budget.

## Status

End-of-Week-4 (2026-05-17): public dashboard live, autonomous pipeline
running 4×/day, 605 articles / 318 mentions / 14 candidates / 15
sources in production. Phase 2 eval harness next. Full design lives in
[`dev/PLAN-v1.md`](./dev/PLAN-v1.md); session checkpoints in
[`dev/CHECKPOINT-2026-05-17.md`](./dev/CHECKPOINT-2026-05-17.md).

## Architecture

```
EventBridge (cron 4×/day, 0/6/12/18 UTC)
   │
   ▼
ingest Lambda (NO VPC, free internet egress)
   │   fetches RSS feeds, uploads raw XML to S3
   ▼
S3 raw bucket (Glacier 30d, expire 365d)
   │   S3 ObjectCreated event per uploaded file
   ▼
loader Lambda (IN VPC, PRIVATE_ISOLATED subnets)
   │   parses, dedups, inserts to RDS, runs keyword extractor inline
   ▼
RDS Postgres (db.t3.micro, Free Tier)
   ▲
   │
API Lambda (FastAPI via Mangum, also in VPC)
   ▲
   │
API Gateway HTTP API
   ▲
   │
CloudFront ◀ S3 (static React SPA bundle)
```

**The no-NAT split** is the load-bearing choice: the ingest Lambda
runs outside the VPC where Lambda gets free public-internet egress;
everything that needs to reach RDS runs *inside* a VPC with only
`PRIVATE_ISOLATED` subnets. No NAT Gateway, no Internet Gateway,
nothing whose hourly cost would dominate the Free Tier budget.

## Tech stack

- **Backend**: Python 3.12, FastAPI, SQLAlchemy 2.0 async, asyncpg, alembic, structlog
- **Frontend**: Vite + React 19 + TypeScript, TanStack Query, ECharts, Tailwind v4, Zod
- **Infrastructure**: AWS CDK in Python — 7 CloudFormation stacks in `eu-west-3`
- **CI/CD**: GitHub Actions with OIDC federation to AWS (no long-lived keys)
- **Tooling**: uv, ruff, mypy `--strict`, pytest + testcontainers-postgres

## Local development

Requires Python 3.12 and Docker.

```bash
make install      # creates .venv and installs project + dev deps
make db-up        # starts Postgres in docker
make migrate      # applies alembic migrations
make seed         # loads candidates.yaml + sources.yaml
make ingest-once  # fetches all RSS feeds once
make extract      # runs keyword extractor over articles
make api          # starts FastAPI on http://localhost:8000
```

For the dashboard (in another terminal, after `make api` is up):

```bash
make frontend-install    # one-time: installs npm deps
make frontend-dev        # starts Vite on http://localhost:5173
```

`make help` lists all targets.

## Cloud deployment

Every push to `main` triggers `.github/workflows/deploy.yml`:

1. OIDC-authenticated assumption of `media27-github-deploy` role
2. `cdk synth` (sanity check)
3. `cdk deploy` of the six backend stacks (`Media27Network`,
   `Media27Runtime`, `Media27Data`, `Media27Ingest`, `Media27Api`,
   `Media27Observability`)
4. Reads the API URL from CloudFormation outputs
5. Builds the frontend with `VITE_API_BASE_URL` baked in
6. `cdk deploy Media27Frontend` to push the SPA to S3 + invalidate
   CloudFront

`.md` files and `LICENSE` / `.gitignore` are in `paths-ignore` so
documentation-only changes don't trigger a redeploy.

### Admin operations on the loader Lambda

The loader Lambda doubles as the in-VPC admin entrypoint:

```bash
LOADER=$(aws lambda list-functions --region eu-west-3 \
  --query "Functions[?starts_with(FunctionName,'Media27Ingest-LoaderFn')].FunctionName" \
  --output text)

# Run pending alembic migrations
aws lambda invoke --function-name "$LOADER" --region eu-west-3 \
  --cli-binary-format raw-in-base64-out --cli-read-timeout 300 \
  --payload '{"action":"migrate"}' /tmp/out.json

# Re-seed candidates + sources from bundled YAML
aws lambda invoke ... --payload '{"action":"seed"}' /tmp/out.json

# Manual keyword extraction run
aws lambda invoke ... --payload '{"action":"extract"}' /tmp/out.json
```

The `--cli-read-timeout 300` is necessary on cold starts — the AWS
CLI's default 60 seconds is too short for the first invocation after
a deploy.

## Costs

Hard target: **gross spend under $5/month** throughout the 12-month
build + campaign window, then migrate to Neon + Vercel after the
April 2027 election.

- Billing alarms in `us-east-1` at $1 and $5 (non-negotiable)
- AWS Budgets with notifications at 80% and 100% of $5
- S3 lifecycle: Glacier-IR after 30 days, expire after 365 days
- Lambda `log_retention=ONE_WEEK` on every function
- All resources tagged `project=media27` for Cost Explorer

The most likely Free-Tier-exhaustion vector is the 100 GB/month
data-transfer-out limit, which is roughly 200k full page loads. We
are nowhere near it; the $5 budget alarm will fire on any anomaly
long before any quota is hit.

## Project layout

- `app/api/` — FastAPI service (routers, Mangum Lambda handler)
- `app/ingest/` — RSS fetcher + parser + loader Lambda
- `app/extract/` — keyword extractor with alias-based disambiguation
- `app/db/` — SQLAlchemy ORM + alembic migrations
- `app/sources/` — idempotent YAML seeder
- `frontend/` — Vite + React + TypeScript SPA
- `infra/cdk/` — seven CDK stacks
- `seeds/` — `candidates.yaml` and `sources.yaml` (the editorial config)
- `tests/` — `unit/` (extractor, parser) + `integration/` (testcontainers + httpx async client)
- `.github/workflows/` — CI + deploy

## Documents

All design and reference docs live under [`dev/`](./dev/):

- [`dev/PLAN-v1.md`](./dev/PLAN-v1.md) — original design document with
  the full plan, schema, and phase milestones
- [`dev/MENTAL-MODELS.md`](./dev/MENTAL-MODELS.md) — reference doc for
  the IaC and data-engineering mental models the project exercises
- [`dev/CHECKPOINT-2026-05-17.md`](./dev/CHECKPOINT-2026-05-17.md) —
  end-of-Week-4 notes: what shipped, what nearly didn't, what's next
- [`dev/CHECKPOINT-2026-05-15.md`](./dev/CHECKPOINT-2026-05-15.md) —
  end-of-Week-3 notes from the local-pipeline-complete milestone
- [`dev/SPECIFICATION-REBUILD.md`](./dev/SPECIFICATION-REBUILD.md) —
  self-contained rebuild specification useful for a hand-implementation
  in a separate training repository
- [`dev/cheatsheet_sql.md`](./dev/cheatsheet_sql.md) — Postgres queries
  used during eval and analysis
- [`dev/schema_db.md`](./dev/schema_db.md) — schema notes / reasoning

## Footer

Données : flux RSS publics des outlets référencés. Le projet stocke
uniquement les titres et les liens, et redirige toujours vers
l'article original chez l'éditeur. Posture compatible avec le droit
voisin pour usage non-commercial / pédagogique.
