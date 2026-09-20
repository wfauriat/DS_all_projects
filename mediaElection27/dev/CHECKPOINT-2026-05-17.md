# Checkpoint — 2026-05-17

End-of-session notes after the Week 4 AWS deploy. Captures what shipped,
what nearly didn't, and which decisions held up vs needed walking back.

---

## What shipped today

Public, autonomous, end-to-end:

- **Dashboard**: `https://d2h6mx4fzeshbf.cloudfront.net` (CloudFront → S3, OAC)
- **API**: `https://ewrlfbd0x2.execute-api.eu-west-3.amazonaws.com` (HTTP API → Lambda → RDS)
- **Pipeline**: EventBridge cron (0/6/12/18 UTC) → ingest Lambda (no VPC) → S3 → loader Lambda (in VPC) → RDS, with the keyword extractor running inline after each batch
- **Cost guardrails**: $1 and $5 budget alarms in `us-east-1`, S3 lifecycle (Glacier 30d, expire 365d), `log_retention=ONE_WEEK` on every Lambda

Seven CloudFormation stacks live: `Media27Network`, `Media27Runtime`,
`Media27Data`, `Media27Ingest`, `Media27Api`, `Media27Frontend`,
`Media27Observability` (+ `CDKToolkit` bootstrap).

Current DB content (post-bootstrap): **605 articles · 318 mentions ·
14 candidates · 15 sources · 30 ingest_runs**.

---

## Crises caught during the first real deploys (and what they teach)

1. **EC2 SecurityGroup descriptions must be ASCII-only.**
   The first `cdk deploy` failed because an em-dash (`—`, U+2014) snuck
   into `RdsSg(description="Postgres — accepts traffic…")`. AWS rejects
   non-ASCII with `Character sets beyond ASCII are not supported` *and*
   rolls the whole stack back. Fix: ASCII hyphen only. Other resource
   descriptions (IAM, Lambda, SNS) are more permissive — only EC2 is
   this strict.
   *Lesson:* the most surprising production constraints come from the
   oldest services. EC2 inherited string validation from 2006.

2. **PRIVATE_ISOLATED RDS cannot be made publicly accessible.**
   Picked "option A — temporarily make RDS publicly accessible" for the
   one-time migration. Walked it back: `publicly_accessible=True` only
   takes effect when the DB Subnet Group includes a public subnet. With
   only `PRIVATE_ISOLATED` subnets, there literally is no public path.
   The no-NAT architecture means anything that touches RDS must be
   *inside* the VPC. Fix: extended the in-VPC loader Lambda with
   `{"action":"migrate"}` and `{"action":"seed"}` admin payloads.
   *Lesson:* "I'll just toggle X for a minute" assumes a fully-public
   network model that this project deliberately doesn't have.

3. **PRIVATE_ISOLATED Lambdas can't reach Secrets Manager.**
   The obvious cold-start pattern — `boto3.client("secretsmanager")`
   to resolve the RDS credentials — hangs forever. There's no public
   internet route, no NAT, no VPC interface endpoint. Connect timeout
   after ~4 minutes; Lambda init-time budget exhausted. Adding the VPC
   interface endpoint costs ~$0.01/hr × 2 AZs ≈ $14.60/mo, which blows
   the $5 budget. Fix: CDK token injection —
   `db_secret.secret_value_from_json("password").unsafe_unwrap()` in
   the Lambda env vars. CloudFormation resolves the
   `{{resolve:secretsmanager:...}}` dynamic reference at deploy time
   and bakes plaintext into the function config. The handler reads
   `os.environ["DB_PASSWORD"]` directly — no boto3 call.
   *Lesson:* "isolated" subnets mean isolated from everything,
   including AWS's own public APIs. If the budget can't cover VPC
   endpoints, work around the runtime call entirely.
   *Trade-off accepted:* credentials are visible to anyone with
   `lambda:GetFunction`. Fine for a portfolio project; revisit if the
   threat model tightens.

4. **CloudFormation refuses to update a cross-stack export while in use.**
   Adding `alembic` to the shared Lambda layer changed the LayerVersion
   ARN. The `Media27Runtime` stack tried to update its export to the
   new ARN — CloudFormation refused: `Cannot update export … as it is
   in use by Media27Api and Media27Ingest`. The producer rolled back,
   leaving the consumers still pinned to the old layer. CDK's
   `--concurrency` doesn't solve this; the export-update step happens
   before consumers are touched. Fix: `aws cloudformation delete-stack`
   on Observability → Api → Ingest (in that order, since Observability
   imports the others' Lambda ARNs), then re-trigger the workflow with
   the export now unused.
   *Lesson:* cross-stack exports are an implicit contract that survives
   replacement. Long-term cleaner: publish ARNs via SSM Parameter Store
   and have consumers read with `ssm.StringParameter.value_for_string_parameter`
   — same value resolved per-stack, no export at all.

5. **VPC-attached Lambda deletion takes 15–30 minutes.**
   `DELETE_IN_PROGRESS` looks stuck. It isn't — every VPC Lambda
   attaches an ENI (Elastic Network Interface) per subnet, and AWS
   takes its time releasing them. No events appear in the stack event
   log during the wait. Plan accordingly when destroying VPC stacks.
   *Lesson:* CloudFormation stack-deletion latency is dominated by the
   slowest underlying resource, not the count of resources.

6. **CDK bundles assets via Docker; the bundling image takes a while.**
   `cdk synth` of the runtime stack triggers `pip install` against the
   official Python 3.12 Lambda runtime image. First run pulls the
   image (~1 GB). Subsequent runs are fast. CI/CD does this fresh each
   time, adding ~30s to every deploy.
   *Lesson:* prefer Docker bundling over local pip for reproducible
   layers, but budget the time.

---

## Design decisions that already paid off

1. **One shared Lambda layer for deps.** Lambdas themselves stay
   tiny (just `app/`), so deploy churn is fast. The layer changes
   rarely; consumer Lambdas re-link to the new ARN.
   Downside surfaced today: when the layer *does* change, the
   cross-stack export ripples through (see crisis #4).

2. **No-NAT split (ingest outside VPC, loader inside).** Free-tier
   compatibility hinged on this. Ingest fetches feeds with free
   internet egress; loader runs in PRIVATE_ISOLATED and only needs
   S3 (gateway endpoint, free) + RDS (in-VPC). Validated in production.

3. **Loader Lambda as admin entrypoint.** The same function that
   processes S3 events accepts `{"action": "migrate" | "seed" |
   "extract"}` payloads. No new Lambdas needed for one-off ops; one
   IAM role, one VPC config, one CloudWatch log group. The handler
   dispatches on event shape.

4. **OIDC trust to GitHub Actions instead of long-lived access keys.**
   Set up once at the start of the day; every push to `main` now
   deploys via federated credentials, scoped to
   `repo:wfauriat/mediaElection27:ref:refs/heads/main`. Nothing to
   rotate, nothing to leak.

5. **`removal_policy=SNAPSHOT` on RDS.** Discussed and chosen
   yesterday. Means: if the RDS resource is ever deleted (by `cdk
   destroy` or stack churn), a final snapshot is taken instead of
   silent data loss. Belt-and-braces with the planned monthly pg_dump.

---

## Operational gotchas worth a sticky note

- **`UPDATE_ROLLBACK_COMPLETE` ≠ `ROLLBACK_COMPLETE`.** The first is
  recoverable (`cdk deploy` works against it). The second is terminal
  (must `delete-stack` first). The two-letter-prefix difference is the
  only signal.
- **The AWS CLI `lambda invoke --cli-read-timeout` default is 60s.**
  Cold-start a VPC Lambda for the first time and you hit this. Pass
  `--cli-read-timeout 300` for any first-invoke admin call.
- **`gh run rerun <id>`** re-runs the failed jobs at the same commit
  SHA. Useful when an environmental issue (export-in-use, transient
  service blip) caused the failure and the code is unchanged.
- **`auto_delete_objects=True` on S3 buckets is real magic** — without
  it, `delete-stack` on Ingest would have failed because S3 doesn't
  delete non-empty buckets. CDK adds a custom resource that empties
  the bucket before delete. Side effect: stack deletion costs an extra
  Lambda invocation.
- **Frontend caches API URL at build time** via `VITE_API_BASE_URL`.
  Recreating the API stack changes the URL (new API Gateway ID); the
  CI pipeline rebuilds the frontend automatically (Phase 2 of
  `deploy.yml`), so the dashboard heals itself. Dashboard CloudFront
  URL stays stable across redeploys.

---

## Things likely to bite later (ordered by horizon)

| When | What | Why |
|---|---|---|
| Next 1-2 sessions | Inline extract scans full pending set 15× in parallel | Each S3 event runs an independent `run_extract`. Idempotent (unique constraint) but wasteful. Narrower variant scoped to current batch is the fix. |
| Pre Week 5 milestone | API CORS still `*` | Tighten to the CloudFront distribution origin once the domain is stable. |
| Before 2026-06-02 | GitHub Actions Node 20 → 24 enforcement | `actions/checkout@v4`, `setup-node@v4`, `setup-python@v5`, `aws-actions/configure-aws-credentials@v4` all run on Node 20. Default flips on June 2nd. |
| Month 7 (~Dec 2026) | RDS leaves Free Tier; ~$13/mo gross spend kicks in | $200 credits cover months 7-20. The $5 monthly budget will start firing — switch to `cost_types={"include_credit": False}` so it tracks net spend. |
| Month ~21 (~Feb 2028) | Credits depleted | Migration to Neon + Vercel per the original plan. Election is April 2027, so this lines up. |
| Anytime | `log_retention` arg deprecated in favor of `logGroup` | Warning surfaces on every deploy. Mechanical replacement. |
| Anytime | Secret values bake into Lambda env vars at deploy time | Means the password is visible to anyone with `lambda:GetFunction`. Rotating the secret requires a redeploy (no live rotation). For a portfolio project: fine. |

---

## What remains before the Week 5 milestone (2026-06-19)

Plan target: deployed dashboard + public write-up. Status: dashboard is
deployed; matcher autonomous; data flowing. Hard things are done.

| Priority | Item | Effort |
|---|---|---|
| Must | Frontend end-to-end smoke: open `https://d2h6mx4fzeshbf.cloudfront.net`, confirm timeseries renders, filters work, no console errors | 15 min |
| Must | One full cron cycle observed end-to-end (next 0/6/12/18 UTC) — confirm mentions grow without manual intervention | passive |
| Should | Tighten API CORS from `*` to the CloudFront distribution origin | 20 min, 1 stack redeploy |
| Should | README front-matter update: badges, screenshot, "what this does" paragraph, the live URL | 30 min |
| Should | Short write-up for portfolio / LinkedIn — three things worth highlighting: no-NAT architecture, the cost-discipline pattern, the keyword-vs-NER staged approach | 1-2 hrs |
| Could | Add `cost_types={"include_credit": False}` to the CfnBudget construct — pre-empt month-7 noise | 10 min |
| Could | Replace `log_retention` with `logGroup` across all Lambda Functions | 30 min |
| Could | Narrow the inline extract to articles from the current batch only | 1 hr |

Nothing on this list is structurally hard. The pipeline works; the
remaining items are polish, observability, and the actual write-up.

---

## Weeks 6-8 — Phase 2 eval harness

Per `PLAN-v1.md`:

1. Hand-label 300 articles (~20/outlet) into
   `tests/eval/labeled_samples.jsonl`.
2. `scripts/run_eval.py` computes per-candidate precision / recall /
   F1. Macro- and micro-averaged.
3. Apply the NER decision rule: ship keyword-only if macro-F1 ≥ 0.90
   **and** every candidate's recall ≥ 0.85 **and** no candidate's
   precision < 0.80.
4. If recall failures dominate, expand `candidate_aliases.yaml` and
   rerun before adding NER.
5. If still under threshold: `fr_core_news_md` or CamemBERT-NER.
6. `mention_daily_counts` materialized view + nightly refresh Lambda.
7. `/stats` page surfaces current macro-F1.

The eval is the project's analytical proof point and the artefact that
makes the whole thing a portfolio piece rather than "another scraper".

---

## Beyond Phase 2

Per the original plan: stacked-area share-of-voice, per-source
drilldown, CSV export, Phase 3 sentiment (only if v1 feels complete).
Election in April 2027; migration to Neon + Vercel after the second
round.

---

## Status one-liner for the future

> AWS infrastructure live and autonomous as of 2026-05-17. Seven CDK
> stacks deployed to eu-west-3; ingest → loader → extract pipeline
> runs 4×/day on its own; dashboard public at
> `d2h6mx4fzeshbf.cloudfront.net`. 605 articles / 318 mentions / 14
> candidates / 15 sources in production RDS. Next chunk: Week 5
> public-deploy write-up + Phase 2 eval harness.
