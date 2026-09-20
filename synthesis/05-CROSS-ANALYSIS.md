# Cross-analysis — what is common, what is specific, and why

Read after the four per-project syntheses. Part A is what transfers; Part B is what does not, and the
constraint that explains each divergence.

---

## 0. The four projects in one table

| | **DE_pipeline_stream** | **mediaElection27** | **energy_predML** | **refund-lab-autom** |
|---|---|---|---|---|
| Genre | reference wiring of a modern streaming stack | deployed production web product | MLOps lifecycle | adversarial data-quality / detection lab |
| Primary deliverable | a readable, correctly-wired stack | a public dashboard | a served, monitored model | `incidents.jsonl` |
| Data motion | continuous stream + batch pulls | scheduled batch (4×/day) | scheduled batch (daily intent) | scheduled batch (1 simulated day/cycle) |
| Storage | Kafka + DuckDB + Parquet lake | Postgres (RDS) | DuckDB + Parquet | SQLite (bitemporal) |
| Transform | dbt (SQL/Jinja) | SQL in the API layer + Python | pandas | hand-written SQL in Python |
| Orchestration | Airflow 3 (real scheduler, assets) | EventBridge cron + S3 events | Prefect decorators (**no scheduler running**) | a hand-rolled `while` loop + `loop.sh` |
| Serving | Streamlit on a serving copy | FastAPI+Mangum → API GW → React SPA | FastAPI + Streamlit | CLI (`asof.py`, `score.py`) |
| ML | none (rule detectors) | regex/alias matcher (versioned in the data) | XGBoost + Optuna + MLflow registry | HistGradientBoosting, point-in-time |
| Deployment | Docker Compose, local | AWS, 7 CDK stacks, GH Actions OIDC | Docker Compose, dual local/docker modes | none — one process on a host |
| Dominant constraint | *readability of the plumbing* | **< $5/month** | full lifecycle coverage | **specificity of claims** (false positives are scored) |
| Ground truth available? | yes (injected fault log) | no | no | yes, but **sealed** |
| Lines of code (approx.) | 5k | 4k (Py+TS) | 1.8k | 3.2k |

**The single most useful framing:** these are four different answers to *"where does the difficulty
live?"* — in the wiring (1), in the cost and deployment surface (2), in the model lifecycle (3), in the
data itself (4). Almost every difference below follows from that.

---

# Part A — what is common

## A1. The landing zone is sacred, and typing happens later (ELT)

All four separate "what arrived" from "what it means", and all four give the reason explicitly.

| Project | Raw form | Where typing happens |
|---|---|---|
| DE_pipeline_stream | `raw.<topic>(topic, partition, offset, msg_ts, headers VARCHAR, value VARCHAR)` — JSON kept as **text, not the JSON type**, "so a malformed message can never block landing" | dbt staging views (`value ->> '$.payload.x'` + casts) |
| mediaElection27 | raw feed XML in S3 (`feeds/YYYY-MM-DD/{slug}.xml`), plus the whole parsed entry in `articles.raw JSONB` | the parser + ORM columns |
| energy_predML | timestamped raw JSON in `data/raw/{REGION}_{ts}.json` | `validate.py` → typed DataFrame |
| refund-lab-autom | `landing/` raw bytes and raw JSONL.gz "exactly as delivered, never rewritten"; `src_*` rows as delivered | `normalize.py` + the conformed layer |

**Transferable rule:** *schema drift should break your model, never your ingestion.* The corollary
appears in all four: keep the untouched bytes so an event can be replayed once the bug is fixed
(DE's DLQ carries `raw`; refund-lab's `src_*` keeps every delivered version).

## A2. Idempotency is designed in, not hoped for

Every project can re-run any stage safely, and every one uses the same two primitives.

| Primitive | Where |
|---|---|
| natural key + `ON CONFLICT DO NOTHING` / `INSERT OR IGNORE` | mediaElection27 (`(source_id, guid)`, the positional mention key); energy_predML (`INSERT … WHERE NOT EXISTS` on `(timestamp, region)`); refund-lab (`INSERT OR IGNORE` into every `src_*`) |
| version- or key-guarded upsert | DE (dbt `delete+insert` on `trip_id`); refund-lab (`… DO UPDATE … WHERE excluded.version > cur.version`); energy_predML (`log_prediction` `DO UPDATE`) |
| snapshot tables replaced wholesale | DE (`raw.stations`/`raw.bikes` `DELETE`+insert); mediaElection27 (aliases delete-all + reinsert per candidate) |

And all four use `.returning(id)` / count-delta to distinguish *inserted* from *already there*, because
that difference is itself a metric (duplicates, late arrivals, re-sends).

## A3. Atomic file swap for anything a reader might open mid-write

`tmp` + `os.replace` appears **three times independently**, always with the same argument (on Linux the
old inode stays alive for an open reader, so nobody sees half a file and nobody waits):

- DE: `bikeshare_serving.duckdb` published by `publish.py`; also `state.pkl` and the bridge checkpoint.
- energy_predML: not used (MLflow proxies artifacts instead) — but the same *problem* is solved by
  `--serve-artifacts`.
- refund-lab: SQLite WAL + single writer plays the role.

The reader-side half of the handshake is equally transferable: **cache by data version, not by clock**
(DE's Streamlit `@st.cache_data(path, mtime)` + a 30 s fragment comparing mtime).

## A4. Time is never one thing

Every project has an explicit multi-clock discipline, and every one states that conflating them is the
classic bug.

| Project | Clocks |
|---|---|
| DE_pipeline_stream | `event_time` (sim) · `emitted_at` (sim) · `ingested_at`/`loaded_at`/`detected_at` (wall). Spark windows use event time; dbt freshness uses landing time; Airflow schedules on wall time |
| refund-lab-autom | **valid time** (`occurred_at`/`valid_from`) vs **knowledge time** (`knowledge_time`) vs pipeline cycle. The bitemporal store is literally built on the distinction |
| mediaElection27 | `published_at` (the outlet's) vs `fetched_at` (ours), and a **timezone-parameterised day boundary** (`AT TIME ZONE :tz`, default Europe/Paris) so a "day" is the editorial day |
| energy_predML | target hour vs `predicted_at` vs the **training cutoff** (`train_cutoff.txt`) that defines "reference vs current" for drift |

**Transferable rule:** decide, per column, *whose clock is this and what is it for*, then write it down
where both producers and consumers will read it. Three of the four also had a dated bug caused by
getting a clock wrong (DE's mid-step snapshot timestamps; refund-lab's lateness measured in days
instead of cycles; mediaElection27's naive dates assumed Paris).

## A5. Late-arriving data is designed for, using landing time as the incremental key

The same insight, three ways:
- DE: `fct_trips` is incremental on **`loaded_at`, not `event_time`** — "filtering on landing time is
  what makes late events safe"; and `int_trips` uses `max(event_time)` as a batch-side watermark.
- refund-lab: rolling re-pull + reconciliation + audit exist *because* the API cannot answer "what
  became knowable since I last looked"; late labels/payments mark their **order's** partition dirty.
- energy_predML: lag features are computed at request time from history, so a late actual is picked up
  on the next feature build.

## A6. "Tests" means at least three different things, and all four build all three

| Layer | DE_pipeline_stream | mediaElection27 | energy_predML | refund-lab-autom |
|---|---|---|---|---|
| **unit** (pure logic) | clock math, fault signatures, SSE parse, Spark rules on static DataFrames | 16 matcher tests reading as a spec | lag index arithmetic, cyclical bounds, metrics | deterministic detectors vs synthetic corruption |
| **integration** (real components, fake edges) | DuckDB in `tmp_path`, `FakeProducer`, `MockTransport` SSE, `AppTest` | `ASGITransport` + real DB, auto-skipped if Postgres is down | `tmp_duckdb` monkeypatch + `seed_demand`, only the *model* mocked | `test_drop_to_monitor` |
| **data tests** (assertions about the data itself) | dbt generic + custom generic + singular (stored failures) + unit test + contract + freshness | (gap — Phase 2 "eval harness") | value assertions inside `build_features` | `Transformer.test()`, 14,862 pass / 335 fail over the run |
| **end-to-end** | `scripts/smoke.py`, 6 ordered hops | CI (unit only) + a live deploy | `scripts/test_commands.sh` | the 2200-cycle run itself |

**Shared mocking discipline:** mock the *network* and the *model*; never mock the data layer or the
feature code. Both DE and energy_predML state this explicitly, and both use a temp database rather than
a fake one.

## A7. Self-documenting Makefile as the single human entry point

Three of four use the identical idiom — `grep`/`awk` over `## ` comments to generate `make help` —
and all three keep the "how do I poke at this" commands next to the thing they poke. DE takes it
furthest (`##@` section headers, raw `curl` calls left visible so the HTTP contract is readable, jq
programs hoisted into variables). refund-lab replaces it with `run.py <verb>` — same role, no Make.

The generalisable point: **there should be exactly one file a newcomer can read to learn what they can
do**, and its entries should be runnable verbatim.

## A8. Config is layered, and every wiring point is commented on both ends

- DE: `.env` → compose `${VAR}` → container env → module docstring listing the variables it reads;
  `PLAN.md` §11 makes it a rule ("each wiring point is commented on both ends") and TOUR.md §3 has an
  explicit *"I want to change X → edit Y, also keep Z in sync"* table.
- mediaElection27: `pydantic-settings` + `.env`; CDK env vars; `VITE_API_BASE_URL` baked at build time.
- energy_predML: `pydantic-settings` + path constants; per-service requirements files.
- refund-lab: one `config.py` holding *everything environmental*, including a deliberately narrow
  five-line regex read of the vendor's `lab.toml` ("we read exactly the two keys we are entitled to").

Three of the four use **pydantic(-settings) or a frozen dataclass** for settings, validated at load.

## A9. Cross-component thresholds are duplicated — and the duplication is annotated

DE's `status_interval_min` lives in `config/source.toml`, in Spark's compose env, and in
`dbt_project.yml` vars; each copy carries a comment naming the others. refund-lab's `DOMAINS` mirror
the vendor's documented closed domains. energy_predML's `NON_FEATURE_COLS` is the single definition
used by training, serving, monitoring and the dashboard — the counter-example that shows the better
solution when the components share a process.

**Rule of thumb observed:** if two runtimes cannot import the same constant, duplicate it and comment
both ends; if they can, define it once and import it.

## A10. Measurement before design, and honest numbers after

- refund-lab: `NOTES.md` §2 — nine measured facts about the vendor, each with its design consequence.
- DE: `logging_build.md` — every tuning decision carries the measurement that forced it (30-min health
  windows 8 % precision → 2 h 86 %; orphaned-task recovery 300 s → 87 s).
- energy_predML: a baseline that is one column (`demand_lag_24h`), so "RMSE 501" means something.
- mediaElection27: cost arithmetic (NAT ≈ $32/mo, VPC interface endpoint ≈ $14.60/mo) driving architecture.

All four also record **what they got wrong**, at length, in a dated document. This is arguably the most
transferable practice in the whole corpus.

## A11. Detection is scored, not asserted

Two projects build an explicit precision/recall apparatus against ground truth:
- DE's `mart_detection_scorecard` (seed-driven matching, `explained_by_other_faults`, a judging horizon
  so not-yet-detectable faults are not counted as misses).
- refund-lab's whole design (dedupe policy, retractions, coverage table with per-fault miss risk).

Both arrive independently at the same subtle point: **a "false positive" caused by a *different* real
fault is a finding, not noise** — DE calls it `explained`, refund-lab spends a retraction working out
that its `id_reuse` hit was actually `backfill_restatement`.

## A12. Structured JSON logging + a periodic throughput line

DE's bridge (`JsonFormatter` + `event(log, msg, **fields)`) and Spark listener (one JSON line per
micro-batch) and refund-lab's transport stats snapshot are the same pattern: **one machine-readable
line per unit of work, carrying the counters you will want when it misbehaves.** mediaElection27 stores
the equivalent as *rows* (`ingest_runs`), which is the same idea with SQL as the query language.

---

# Part B — what is specific, and why

## B1. Orchestration: four points on one axis

| Project | Choice | The constraint that forced it |
|---|---|---|
| DE_pipeline_stream | **Airflow 3**, LocalExecutor, pools, assets, data-aware scheduling | the brief was *"show a modern stack correctly wired"*; Airflow is the thing being demonstrated. Also: DuckDB's single writer needs a **pool with one slot**, which is an orchestrator feature |
| mediaElection27 | **EventBridge cron + S3 event** | serverless and free; an always-on scheduler would cost money. The event-driven hop (S3 ObjectCreated → loader) *is* the orchestration |
| energy_predML | **Prefect as a decorator library, no scheduler running** | the brief was the ML lifecycle; scheduling was the one deferred item. Honestly flagged: "transforms the project from *pipeline that can run* to *pipeline that does run*" |
| refund-lab-autom | **a `while` loop + `loop.sh`** | the loop must advance a *simulated* clock and retry the **same** simulated day on failure. No off-the-shelf scheduler has that semantics, and the run is one process for a few hours |

**The transferable question** is not "which orchestrator" but *what does a run boundary mean here?* —
a wall-clock interval (DE), an event (mediaElection27), a manual invocation (energy_predML), or a tick
of a simulated clock the runner owns (refund-lab).

## B2. Storage: the same reasoning, four answers

- **DuckDB** (DE, energy_predML) — embedded, analytical, zero ops. DE then spends real design effort on
  the consequence (single writer → pool, retrying connect, serving copy, structure-only copy for docs).
- **Postgres/RDS** (mediaElection27) — because it is a *multi-reader transactional web backend*, and
  because Lambda concurrency means several writers.
- **SQLite** (refund-lab) — stated plainly: DuckDB wasn't available, the working set fits, one file,
  crash-safe, no service to babysit for an unattended run.
- **Kafka + Parquet lake** (DE only) — because it is the only project with a genuine stream, and
  because Kafka is the fan-out point that gives Spark and the loader independent offsets.

Note what *doesn't* differ: all four keep an **audit table in the same database as the data**
(`raw._ingest_batches`, `ingest_runs`, `predictions`/`model_runs`, `pull_log`/`test_results`), so
"what happened in the pipeline" is answerable in SQL next to "what the data says".

## B3. Contract enforcement: three positions on the same spectrum

| Project | Where the contract is enforced | What happens on a violation |
|---|---|---|
| DE_pipeline_stream | **at ingestion**, strict Pydantic in the bridge (`extra="forbid"`, `strict=True`) | dead-letter topic with the errors and the raw text; the rest of the pipeline can then assume shape |
| refund-lab-autom | **nowhere** — the contract is a fiction the vendor does not honour | normalise, repair, *record*: unknown fields to `extra_json`, renamed fields followed by similarity, ids re-padded; every repair is counted as evidence |
| mediaElection27 | **at the edges of the process**: Pydantic response models out, Zod parsing in the browser | a 422 / a thrown `ApiError` |
| energy_predML | **at the boundary of the data layer**: `validate_records` raises; `build_features` asserts no nulls | the run fails loudly |

DE and refund-lab are the instructive pair, because they are *opposites* and both are right for their
setting. DE owns both ends of the contract, so rejecting is cheap and dead-lettering preserves the
evidence. refund-lab does not own the vendor and cannot afford to drop data, so it repairs and reports.
DE's own doctrine actually contains refund-lab's rule in miniature: **syntax at ingestion, semantics
downstream** (a negative bike count is dead-lettered; "more bikes than docks" needs reference data and
is left for Spark and dbt).

## B4. Detection: rules in SQL vs detectors in Python

Both DE and refund-lab detect data faults against ground truth, but they are built oppositely:

| | DE_pipeline_stream | refund-lab-autom |
|---|---|---|
| Where | Spark DataFrame rules (hot path) + dbt SQL models (batch) | 17 Python methods over one evidence bundle per cycle |
| Matching to truth | a **seed CSV** (`detection_checks.csv`) drives the join; adding a detector = adding rows | a coverage table in prose; matching is done by the sealed grader |
| Tuning | measured and recorded (window sizes, thresholds) | deliberately *not* tuned where a threshold would be blind; narrowed after false positives |
| Redundancy | deliberate and *celebrated* — the scorecard shows bridge+Spark+dbt as complementary | deliberately *avoided* — two detectors naming one event is "a guaranteed false positive" |

The difference is entirely the scoring regime. DE is measuring *the pipeline*, so overlapping detectors
are informative. refund-lab is being scored on *claims*, so overlap is a liability. **The same
technical fact — two detectors firing on one event — is a feature in one and a bug in the other.**

## B5. Streaming vs batch: DE is the only one, and it shows

DE is alone in needing watermarks, windows, stream-stream joins, dedup-within-watermark, checkpoints,
and three different ways to remember a consumer position. It is also the only one where *the same
computation is done twice on purpose* (`mart_stream_vs_batch` compares Spark's windows with SQL's) —
which is the clearest available demonstration of the streaming/batch trade-off:

> Spark's 2-hour windows are a heuristic; dbt's `LAG`-based sequential check is exact but noisier.
> Spark sees it in 20 s; dbt sees it a landing later. Neither is "the right answer".

refund-lab reaches the same conclusion from the other direction: its exactly-comparable detectors are
deterministic single-observation ones, and its *statistical* detectors are "the weakest part".

## B6. ML: three different jobs called "the model"

| Project | Model | The hard part |
|---|---|---|
| mediaElection27 | a regex/alias matcher | **disambiguation** ("Le Pen" requires "Marine") and span subsumption; versioned in the data so a future NER model coexists |
| energy_predML | XGBoost + Optuna | **leakage** (`shift(1)` everywhere), temporal splits, train/serve skew, the registry as deployment |
| refund-lab-autom | HistGradientBoosting | **censoring** (no label row until the window closes) and **point-in-time features** (only labels whose `knowledge_time` preceded the order) |

Shared across all three: **an explicit, cheap baseline or screen** (energy's lag-24h column;
refund-lab's univariate AUC leakage screen that raises an incident; mediaElection27's alias counts).
And two of three keep the *model version in the row* rather than in a deployment config
(`(extractor, extractor_version)`; `predictions(order_id, model_version)`), which makes A/B comparison a
`GROUP BY`.

The sharpest contrast is **evaluation honesty**:
- energy_predML reports a holdout AUC and admits `scripts/compare_models.py` shows the 24 h recursive
  horizon is a different (worse) problem.
- refund-lab writes `evaluate.py` to be *prospective*: score each order with the number actually
  written at ingest, by whichever model was live then. "Not the training holdout — that is a self-report."

## B7. Deployment surface: from `make up` to seven CloudFormation stacks

mediaElection27 is the only project with a real deployment, and it is the only one where the *deployment
model constrains the application code*:
- credentials injected as plaintext env vars because isolated subnets cannot reach Secrets Manager;
- a cold-start hook rewriting the DSN *before* any engine is built, forcing a documented import-order
  violation;
- the loader Lambda doubling as the admin entrypoint because nothing else is inside the VPC;
- the frontend's API URL baked in at build time, which forces a two-phase deploy.

The others deploy to `docker compose up` (DE, energy_predML) or to nothing at all (refund-lab). The
transferable lesson is in `dev/MENTAL-MODELS.md`: **CDK Python → CloudFormation template → real
resources** are three artefacts with three destinations, `cdk synth` is free and local, and renaming a
construct changes its logical ID and therefore destroys and recreates the resource.

## B8. Dashboards: three, all different, all reasonable

| | DE (Streamlit + Altair) | mediaElection27 (React + ECharts) | energy_predML (Streamlit + matplotlib) |
|---|---|---|---|
| Reads | the serving copy, and nothing else | the API | DuckDB + MLflow directly |
| State | one slider, `st.cache_data` keyed on mtime | **all filter state in the URL** | sidebar widgets |
| Chart discipline | palette validated for colour-vision deficiency, fixed slot per detector across every chart, tooltips everywhere, a table view per chart, explicit UTC scale, near-zero series rendered as a table | stable palette slot per candidate id, lean-ordered sorting, fr-FR string table | default styling |
| Why it differs | it is a *reader of the serving layer*, deliberately no more special than a notebook | it is the product | it is a debugging surface for the ML loop |

DE's and mediaElection27's shared instinct is worth extracting: **an entity keeps its colour
everywhere**, and the ordering of categories encodes domain meaning (pipeline order; political lean).

## B9. Documentation genres

Each project independently invented a different document, and together they form a usable set:

| Genre | Example | What it is for |
|---|---|---|
| **plan of record** with the verbatim original request | `DE/PLAN.md` | why the architecture is what it is; settles "what were we asked for" |
| **build log / decision journal**, written during the work | `DE/logging_build.md`, `refund-lab/NOTES.md`, `mediaElection27/dev/CHECKPOINT-*.md` | every deviation with its measurement; every mistake at full length |
| **guided tour** with reproducible experiments | `DE/TOUR.md` (10 experiments with expected and *measured* outcomes) | how to learn the system by poking it |
| **mental-models reference**, project-independent | `mediaElection27/dev/MENTAL-MODELS.md` | the transferable concepts, separated from this codebase |
| **focused RFC** on one infrastructure problem | `energy_predML/STRATEGY.md` | problem → target architecture → why it works → stepwise plan |
| **rebuild specification** | `mediaElection27/dev/SPECIFICATION-REBUILD.md` | enough to re-implement from scratch, for practice |
| **coverage table with explicit gaps** | `refund-lab/NOTES.md` §13 | states what would be missed and why, instead of implying completeness |

---

## C. Where each project is strongest — a reading order by topic

| If you want to learn… | Read |
|---|---|
| streaming semantics (watermarks, windows, stream-stream joins, checkpoints) | DE: `streaming/analyzer/rules.py`, `main.py` |
| exactly-once into a warehouse | DE: `orchestration/include/landing/kafka_loader.py` (the argument is in the docstring) |
| orchestration idioms (pools, assets, task factories, connections from env) | DE: `orchestration/dags/*` + `x-airflow-common` in compose |
| dbt as a discipline (layers, snapshots, incremental, tests-as-monitors, contracts, unit tests) | DE: `transform/` in full |
| bitemporal modelling and "what did we believe then" | refund-lab: `pipeline/storage.py` + `asof.py` |
| defensive ingestion from a hostile source | refund-lab: `pipeline/vendor.py`, `extract_drop.py`, `normalize.py` |
| data-quality detection and how to avoid false positives | refund-lab: `pipeline/monitors.py` + `NOTES.md` §6, §11, §13 |
| scoring your own detectors against ground truth | DE: `mart_detection_scorecard.sql` + `seeds/detection_checks.csv` |
| time-series ML hygiene (leakage, temporal CV, train/serve skew) | energy_predML: `src/features/lag.py`, `src/serving/predict.py` |
| model registry as a deployment mechanism, drift → retrain loops | energy_predML: `src/serving/api.py`, `src/workflows/monitor.py` |
| point-in-time features and censoring | refund-lab: `pipeline/ml.py` (+ `evaluate.py` for honest evaluation) |
| async Python ingestion (bounded concurrency, conditional GET, tolerant parsing) | mediaElection27: `app/ingest/fetcher.py`, `parser.py` |
| SQLAlchemy 2.0 async + FastAPI patterns | mediaElection27: `app/db/`, `app/api/` |
| IaC, cost-driven architecture, OIDC CI/CD | mediaElection27: `infra/cdk/`, `.github/workflows/deploy.yml`, `dev/MENTAL-MODELS.md` |
| a modern typed frontend over an analytics API | mediaElection27: `frontend/src/` (Zod at the boundary, URL as state) |
| packaging and environments (uv workspace, per-service images, two run modes) | DE: root `pyproject.toml` + Dockerfiles; energy_predML: `docker/` + Makefile |
| container wiring (profiles, anchors, healthchecks, UID mapping, dual listeners) | DE: `docker-compose.yml` in full |

---

## D. Contradictions worth noticing

Places where two projects do the *opposite* thing on purpose. These are the highest-value
disagreements to understand, because each is right in its own setting.

1. **Reject vs repair on contract violation.** DE dead-letters; refund-lab repairs and records.
   → Do you own the producer? Can you afford to lose the row?
2. **Redundant detectors: feature or bug.** DE celebrates complementary detectors; refund-lab treats a
   second name for one event as a guaranteed false positive.
   → Are you measuring the *pipeline* or being scored on *claims*?
3. **Duplicate the constant vs define it once.** DE duplicates thresholds across runtimes with
   cross-references; energy_predML defines `NON_FEATURE_COLS` once and imports it everywhere.
   → Can the consumers share a process?
4. **Dedup scope.** mediaElection27 *removed* a global `UNIQUE(content_hash)` because each outlet's
   pick-up of the same AFP wire is a distinct data point; DE dedups aggressively on `event_id` but
   keeps two counters so a source duplicate and a bridge re-send stay distinguishable.
   → Dedup scope is an analytical decision, not a technical one.
5. **Where the drift reference comes from.** energy_predML pins it to the training cutoff so "monitor
   right after retraining ⇒ no drift" is true; refund-lab compares against a *trailing robust baseline*
   because there is no training event to anchor to.
6. **Schedule the stream or service the stream.** DE runs long-lived streaming as *services* and gives
   Airflow only bounded batches; refund-lab runs everything in one synchronous loop because the clock
   is simulated and must be owned by the runner.

---

## E. The five ideas that appear in all four

1. **Keep the raw thing.** Whatever arrived, unmodified, addressable, replayable.
2. **Make re-running free.** Natural keys, conflict clauses, version guards, atomic swaps.
3. **Name your clocks.** And write down which one each column is on.
4. **Instrument the pipeline in the pipeline's own store.** An audit row per run, queryable in SQL.
5. **Write down what you measured and what you got wrong**, dated, while you are doing it.
