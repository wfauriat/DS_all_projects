# Tools and concepts — a verbose index

Companion to [`07-LEARNING-GUIDE.md`](07-LEARNING-GUIDE.md). Two parts:

- **[Part A — Tools](#part-a--tools)**, grouped by what they are for. Each entry answers: *what is it?
  what problem does it solve? what does using it look like? where is it in these projects? when would
  you choose something else?*
- **[Part B — Concepts and terms](#part-b--concepts-and-terms)**, alphabetical, plain definitions.

Project shorthand: **DE** = `DE_pipeline_stream` · **ME** = `mediaElection27` ·
**EP** = `energy_predML` · **RL** = `refund-lab-autom`.

---

# Part A — Tools

## A1. Languages and file formats

### Python
**What it is.** A general-purpose programming language, and the default language of data work.
**Why it dominates here.** It is the only language with first-class libraries for every stage at once:
HTTP clients, dataframes, databases, machine learning, web APIs, dashboards. All four projects are
primarily Python.
**Worth knowing.** Modern Python (3.10+) has features these projects use heavily and older tutorials
don't mention: type hints (`list[dict]`, `str | None`), dataclasses, `match` statements, `asyncio`,
and `pathlib` instead of string paths.

### SQL
**What it is.** The language for querying and manipulating relational data. Declarative: you describe
the result you want, the database decides how to get it.
**Why it still matters.** The data is already in a database. Moving ten million rows into Python to
filter them and move them back is slower and more code.
**What is new that you may not have been taught.** Modern analytical SQL is far more powerful than the
SQL of textbooks. These projects use: **window functions** (`lag()`, `row_number() over (...)`),
`QUALIFY` (a `WHERE` that runs after window functions), `count(*) FILTER (WHERE ...)` (a conditional
count without a `CASE`), `GROUP BY ALL` (group by every non-aggregated column), `list()` aggregation,
JSON extraction (`value ->> '$.payload.trip_id'`), and `time_bucket(INTERVAL '30 minutes', ts)`.
**Where.** DE's whole transformation layer; ME's time-series endpoint; RL's hand-written queries.

### Bash / shell scripting
**What it is.** The language of the command line and of glue scripts.
**What it is good for here.** Small, explicit wrappers: creating topics, waiting for a service, driving
a restart loop.
**Two idioms worth copying.** `set -euo pipefail` makes a script stop on the first error, treat unset
variables as errors, and fail a pipeline if any stage fails — the opposite of shell defaults. But
ME's cron wrapper deliberately uses `set -uo pipefail` **without `-e`**, so that a failure is *logged*
rather than truncating the script before it can print anything. Knowing when to use each is the skill.
**Where.** DE `kafka/create-topics.sh`, `streaming/entrypoint.sh`; RL `loop.sh`; ME `scripts/cron_ingest.sh`.

### TypeScript
**What it is.** JavaScript with a type system, checked before the code runs and erased afterwards.
**What it buys you.** In a browser application talking to an API, the types document the shape of
everything and the compiler catches whole classes of mistake (a misspelled field, a value that might
be `null`).
**Its limit, and the fix.** Types are checked at compile time and gone at run time, so they cannot
verify that the API actually sent what it promised. ME solves this by validating incoming data with
Zod (below) and *deriving* the TypeScript type from the validator, so there is one definition instead
of two.
**Where.** ME `frontend/`.

### Jinja
**What it is.** A templating language: text with `{{ placeholders }}` and `{% logic %}`, rendered
before use.
**Where you meet it in data work.** dbt. Your SQL files are Jinja templates, rendered to real SQL
before execution. That is what makes `{{ ref('other_model') }}`, `{% if is_incremental() %}` and
reusable SQL macros possible.
**Mental model.** Two phases: Jinja runs first and produces SQL; then the database runs the SQL. Almost
every confusing dbt error comes from forgetting which phase you are in.

### JSON and JSONL
**What they are.** JSON is the standard text format for structured data (objects, arrays, strings,
numbers). **JSONL** ("JSON lines") is one JSON object per line — which makes a file streamable and
appendable, unlike a single big JSON array.
**Where.** Every API response here; DE's Kafka messages; RL's `incidents.jsonl` (append one line as
each detection happens — a format chosen precisely because you can append to it safely).

### YAML
**What it is.** A human-friendly configuration format: indentation instead of braces, comments allowed.
**What it is good for.** Configuration that people edit. ME's candidate and source registries are YAML
because they are *editorial* decisions — add a candidate, fix a feed URL, refine a name-matching rule —
and should not require a code change.
**Warning.** YAML has surprising rules (`no` parses as `false`, `1.0` as a number, indentation errors
are silent). For machine-generated config, JSON is safer; for human-edited config, YAML is nicer.
**Where.** ME `seeds/*.yaml`; DE's dbt configuration and `docker-compose.yml`; every GitHub Actions
workflow.

### TOML
**What it is.** A configuration format designed to be unambiguous and easy to read. Sections in square
brackets, `key = value`, real comment support, no significant whitespace.
**Why it appears.** It is the standard for Python project metadata (`pyproject.toml`), and it is a good
choice for application config that a person will read and edit.
**Where.** DE `config/source.toml` (the entire simulated world: seed, size, clock speed, fault rates —
every line commented), and every `pyproject.toml`.

### CSV
**What it is.** Comma-separated values. Ancient, universal, and full of traps: no types, no encoding
declaration, no schema, ambiguous quoting, no way to know if a file is complete.
**Why it still matters.** Because suppliers still send it. RL's entire file channel is CSV, and the
reason its reader is so defensive (§2.1 of the guide) is that CSV tells you nothing about itself.
**Where.** RL's payment and catalogue files; DE's dbt seed file.

### Parquet
**What it is.** A **columnar** binary file format for analytical data. Instead of storing row 1, row 2,
row 3, it stores all of column A, then all of column B.
**Why that matters.** Averaging one column reads only that column off disk. And because a column holds
values of one type, compression is dramatically better than CSV — often 5–10×.
**What it also carries.** The schema (column names and types) is inside the file, so unlike CSV there is
nothing to guess.
**Where.** DE's stream processor writes 30-minute aggregates as Parquet into a directory, which DuckDB
then reads **in place**, no loading step. EP saves timestamped feature snapshots as Parquet.
**Choose CSV instead when** a human must open it in a spreadsheet. Otherwise prefer Parquet.

### XML / RSS
**What they are.** XML is a verbose markup format; RSS and Atom are XML formats that news sites use to
publish their headlines.
**Where.** ME fetches about fifteen RSS feeds. It parses them with `feedparser` rather than an XML
library, because real feeds are malformed in dozens of specific ways and `feedparser` knows them all.

### Markdown
**What it is.** Plain text with light formatting, renderable as HTML. All the documentation in these
projects — including this document — is Markdown.
**Why it matters for engineering.** It lives in version control next to the code, so documentation is
reviewed, diffed and versioned along with what it describes.

### `.env` files
**What they are.** A file of `KEY=value` lines holding environment-specific settings — ports, hostnames,
feature flags, and sometimes secrets.
**The convention.** Commit a `.env.example` with every key and a safe default; never commit `.env`
itself. DE's `make setup` generates `.env` from the example, filling in your user id automatically.
**Where.** DE, ME, EP.

---

## A2. Storage and databases

### SQLite
**What it is.** A complete relational database in a library. No server, no configuration, no network —
just a file and an `import`.
**What problem it solves.** Everything you want from a database (transactions, indexes, constraints,
SQL) with zero operational burden.
**Its limit.** One writer at a time, and it is optimised for transactional access rather than large
analytical scans.
**Where.** RL uses it for the entire warehouse — hundreds of thousands of orders, over a million events
— and states the reasoning plainly: the data fits, one file, crash-safe, nothing to babysit across an
unattended multi-hour run.
**Worth knowing.** The performance settings RL uses are the standard ones for a single-writer
analytical workload: `PRAGMA journal_mode=WAL` (readers don't block the writer),
`synchronous=OFF` (faster, at the cost of durability across an OS crash — acceptable here because the
whole run is reproducible), and a large page cache.

### DuckDB
**What it is.** "SQLite for analytics": an embedded database, but **columnar** and built for scanning
and aggregating millions of rows.
**What problem it solves.** You want warehouse-style queries without running a warehouse. It reads
Parquet, CSV and JSON files directly, so it can query a directory of files as if they were tables.
**What using it looks like.**
```python
conn = duckdb.connect("warehouse.duckdb")
conn.execute("SET TimeZone = 'UTC'")
df = conn.execute("SELECT ... FROM marts.fct_trips WHERE trip_date = ?", [day]).fetchdf()
```
**The one thing to know before you rely on it.** One writer at a time. DE builds five separate
mechanisms around that single fact (§2.9 of the guide): a scheduler pool with one slot, short
connections with a retrying `connect()`, a published read-only copy, and a structure-only copy for
documentation.
**A second gotcha, learned the hard way.** By default DuckDB returns timezone-aware timestamps in the
*machine's local* timezone — so the same query gives `+01:00` on a Paris laptop and `+00:00` in a
container. Both projects that use it set `SET TimeZone = 'UTC'` immediately after connecting.
**Where.** DE (the warehouse), EP (demand and prediction storage).

### PostgreSQL
**What it is.** The default serious open-source relational database. A server process; many clients
connect concurrently.
**What problem it solves.** Concurrent readers and writers, strong constraints, mature tooling, and a
very rich type system (JSON columns with indexes, arrays, ranges, full-text search).
**Why ME uses it and not DuckDB.** ME *is* a web application: several cloud functions writing at once,
a public API reading, small indexed lookups rather than full scans. That is exactly the transactional
workload a server database is built for.
**Features ME actually uses.** `JSONB` columns (binary JSON, indexable) with a **GIN index** for
searching inside them; `ON CONFLICT` clauses for idempotent inserts; `AT TIME ZONE` for timezone-aware
day bucketing; array parameters (`= ANY(:ids)`).

### Amazon S3 (object storage)
**What it is.** Storage for files ("objects") addressed by a key, accessed over HTTP. Effectively
unlimited, cheap, and durable.
**What problem it solves.** Somewhere to put raw data that is not a database — and, crucially,
something that can *emit an event when a file appears*.
**Where.** ME stores each fetched feed at `feeds/YYYY-MM-DD/{slug}.xml`, and the creation of that
object triggers the next stage. No scheduler involved.
**Features worth knowing.** *Lifecycle rules* automatically move old objects to cheaper storage and
delete them after a period — ME uses this as a cost control. *Block public access* and *enforce SSL* are
the default-safe settings. A *gateway endpoint* lets a private network reach S3 without internet access,
and is free — which is what makes ME's no-internet architecture possible.

### Redpanda Console
**What it is.** A web interface for a Kafka cluster: browse topics, read individual messages with their
headers and keys, watch consumer groups and how far behind they are.
**Why it earns its place.** Streaming systems are invisible by default. Being able to *look at the
actual bytes* of a message is the difference between debugging and guessing.
**Where.** DE, on port 8081.

### harlequin
**What it is.** A SQL editor that runs in the terminal, for DuckDB and friends.
**Why it appears.** DE wants an interactive way to poke at the warehouse without a notebook or a GUI.
`make sql` opens it **read-only against the published copy**, which means exploring can never block the
pipeline or be blocked by it.

---

## A3. Moving data around

### HTTP and REST
**What they are.** HTTP is the protocol of the web. **REST** is a common convention for designing APIs
on top of it: a URL identifies a resource (`/v1/stations`), and the method says what to do (`GET` read,
`POST` create, `PATCH` modify).
**The parts that matter for data work.**
- **Status codes**: `200` fine · `4xx` you did something wrong (`401` not authenticated, `404` doesn't
  exist, `429` slow down) · `5xx` the server has a problem (`503` temporarily unavailable).
- **Headers**: metadata alongside the body. `Authorization` carries your token; `Retry-After` tells you
  how long to wait; `Content-Encoding` claims how the body is compressed (RL's client famously does not
  believe it); `ETag` and `If-None-Match` allow conditional requests ("only send it if it changed").
- **Pagination**: big results come in pages, either by *offset* ("rows 5000–9999") or by *cursor* (an
  opaque token you hand back for the next page).

### Server-Sent Events (SSE)
**What it is.** The simplest streaming protocol: an HTTP response that never ends, carrying messages in
a plain text format (`id:`, `event:`, `data:`, blank line to separate).
**What problem it solves.** Pushing a continuous feed to a client without WebSockets — one-directional,
works through proxies, trivially debuggable with `curl`.
**Its best feature.** Built-in resume: each message has an id, and a reconnecting client sends a
`Last-Event-ID` header to continue where it left off.
**Where.** DE's simulated source publishes over SSE; the bridge consumes it in about forty lines.
**Choose WebSockets instead when** you need the client to send messages too.

### Apache Kafka
**What it is.** A distributed, durable, replayable log. Producers append messages to **topics**;
consumers read them at their own pace, and messages stay for a configured **retention** period whether
or not anyone read them.
**What problem it solves.** Decoupling producers from consumers. Without it, every new consumer of your
event stream is a change to the producer. With it, a new consumer just starts reading — including from
the beginning.
**The vocabulary** (also in Part B): *topic* (a named stream) · *partition* (an ordered slice of a
topic; ordering is only guaranteed within one) · *key* (decides the partition, so all messages with the
same key stay in order) · *offset* (a message's position) · *consumer group* (a set of consumers
sharing the partitions) · *retention* (how long messages are kept).
**Why DE uses it as the centre of the architecture.** It is the fan-out point: the stream processor and
the warehouse loader are independent consumers with independent positions, so *"replay and backfill
come for free."*
**Operational notes from DE.** Create topics explicitly and disable auto-creation (a typo should fail,
not silently create a topic). Name topics `<domain>.<dataset>.v<major>`, and treat a breaking change as
a new topic rather than a silent edit. The two-listener configuration (one address for containers, one
for your laptop) exists because Kafka tells each client which address to use next, and no single
address works from both places.
**Choose something else when** you don't need replay and multiple independent consumers — a simple
task queue or direct HTTP calls will be far less to operate.

### Cloud event triggers (S3 events, EventBridge)
**What they are.** Cloud-native equivalents of "something happened, run this". EventBridge fires on a
schedule (a cron replacement); S3 fires when an object is created.
**What problem they solve.** Orchestration with no orchestrator to run and pay for. ME's entire
pipeline is: a schedule triggers the fetcher; the fetcher writes files; **writing the file triggers the
loader.**
**Trade-off.** Very cheap and very simple, but you lose what an orchestrator gives you: a picture of
the whole pipeline, dependency management, retries with policy, backfills, and a place to look when
something didn't run.

---

## A4. Processing and transformation

### pandas
**What it is.** The standard Python library for tabular data in memory. A `DataFrame` is a table with
named, typed columns.
**What it is for.** Everything between "I have rows" and "I have a model": feature engineering, joins,
reshaping, time-series operations.
**The operations that matter for time series**, all visible in EP:
```python
df["demand_lag_24h"]      = df["demand"].shift(24)              # the value 24 rows ago
df["demand_roll_24h_mean"]= df["demand"].shift(1).rolling(24).mean()  # shifted → no leakage
df["hour"]                = df["timestamp"].dt.hour             # .dt accessor for datetime columns
merged = preds.merge(actuals, on=["timestamp","region"], how="inner")
```
**The trap.** `shift()` and `rolling()` depend entirely on row order. Sort first, always, and say so in
a comment.
**Where.** EP throughout; RL for the model's feature work.
**Use something else when** the data does not fit in memory (use the database, or Spark).

### NumPy
**What it is.** The array library everything else is built on. Fast vectorised arithmetic over whole
arrays instead of Python loops.
**Where it shows up interestingly.** DE's simulator uses it to generate a whole day of demand across 40
stations in one operation, and uses **seeded random number generators** so the same seed always produces
the same world:
```python
rng = np.random.default_rng([seed, 1])   # a separate, reproducible stream
counts = rng.poisson(rate_per_second * step_seconds)
```
Three separate generators are derived from one seed — one for building the world, one for the engine,
one for the injected faults — so **turning faults on or off cannot change the underlying world**. That
is a neat trick worth remembering for any simulation or test-data generator.

### Apache Spark / PySpark / Structured Streaming
**What it is.** A distributed data-processing engine. **PySpark** is its Python interface. **Structured
Streaming** is its streaming mode, and its central idea is that *a stream is an unbounded table* — so
you write the same DataFrame code for both.
**What problem it solves.** Computations too big for one machine, and continuous computations over
streams with proper handling of windows, late data and state.
**What using it looks like — and why this is the important part:**
```python
def over_capacity(events, stations):          # a plain function: DataFrame in, DataFrame out
    status = events.where(F.col("event_type") == "station_status")
    joined = status.join(F.broadcast(stations.select("station_id", "capacity")), "station_id")
    return joined.where(F.col("bikes_available") > F.col("capacity"))
```
That function runs unchanged on a live stream in production *and* on a five-row static DataFrame in a
unit test. DE's whole rules module is written this way, and it is the single highest-leverage
structural decision in that layer.
**Concepts you must meet.** Watermark, window, stateful vs stateless, checkpoint, `foreachBatch` (run
arbitrary code once per micro-batch, on the driver). All in Part B.
**Honest note.** Spark is heavy — a JVM, roughly 1.5 GB of memory in DE's configuration, and real
operational complexity. DE uses it because demonstrating a modern streaming stack is the point. For
data that fits on one machine, DuckDB or pandas will be faster and far simpler.

### dbt (data build tool)
**What it is.** In one line: **SQL files plus a build system.** You write `SELECT` statements; dbt
turns each into a table or a view, in the right order, with tests and documentation.
**What problem it solves.** Before dbt, SQL transformations were either stored procedures (invisible to
version control) or a pile of scripts with hand-maintained execution order. dbt makes transformations
*software*: files in git, dependencies inferred, tests that run with the build, generated docs.
**What using it looks like.**
```sql
-- models/marts/fct_trips.sql
{{ config(materialized='incremental', unique_key='trip_id', incremental_strategy='delete+insert') }}
select trip_id, started_at, distance_km, speed_kmh, loaded_at
from {{ ref('int_trips') }}
where pairing = 'complete'
{% if is_incremental() %}
  and loaded_at > (select max(loaded_at) from {{ this }})
{% endif %}
```
**What it gives you, concretely:**
- **`ref()` builds a dependency graph.** dbt knows the build order and can draw you a lineage diagram —
  the answer to "if I change this, what breaks?"
- **Materialisation is configuration.** The same query can be a view (always current, free to store) or
  a table (built once, fast to query) or incremental — without rewriting it.
- **Tests run with the build.** A dbt test is a query that should return zero rows; if it returns rows,
  *those rows are the failure* and you can look at them. Built-in generics (`unique`, `not_null`,
  `accepted_values`, `relationships`), your own generics, one-off "singular" tests, **unit tests** with
  hand-written input and expected output, and **contracts** that fail the build if a table's columns
  change.
- **Snapshots** implement SCD2 history for you.
- **Seeds** load small CSVs from version control as tables — ideal for mappings that are editorial
  decisions.
- **Macros** are reusable SQL written in Jinja.
- **Source freshness** checks how long ago the newest row landed.
**Where.** DE `transform/`. The one project that uses it, and it uses essentially every feature.
**Choose something else when** your transformations are not SQL, or you have three models and no team.

---

## A5. Orchestration and scheduling

### cron
**What it is.** The Unix scheduler. A line per job: when to run it and what to run.
**What it does not do.** Dependencies, retries, backfills, visibility, or any notion of whether the
previous run succeeded.
**When it is genuinely the right answer.** One job, no dependencies, and you would rather have five
lines of configuration than a scheduler to operate. ME ran on cron before moving to the cloud, and kept
the scripts in the repository.
**How to use it properly.** Never put the command directly in the crontab — cron's environment is
minimal (almost no `PATH`, no project context). Write a wrapper script that sets the environment,
changes to the right directory, waits for dependencies to be ready, captures the exit code and prints
scannable start/end markers. ME's `scripts/cron_ingest.sh` is a good template.

### Apache Airflow
**What it is.** The most widely used data orchestrator: you define pipelines in Python, it schedules
them, runs them, retries them, and shows you what happened in a web interface.
**Vocabulary.** A **DAG** is one pipeline (a directed acyclic graph of tasks) and also the unit of
scheduling. A **task** is one step; a **DAG run** is one execution for one scheduled interval. An
**executor** decides where tasks actually run.
**What using it looks like** (the modern "TaskFlow" style):
```python
@dag(schedule=timedelta(minutes=5), catchup=False, max_active_runs=1,
     default_args={"retries": 2, "retry_delay": timedelta(seconds=30)})
def stream_landing():
    @task(pool="duckdb")
    def prepare_warehouse(): ...
    @task(outlets=[RAW_STREAM])
    def report(summaries: list[dict]): ...
    prepare_warehouse() >> landed
    report(landed)
```
**Three features worth knowing even if you use something else.**
- **Pools** are named semaphores for tasks. DE creates a pool with **one slot** so that only one task
  can write to DuckDB at a time — a concurrency primitive expressed as configuration.
- **Assets / data-aware scheduling.** A DAG can be scheduled *by data* rather than by a clock: one DAG
  declares it produces a dataset, another declares it consumes it. No guessing at timings.
- **Connections and variables** hold addresses and credentials. DE sets them entirely from environment
  variables so that nothing lives only in a UI.
**The rule that matters most.** Keep your business logic in plain modules with **no Airflow import**,
and let the DAG only schedule them. DE does this rigorously, which is why its loading code can be
unit-tested and reused by command-line scripts.
**Honest note.** Airflow is a real system: a database, a scheduler, a web server, a file parser. For one
job on one machine, it is far too much.

### Prefect
**What it is.** A lighter-weight workflow library. Decorate functions with `@task` and `@flow` and get
retries, caching and observability, without necessarily running a server.
**What using it looks like.**
```python
@task(retries=3, retry_delay_seconds=60, cache_key_fn=task_input_hash)
def fetch_data(region, start, end): return fetch_demand(region=region, start=start, end=end)

@flow(name="data-ingestion")
def ingest_flow(region=None, start=None, end=None):
    records = fetch_data(region, start, end)
    return store_data(validate_data(records))
```
**Where.** EP — though note the honest caveat in that project's own documentation: the decorators are
used, but **nothing is actually scheduling the flows.** It is a pipeline that *can* run, not one that
*does*. Recognising that distinction in your own projects is worth more than the tool.

### AWS Lambda (serverless functions)
**What it is.** You upload a function; the cloud runs it when something triggers it, and you pay per
execution rather than per hour.
**What problem it solves.** Work that happens occasionally. ME's ingestion runs four times a day for a
few seconds — a server would be idle 99.9% of the time.
**What you must know.** A **cold start** is the delay when a function runs after being idle: the
runtime is created and your module is imported. That import time is real (ME needed to raise a
client-side timeout to 300 seconds for the first call after a deploy), and it is why ME does its
database-address setup at import time rather than per request.
**The pattern worth stealing.** ME's loader function **dispatches on the shape of its input**: an S3
event means "load these files", while `{"action": "migrate"}` runs database migrations and
`{"action": "seed"}` reloads configuration. One function, one set of permissions, one network
configuration, one log group — instead of four of each.

---

## A6. Machine learning

### scikit-learn
**What it is.** The standard Python machine-learning library. Its lasting contribution is less the
algorithms than the **interface**: everything has `.fit(X, y)` and `.predict(X)`, so models are
interchangeable.
**What these projects use it for.** Metrics (`mean_squared_error`, `roc_auc_score`), the
time-series-aware cross-validation splitter `TimeSeriesSplit`, and `HistGradientBoostingClassifier`
(RL's model — a fast gradient-boosting implementation built in, which also handles missing values and
categorical features natively).

### XGBoost
**What it is.** A gradient-boosting library: it builds many small decision trees, each correcting the
previous ones' errors.
**Why it is the default for tabular data.** On tables — as opposed to images or text — boosted trees
routinely beat neural networks, train in seconds to minutes, handle missing values, need no feature
scaling, and tell you which features mattered.
**What using it looks like.**
```python
model = xgb.XGBRegressor(n_estimators=500, max_depth=6, learning_rate=0.05,
                         objective="reg:squarederror", early_stopping_rounds=50)
model.fit(X_train, y_train, eval_set=[(X_val, y_val)], verbose=False)
```
**`early_stopping_rounds` explained.** Watch performance on the validation set while training; when it
stops improving for this many rounds, stop. This is the main defence against overfitting in boosting,
and it costs one line.
**Where.** EP. Result: RMSE 1927 (naive baseline) → 564 (default settings) → 501 (tuned).

### Optuna
**What it is.** A hyperparameter search library. You write a function that trains a model with a
suggested set of parameters and returns a score; Optuna suggests parameters, learns from the results,
and searches the promising regions.
**Why not a grid search.** A grid over eight parameters is combinatorially hopeless. Optuna's
Bayesian-style search finds good configurations in tens of trials rather than thousands.
**What using it looks like.**
```python
def objective(trial):
    params = {"max_depth": trial.suggest_int("max_depth", 3, 10),
              "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.3, log=True), ...}
    return mean_rmse_across_time_series_folds(params)

study = optuna.create_study(direction="minimize")
study.optimize(objective, n_trials=50)
```
**The detail that matters here.** The objective averages error across **`TimeSeriesSplit`** folds, not
random folds. Tuning against a shuffled split would tune for a problem you do not have.
**Perspective, from EP's own numbers.** Tuning improved RMSE by 11%. Having any model at all improved
it by 71%. Spend your effort accordingly.

### MLflow
**What it is.** Two things in one tool. **Experiment tracking**: every training run records its
parameters, metrics and artefacts, so you can compare runs later. **A model registry**: named,
versioned models you can load by name.
**What problem it solves.** The universal failure mode of `model_final_v2_REAL.pkl` and a metric written
on a sticky note. Tracking makes "which settings produced that score?" answerable months later.
**What using it looks like.**
```python
with mlflow.start_run(run_name="xgboost_tuned"):
    mlflow.log_params(best_params)
    mlflow.log_metrics({"rmse": rmse, "mae": mae, "r2": r2})
    mlflow.xgboost.log_model(model, artifact_path="model",
                             registered_model_name="energy_demand_xgboost")
```
**The registry as a deployment mechanism.** EP's API, at startup, resolves the highest registered
version and loads it — so retraining *is* deploying. It also fetches that run's logged `rmse` and uses
it to size the prediction interval, so **the model and its uncertainty travel together.**
**A practical warning, from EP's design note.** The file-based storage backend records the *absolute
path* where a model was saved, which breaks the moment a second machine is involved. Use a
database-backed store with `--serve-artifacts`, so that file transfers are proxied through the server
and no host path ever appears in the metadata.

### Evidently AI
**What it is.** A library for data and model monitoring: given a "reference" dataset and a "current"
one, it compares them and reports which features have drifted, with statistical tests and an HTML
report.
**What problem it solves.** Writing drift detection by hand means choosing a test per feature type,
handling small samples, and building a report. Evidently does that.
**What it does not solve.** *What* to compare. EP's most important monitoring decision is not the
library but the definition: reference = the data the model was trained on (determined by a training
cutoff written to a file), current = everything that arrived since. Get that wrong and the tool
faithfully reports nonsense.
**Related caution.** RL computed a population-stability statistic over the *union* of categories, and a
newly added category produced a huge score through a near-zero denominator. Over the shared categories
the distributions were identical. **Distribution comparisons must be computed over the common support.**

---

## A7. Serving and interfaces

### FastAPI
**What it is.** A modern Python web framework. You write typed function signatures; it handles
validation, serialisation, errors and interactive documentation.
**What problem it solves.** Removing the boilerplate between an HTTP request and your function — and
making the *contract* explicit, because the types in the signature become the validation rules and the
published API schema.
**What using it looks like.**
```python
@app.post("/predict", response_model=PredictResponse)
async def predict(request: PredictRequest):
    if _model is None:
        raise HTTPException(status_code=503, detail="Model not loaded")
    ...
```
`PredictRequest` is a Pydantic model; if a caller sends a malformed body, FastAPI rejects it with a
detailed error before your code runs. It also generates a browsable documentation page at `/docs`.
**Patterns from these projects.** A **lifespan** hook for startup work (load the model; start a
background task). One injected dependency for the database session
(`SessionDep = Annotated[AsyncSession, Depends(get_session)]`). Deliberate **error mapping** (503 vs
400 vs 500) as part of the contract.
**Where.** DE's simulated source, EP's prediction API, ME's public API.

### uvicorn
**What it is.** The server that actually runs a FastAPI application — accepts connections, speaks HTTP,
calls your code.
**Worth knowing.** DE subclasses it to add one step on shutdown: closing the open streaming responses.
Without that, a never-ending SSE response keeps the server from shutting down cleanly, and every
restart logs an error. A small reminder that graceful shutdown is a feature you sometimes have to write.

### Mangum
**What it is.** An adapter that lets an ordinary Python web application run as a cloud function,
translating the cloud's event format into what the framework expects.
**Why it matters.** ME runs *the same FastAPI application* locally with uvicorn and in production on
Lambda. One line: `handler = Mangum(app, lifespan="off")`. No separate serverless codebase.

### Pydantic
**What it is.** Data validation driven by Python type hints. Declare a class with typed fields; it
parses, validates and converts input, and gives precise errors when it cannot.
**What problem it solves.** The boundary between your program and the outside world. Everything past
that boundary can then be trusted.
**What using it looks like.**
```python
class StationStatus(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    station_id: str
    bikes_available: int = Field(ge=0)
    is_renting: bool
```
**The two settings that turn it into a contract enforcer.** `extra="forbid"` makes an unexpected field
an error (so a *renamed* field is caught). `strict=True` disables helpful conversions, so `"754s"` is
not quietly accepted where an integer was promised.
**Also.** `pydantic-settings` loads configuration from environment variables and `.env` files with the
same validation — so a bad configuration value fails at startup with a clear message.
**Where.** All three Python API projects, and DE's fault-injection catalogue.

### SQLAlchemy
**What it is.** Python's database toolkit. Two layers: a *Core* for building SQL programmatically, and
an *ORM* (object-relational mapper) that maps table rows to Python objects.
**What problem it solves.** Portable, composable, injection-safe queries; and schema defined once in
Python, from which migrations can be generated.
**What using it looks like.**
```python
class Article(Base):
    __tablename__ = "articles"
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    published_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    raw: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    __table_args__ = (UniqueConstraint("source_id", "guid", name="uq_articles_source_guid"),)
```
**Worth knowing.** ME creates **two engines**: an asynchronous one for the API and a synchronous one for
scripts and migrations, because the async machinery is unnecessary complexity in a batch script.
**A note on the ORM debate.** ME uses the ORM for straightforward reads and writes, but drops to raw
SQL for the one genuinely analytical query. That is the right instinct: an ORM is excellent for
row-at-a-time work and a hindrance for aggregate queries.

### Alembic
**What it is.** Database migrations for SQLAlchemy. Each migration is a file with `upgrade()` and
`downgrade()` functions; Alembic tracks which have been applied.
**What problem it solves.** Changing a schema that already has data in it, reproducibly, across
environments, in version control.
**The valuable habit it enforces.** A migration is a place to write *why*. ME's migration that removes
a uniqueness constraint has a docstring explaining that the constraint collapsed the same news-agency
story republished by several outlets into one row, and that each outlet's pick-up is a distinct data
point. That reasoning would otherwise be lost.

### Streamlit
**What it is.** A library that turns a Python script into a web application. No HTML, no JavaScript: you
write top-to-bottom Python and widgets appear.
**What problem it solves.** Internal dashboards and demos in an hour instead of a week.
**How it works (the part that confuses people).** The entire script re-runs on every interaction. That
is why caching matters so much.
**The two decorators that matter.**
```python
@st.cache_data                      # cache a computation's result
def load(path: str, mtime: float) -> dict[str, pd.DataFrame]: ...
```
Note what DE passes as the cache key: the file's **modification time**. So the cache is keyed on the
*version of the data*, not on a timer — a reload costs nothing until the pipeline publishes something
new. `@st.cache_resource` is the variant for things you load once and reuse, like a model.
**Where.** DE's four-tab dashboard, EP's monitoring dashboard.
**Choose a real frontend instead when** the audience is the public and the interactions are rich —
which is exactly why ME uses React.

### Altair / Vega-Lite
**What it is.** A *declarative* charting library: you describe what the data means (this field is the
x position, this one is the colour) and it decides how to draw it.
**Why declarative helps.** Composition. DE builds a hover crosshair by *layering* four charts — a line,
a vertical rule filtered to the nearest point, a highlighted point, and an invisible wide mark that
captures the pointer — rather than writing event handlers.
**Where.** DE's dashboard.

### matplotlib
**What it is.** The original Python plotting library. Imperative, endlessly configurable, occasionally
awkward, and present everywhere.
**When to use it.** Quick diagnostic plots, and anything that needs to be a static image file.
**Where.** EP's dashboard and comparison script.

### React, Vite, TanStack Query, ECharts, Tailwind, Zod, react-router
ME's frontend stack. Briefly, because each is a large topic:

- **React** — a library for building user interfaces from composable components whose output is a
  function of their state.
- **Vite** — the build tool: instant reloads during development, an optimised bundle for production.
  It also *bakes environment variables in at build time*, which is why ME's deployment must deploy the
  API first and then build the frontend with its address.
- **TanStack Query** — manages server data: caching, deduplicating identical requests, retries, loading
  and error states. ME configures `staleTime: 60_000` — do not refetch within a minute, since news feeds
  do not update faster than that.
- **ECharts** — a mature charting library; ME builds its option object inside a memo so it is only
  recomputed when the data changes.
- **Tailwind** — styling by composing small utility classes in the markup, rather than writing CSS files.
- **Zod** — schema validation for TypeScript. Crucially, the TypeScript type is *derived* from the
  validator (`z.infer<typeof Schema>`), so there is one definition. This is how ME gets **runtime**
  validation of API responses, which TypeScript alone cannot provide.
- **react-router** — maps URLs to components. ME keeps all its filter state in the URL, so refreshing
  or sharing a link reproduces the view exactly.

---

## A8. Development tooling

### Git
**What it is.** Version control: a complete history of every change, with branching and merging.
**Why it is mentioned here at all.** Because in these projects *everything* is in it — not only code
but schema definitions (via migrations), infrastructure (via CDK), transformations (via dbt), editorial
configuration (via YAML), and the documentation. "It is in version control" is the property that makes
all the other practices reviewable and reversible.

### Make
**What it is.** An old build tool, used in all these projects for something simpler: as the **single
list of things a human can do**.
**What problem it solves.** The gap between a README that says "run this long command" and actually
running it. `make dashboard` is discoverable, tab-completes, and cannot be mistyped.
**The idiom worth copying everywhere.** A self-documenting help target:
```makefile
help:  ## list targets
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / \
	  {printf "  \033[36m%-13s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

dashboard: ## Streamlit dashboard on the serving copy: http://localhost:8501
	$(UV) run streamlit run dashboard/app.py --server.port 8501
```
Now `make help` prints every target with its description, generated from the file itself, so it cannot
drift out of date.
**Two non-obvious tricks from DE.** `-include .env` followed by `export` means make and Docker Compose
read the same settings. And `export BUILDX_NO_DEFAULT_ATTESTATIONS := 1` stops Docker from attaching a
timestamped attestation to every image — which otherwise changes the image identifier on every build and
needlessly restarts every container.

### Docker and Docker Compose
**What they are.** **Docker** packages an application with its entire environment into an *image*, run
as a *container*. **Compose** describes several containers and their relationships in one YAML file.
**What problem they solve.** "Works on my machine", and running seven interdependent services with one
command.
**The vocabulary.** *Image* (frozen filesystem + default command, built from a `Dockerfile`) ·
*container* (a running instance; disposable) · *volume* / *bind mount* (storage that outlives the
container) · *port mapping* (expose a container port on your machine) · *layer* (each `Dockerfile`
instruction creates one, and they are cached — which is why you copy dependency files and install
*before* copying your code).
**Compose features worth knowing.** **Profiles** mark optional services, so `COMPOSE_PROFILES=` runs
only the core stack (on DE, roughly 1.5 GB less memory). **Health checks** plus
`depends_on: condition: service_healthy` give ordered startup. `user: "${HOST_UID}:${HOST_GID}"` makes
files written into mounted directories belong to you rather than root. **YAML anchors**
(`x-airflow-common: &airflow-common`, then `<<: *airflow-common`) declare a shared block once for
several services.

### uv
**What it is.** A fast Python package manager and project tool: creates environments, resolves
dependencies, writes a lockfile, and runs commands inside the environment.
**What problem it solves.** Reproducibility and speed. `uv.lock` pins the exact version of every package
and every transitive dependency, so `uv sync` produces an identical environment anywhere.
**The workspace feature, and why it matters.** DE has several packages that share dependencies and are
deployed separately. A **workspace** is one project with several members and **one shared lockfile**.
Each container then installs only its own member — `uv sync --frozen --package bikeshare-sim` — so the
container runs *exactly* the versions the tests ran against.
**Where.** DE. (ME and EP use plain `pip` with a virtual environment, which is fine and simpler.)

### pytest
**What it is.** Python's testing framework. Test functions are plain functions whose names start with
`test_`; assertions are plain `assert` statements.
**The concept that repays learning: fixtures.** A fixture is a named piece of setup that tests request
by naming it as a parameter.
```python
@pytest.fixture
def tmp_duckdb(tmp_path, monkeypatch):
    db = tmp_path / "test.duckdb"
    monkeypatch.setattr("src.data.store.DB_PATH", db)
    return db

def test_something(tmp_duckdb, seed_demand):   # requesting them runs them
    ...
```
`tmp_path` (a fresh temporary directory per test) and `monkeypatch` (change something and have it
restored automatically) are built in. The four lines above make it *impossible* for a test to touch
the real database.
**Other pieces in use here.** `pytest.mark.skipif` to skip a whole file when a service is unreachable;
`unittest.mock.patch` for replacing components; `--import-mode=importlib` so several packages can each
have a `test_api.py` without colliding.

### ruff
**What it is.** A very fast linter and formatter for Python, replacing several older tools.
**What a linter does.** Reads your code without running it and flags style problems, likely bugs,
unused imports, and so on. A formatter rewrites the code to a consistent style so nobody argues about
it in review.
**Where.** DE, ME.

### mypy
**What it is.** A static type checker for Python. Verifies that the type hints you wrote are consistent.
**Why `--strict`.** In strict mode mypy requires you to annotate everything and refuses to guess. ME
runs `mypy app` in strict mode in its CI pipeline, which catches a specific and valuable class of bug:
"this can be `None` and you did not check."
**Where.** ME.

### httpx and requests
**What they are.** Python HTTP clients. `requests` is the long-standing synchronous one; `httpx` has a
very similar API and also supports async.
**Why the distinction matters here.** ME fetches fifteen feeds concurrently, which needs async — so
httpx. RL makes sequential calls in a loop — so `requests` is fine and simpler.
**The feature worth knowing.** httpx supports a pluggable *transport*, which means you can substitute a
fake one in tests and serve scripted responses without a network. Both DE and ME test their HTTP code
this way.

### feedparser
**What it is.** A library for parsing RSS and Atom feeds.
**Why not just use an XML parser.** Because real feeds in the wild are malformed in dozens of specific
ways, use a dozen different date formats, and disagree about field names. `feedparser` normalises all
of that. ME still has to add its own date handling on top (§2.2 of the guide), which tells you how bad
the raw situation is.

### structlog
**What it is.** A library for structured logging — log entries as key-value data rather than
pre-formatted sentences.
**Why it matters.** `{"msg":"throughput","received":450,"per_s":15.0}` is queryable; "processed 450
events (15/s)" is not. DE writes a six-line equivalent by hand:
```python
def event(logger, msg, level=logging.INFO, **fields):
    logger.log(level, msg, extra={"fields": fields})
```
plus a formatter that emits JSON. Worth doing on day one of any project.

### confluent-kafka, pyarrow, boto3, PyYAML
Small but load-bearing:
- **confluent-kafka** — the Python client for Kafka, wrapping the C library. Its asynchronous delivery
  model is the thing to understand: `produce()` only *queues* a message; the acknowledgement arrives
  later through a callback served by `poll()` or `flush()`. This is exactly why DE only advances its
  checkpoint after `flush()` returns successfully.
- **pyarrow** — an in-memory columnar format. Used here to hand a batch of rows to DuckDB in one bulk
  operation instead of row-by-row inserts.
- **boto3** — the AWS SDK for Python.
- **PyYAML** — reads YAML. Always `yaml.safe_load`, never `yaml.load`.

### GitHub Actions
**What it is.** CI/CD built into GitHub: YAML workflow files describing jobs that run on pushes, pull
requests or schedules.
**The two workflows in ME, and why they are split.**
- `ci.yml` runs on *every branch*: lint, type-check, **unit tests only** (they must not need a database).
- `deploy.yml` runs on pushes to the main branch, with documentation-only changes excluded, and
  deploys in two phases because the frontend needs the API's address.
**The feature most worth knowing: OIDC federation.** Instead of storing cloud credentials as secrets,
the workflow requests a short-lived identity token, and the cloud account is configured to trust tokens
that say "this repository, this branch". No long-lived keys exist to leak.

### AWS CDK
**What it is.** Infrastructure as code where you write ordinary Python (or TypeScript) and it generates
the cloud provider's native template.
**Why not write the template directly.** Because the templates are enormous and repetitive, and because
a real language gives you loops, functions, and the ability to pass one resource to another as an
object.
**What using it looks like.**
```python
self.vpc = ec2.Vpc(self, "Vpc", max_azs=2, nat_gateways=0,
                   subnet_configuration=[ec2.SubnetConfiguration(
                       name="isolated", subnet_type=ec2.SubnetType.PRIVATE_ISOLATED)])
self.vpc.add_gateway_endpoint("S3Endpoint", service=ec2.GatewayVpcEndpointAwsService.S3)
```
**The mental model.** Three artefacts: your Python → a generated template → real resources. Generating
is free and local; deploying computes a difference against what exists.
**The trap.** Renaming a construct changes its logical identifier, so the cloud believes the old
resource is gone and **destroys and recreates it**.
**Where.** ME, seven separate stacks so each can be deployed independently.

### AWS building blocks used by ME
- **Lambda** — run a function on demand; pay per execution.
- **API Gateway** — the public HTTPS front door that routes requests to a function.
- **CloudFront** — a content delivery network: caches your site near users. Also used here to serve a
  single-page application's deep links by returning `index.html` for unknown paths.
- **RDS** — managed PostgreSQL; the cloud runs the database server.
- **S3** — object storage (above).
- **Secrets Manager** — stores credentials. Interesting here because ME *cannot reach it at run time*
  from its isolated network, which forced the deploy-time credential injection described in §3.5 of the
  guide.
- **VPC** — a private network. A **subnet** is a slice of it; `PRIVATE_ISOLATED` means no route to the
  internet at all. A **security group** is a firewall rule set. A **NAT gateway** would let private
  resources reach the internet and costs about $32/month — the single decision the whole architecture
  turns on.
- **EventBridge** — scheduled and event-driven triggers.
- **CloudWatch / SNS / Budgets** — metrics and alarms; a notification fan-out; spending limits with
  email alerts at 80% and 100%.
- **IAM** — permissions. The relevant idea here is *least privilege*: the fetching function can write
  to one bucket and do nothing else.
- **CloudFormation** — the underlying deployment engine that reads the generated template, tracks what
  exists, and applies differences.

---

# Part B — Concepts and terms

**ACID** — the four guarantees a transactional database gives: *Atomicity* (all or nothing),
*Consistency* (constraints hold), *Isolation* (concurrent transactions don't see each other's
half-finished work), *Durability* (committed means committed). The one you will use most deliberately
is atomicity.

**Anchor (YAML)** — a way to define a block once (`&name`) and merge it elsewhere (`<<: *name`). DE uses
it so four Airflow services share one definition of image, environment and mounts.

**As-of join** — joining to the version of a record that was valid at a given time, rather than to the
current version. Written as a join with a time-range condition against a history table. *"Was this
reading impossible **at the time**?"* See §1.6 of the guide.

**At-least-once / at-most-once / exactly-once** — the three delivery guarantees. At-least-once may
duplicate, at-most-once may lose, exactly-once does neither but requires machinery. Most real systems
are at-least-once plus deduplication.

**Atomic** — happens completely or not at all, with no observable in-between state. `os.replace` on a
file is atomic; a database transaction is atomic.

**Audit table** — a table recording what the pipeline did (which batch, from where to where, how many
rows, how long), stored next to the data so both can be queried together.

**Backfill** — deliberately running a pipeline over past periods, to fill a gap or apply a fix to old
data.

**Backpressure** — when a slow consumer causes an upstream producer to slow down rather than buffer
indefinitely. DE gets it for free: when the message queue is slow, the producer's local buffer fills,
the send call blocks, and the pressure propagates all the way back to the source read.

**Baseline** — the simplest model you would use if you had no model. Its purpose is to make your real
model's score meaningful. EP's is one column: "demand will be what it was at this hour yesterday".

**Batch (processing)** — processing a bounded chunk of data with a beginning and an end, as opposed to
a stream.

**Bitemporal** — storing two time dimensions: when a fact was true (*valid time*) and when you knew it
(*knowledge time*). Lets you reconstruct what you believed at any past moment. See §1.6.

**Bootstrap** — the initial full load, as opposed to the incremental updates that follow.

**Censoring** — an outcome that has not been determined *yet*, as opposed to being negative. An order
placed yesterday is not "not refunded"; it is "undetermined". Treating one as the other is a classic
and serious modelling error. See §2.8.

**Checkpoint** — a saved record of how far a process has got, so it can resume after a restart.

**Cold start** — the delay when a serverless function runs after being idle and its runtime must be
created and your code imported.

**Columnar storage** — storing all values of one column together rather than all values of one row.
Much faster for analytics, much better compressed. Parquet and DuckDB are columnar.

**Consumer group** — in Kafka, a set of consumers that share the partitions of a topic between them, so
each message is handled once by the group.

**Contract (data)** — a schema plus a promise: an agreement between a producer and a consumer about the
shape of data, where breaking it is the producer's fault. Distinct from a schema, which is just a
description.

**Cursor** — an opaque token from an API meaning "continue from here". Also, in a database, a handle to
a result set you read progressively.

**DAG** — directed acyclic graph. Tasks with dependencies and no cycles. The standard way to describe a
pipeline.

**Data lake** — files in structured directories, in an open format, queryable where they lie. Not a
product; a pattern.

**Data warehouse** — a database organised for analysis: modelled tables, historical, optimised for
scanning and aggregating.

**Lakehouse** — a recent portmanteau: warehouse-style tables and transactions over lake-style files.

**Dead-letter queue (DLQ)** — a separate destination for messages that could not be processed, carrying
the error *and the original message*, so they can be inspected and replayed after a fix.

**Deduplication** — removing repeated records. Requires deciding *what counts as the same record*,
which is a business decision, not a technical one (see §2.3's note about news-agency stories).

**Dimension (table)** — a table describing entities (customers, stations, products). Changes slowly,
comparatively small. See also *fact table*, *star schema*.

**Drift** — the data or the world changing after a model was trained. *Covariate drift*: the inputs
moved. *Label drift*: the outcomes moved. *Concept drift*: the relationship between them changed — the
only one that truly matters, and the only one you cannot see without outcomes.

**ELT / ETL** — Extract-Load-Transform versus Extract-Transform-Load. ELT stores raw data first and
transforms inside the database; ETL transforms before storing. ELT is the modern default because it
keeps the original.

**Fact (table)** — a table of things that happened: one row per event or transaction. Long, append-only,
grows forever.

**Feature** — an input to a model, derived from raw data. *Feature engineering* is the work of creating
them, and in tabular problems it usually matters more than the choice of algorithm.

**Fixture (testing)** — reusable setup that a test requests by name.

**Freshness** — how old the newest data is. The single cheapest high-value data quality check.

**Idempotency** — doing it twice has the same effect as doing it once. See §1.4.

**Incremental model** — a table that, after the first build, only processes new or changed rows.

**Jitter** — deliberate randomness added to retry delays, so that many clients retrying after an outage
do not all hit the server at the same instant.

**Knowledge time** — when you (or the source) knew something. Contrast *valid time*.

**Lag feature** — a past value of a series used as an input: "demand 24 hours ago".

**Landing zone / raw layer** — where data is first written, unmodified.

**Late-arriving data** — records that arrive after the period they belong to has been processed. The
reason incremental loads must filter on landing time rather than event time.

**Leakage** — using information at training time that will not be available at prediction time. The
commonest cause of a model that looks excellent and performs badly.

**Lineage** — the dependency graph from raw sources to final outputs. Answers "if I change this, what
breaks?" and "where did this number come from?"

**Lock** — a mechanism preventing simultaneous access. DuckDB's single-writer lock drives a large part
of DE's architecture.

**Macro** — reusable templated SQL in dbt.

**Materialisation** — in dbt, how a model is physically built: a view (recomputed on query), a table
(built once per run), or incremental.

**Micro-batch** — how Spark's Structured Streaming actually works: a "stream" is processed as a rapid
series of small batches.

**Migration** — a versioned, scripted change to a database schema.

**Mock / fake / stub** — stand-ins for real components in tests. A *mock* records how it was called; a
*fake* is a simplified working implementation (DE's fake Kafka producer); a *stub* returns canned
answers. What matters more than the vocabulary is *choosing the boundary* deliberately.

**Mojibake** — text corrupted by being decoded with the wrong character set: `Gijón` → `GijÃ³n`.

**Natural key** — the combination of columns that identifies a record in the real world, as opposed to
an auto-generated id.

**Offset** — a message's position in a Kafka partition.

**OLTP / OLAP** — Online Transaction Processing (many small reads and writes; a web application) versus
Online Analytical Processing (few large scans; a report). Different storage layouts suit each.

**Orchestration** — deciding what runs, when, in what order, with what retries, and making it visible.

**ORM** — Object-Relational Mapper: maps table rows to objects in your language.

**Pagination** — returning a large result in pages. Offset-based or cursor-based.

**Partition** — (1) a slice of a Kafka topic; (2) a slice of a table or directory by column value,
usually a date, enabling pruning and targeted rebuilds.

**Point-in-time correctness** — computing features using only information that was actually available at
the moment the decision was made.

**Pool** — in Airflow, a named limit on how many tasks may run concurrently. A semaphore expressed as
configuration.

**Precision / recall** — of the things you flagged, what fraction were real (precision); of the real
things, what fraction did you flag (recall). They trade off.

**Primary key** — the column(s) uniquely identifying a row; the database enforces uniqueness.

**PSI (population stability index)** — a number summarising how much a distribution has moved between
two periods. Must be computed over the categories present in *both*, or a newly-appeared category
dominates it.

**Rate limit** — a cap on how many requests you may make. Signalled by HTTP `429`, often with a
`Retry-After` header.

**Reconciliation** — comparing your counts with the source's to detect divergence. The only way to
detect rows silently *disappearing*.

**Replay** — reprocessing historical data from a durable source, typically after fixing a bug.

**Retention** — how long a messaging system keeps messages regardless of who has read them. What makes
replay possible.

**Retry with exponential backoff** — retry after 1s, 2s, 4s, 8s…, with jitter, up to a limit.

**RMSE / MAE / R² / MAPE** — regression metrics. *RMSE* (root mean squared error): typical error size in
the target's units, penalising large errors heavily. *MAE*: average absolute error, less sensitive to
outliers. *R²*: share of variation explained (1.0 perfect, 0 no better than the mean). *MAPE*: average
error as a percentage — intuitive, but undefined when the true value is zero.

**Schema** — a description of data's shape: fields, types, which are required.

**Schema drift** — the shape changing without warning.

**SCD2 (slowly changing dimension, type 2)** — keeping one row per *version* of an entity, each with a
validity window, so past states can be joined against. See §1.6.

**Seed (dbt)** — a small CSV in version control loaded as a table.

**Seed (random)** — a number initialising a random generator, making "random" output reproducible.

**Semaphore** — a limit on how many things may happen at once.

**Sentinel value** — a magic value standing in for "missing": `-999`, `1900-01-01`, `N/A`. A data
fault; distinct from an honest null.

**Serving copy** — a read-only copy of the data, published atomically for readers, so they never
contend with the writer.

**Smoke test** — a quick end-to-end check that the system as deployed is actually working.

**Snapshot** — (1) in dbt, SCD2 history of a table that gets overwritten; (2) more generally, a complete
picture of something at a moment in time.

**Star schema** — fact tables in the middle, dimension tables around them.

**Stateful / stateless** — whether an operation needs to remember things between records. Stateless
operations are cheap and restartable; stateful ones need durable storage for their state.

**Streaming** — processing an unbounded, continuously arriving sequence.

**Tombstone** — a marker recording that something was deleted, rather than deleting it.

**Transaction** — a group of operations that all happen or none do.

**Upsert** — insert if absent, update if present. In PostgreSQL and DuckDB, `INSERT ... ON CONFLICT ...`.

**Valid time** — when a fact was true in the world. Contrast *knowledge time*.

**Validation** — checking that data matches its expected shape and rules.

**View** — a stored query that behaves like a table but is computed on access. Always current, no
storage cost, recomputed every time.

**Watermark** — in stream processing, a declaration that you no longer expect data older than a given
point. A decision that trades completeness against latency, not a measurement.

**Window function** — SQL that computes across a set of rows related to the current row, without
collapsing them: `lag()`, `row_number() over (partition by ... order by ...)`.

**Windowing** — cutting a stream into finite intervals so aggregates have a defined scope.

---

## Choosing tools: a short, opinionated summary

| Situation | Reasonable first choice | Why |
|---|---|---|
| a local analytical database | **DuckDB** | zero operations, reads files directly, fast for scans |
| a transactional database behind an application | **PostgreSQL** | concurrency, constraints, maturity |
| one file, unattended, single writer | **SQLite** | nothing to run or break |
| SQL transformations you will maintain | **dbt** | dependencies, tests and documentation for free |
| tables that fit in memory | **pandas** | simplest thing that works |
| tables that don't | **the database**, then Spark | move the computation to the data |
| a genuine stream with several consumers | **Kafka** + a stream processor | replay and independent consumers |
| periodic work with dependencies and history | **Airflow** (or your cloud's equivalent) | retries, backfills, visibility |
| periodic work with none of those | **cron** + a wrapper script | five lines beats a service |
| modelling on tables | **XGBoost** / gradient boosting | best default for tabular data, by a distance |
| tracking experiments and deploying models | **MLflow** | tracking plus a registry; registering is deploying |
| an internal dashboard | **Streamlit** | hours, not weeks |
| a public interactive product | **React** + a charting library | control, performance, real interaction |
| a Python HTTP API | **FastAPI** | validation and documentation from the type hints |
| validating anything at a boundary | **Pydantic** (Python) / **Zod** (TypeScript) | one declaration, runtime enforcement |
| reproducible environments | **uv** + a lockfile, then **Docker** | same versions everywhere |
| infrastructure you will change more than once | **CDK** / Terraform | reviewable, reversible, reproducible |
