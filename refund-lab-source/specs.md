# `refund-lab-source` — build specification

**Audience:** the coding session that will build this. Read section 0 completely before writing any code.

---

## 0. Context and prime directive

### What this is

A **training gym** for data engineering. The human operating it is deliberately practising the
vertical stack — ingestion, storage and modelling, transformation, orchestration, serving,
governance — and wants to do it against a source that misbehaves the way real sources
misbehave, without waiting months for real-world pathologies to occur at their natural rate.

You are building **the source system**. Something that pretends to be a badly-run upstream
vendor: an HTTP API and a daily file drop, both operated by an organisation that does not care
about you, changes things without notice, and occasionally breaks.

### Prime directive

> **You build the source. You never build the pipeline.**

The consumer side — ingestion code, landing zone, warehouse models, tests, orchestration,
serving — is the exercise. If you write any of it, or scaffold it, or leave a helpful example
of it, you have destroyed the thing being built. This holds even if asked mid-session; if the
operator asks for pipeline help, refer them to a separate session that does not have this
repo in context.

### The sealed ledger, and why it exists

At initialisation the lab draws a random seed, selects a subset of faults from a published
catalogue, schedules each one at a random point in simulated time, and writes the whole plan
to a **sealed ledger**. The seed is never printed. Nobody — not the operator, not you, not the
session that helped design this — knows which faults fired or when.

This buys three things at once:

1. **Honest detection practice.** You cannot discover a fault you designed. Sealing means the
   operator's monitoring has to actually work.
2. **Real conditions without real waiting.** Curated pathologies at chosen frequency, rather
   than random pathologies at low natural rates.
3. **A reference solution.** Unlike real data, there *is* a right answer. At the end the ledger
   unseals and produces detection rate, mean-time-to-detect and false-positive count — metrics
   about the operator, not the model.

The catalogue is public. The realisation is sealed. That distinction is the whole design.

---

## 1. Repository boundary

Two separate repositories. They share nothing but a network port and a directory.

```
refund-lab-source/        ← you build this
refund-lab-pipeline/      ← the operator builds this, in another session
    └─ (talks to the lab over HTTP + reads the drop dir; never imports lab code)
drop/                     ← shared filesystem path, configurable
```

Rules, to be restated in the generated README:

- No symlinks between repos, no shared virtualenv, no shared package.
- The lab runs as a **long-lived local service** (`uvicorn`), started once and left running.
- The pipeline repo must never contain lab source, and the pipeline session must never be
  given lab source as context.
- The ledger lives at `refund-lab-source/.state/ledger.sealed` — inside the *source* repo, so
  it is not casually visible from the pipeline working directory.

### Anticipated consumer-side extensions

The pipeline repo is expected to grow two further layers later, both entirely on the consumer
side and both out of scope here:

- an **application layer** — a thin interface through which a person reviews, acts on and
  overrides the pipeline's output;
- a **governance layer** — the operator will designate parts of the served data as sensitive
  and impose their own access control, pseudonymisation, retention and erasure rules on top
  of it.

Recorded only so their eventual existence is not a surprise. **Neither requires any
source-side accommodation, and neither is to be built, scaffolded or designed for now.** The
lab continues to serve every field it serves today, in the open; the sensitivity classification
is the operator's to invent downstream, which is part of the exercise.

---

## 2. Architecture

```
lab/
  world.py       bitemporal truth store; loads from the refactored generator
  generator.py   refactored from the operator's existing generate.py
  clock.py       simulated time
  faults/
    catalog.py   fault definitions (public)
    schedule.py  seed draw, selection, scheduling, sealing (secret at runtime)
    data.py      data-defect implementations (from the existing Planter)
    ops.py       operational-fault implementations (new)
  api.py         FastAPI app: read endpoints + ambient hostility middleware
  drop.py        file-drop writer, driven by the clock
  grade.py       unseals the ledger and scores an incident log
  config.py      loads lab.toml
```

Stack: Python 3.11+, FastAPI + uvicorn, SQLite for the world store, `numpy` (already a
dependency of the existing generator). No external services.

---

## 3. The bitemporal core

**This is the load-bearing decision. Get it right before anything else.**

Every fact carries two timestamps:

- `valid_time` — when the thing happened in the world.
- `knowledge_time` — when the source system could first have told you about it.

The API serves `WHERE knowledge_time <= :as_of`. That single predicate gives you late arrivals,
restatements and label maturation for free, and it makes the source *correct by construction*
while leaving the pipeline to work out how to cope.

### Tables

All tables carry `version INTEGER`, `knowledge_time TIMESTAMP`, and a business key. A
restatement is a new row with the same business key, a higher version, and a later
`knowledge_time`.

| Table | Business key | valid_time column | Notes |
|---|---|---|---|
| `customers` | `customer_id` | `valid_from`, `valid_to` | SCD2 on valid time *and* knowledge time |
| `orders` | `order_id` | `occurred_at` | |
| `payments` | `payment_id` | `occurred_at` | `knowledge_time` lags `occurred_at` by a processor-dependent delay |
| `events` | `event_id` | `occurred_at` | high volume; some arrive late by design |
| `labels` | `order_id` | `refund_at` | see maturation below |

### Label maturation

Refund labels must not be knowable before they exist. For a refunded order,
`knowledge_time = refund_at + settlement_delay`. For a non-refunded order there is **no label
row at all** until the refund window closes, at which point a negative label appears with
`knowledge_time = occurred_at + refund_window`.

This makes `label_censoring` structural rather than a planted defect: near the edge of any
`as_of` window, absence genuinely does not mean negative. The operator has to discover that.

### Latent truth must be unreachable

The world store holds columns the source could never expose — true refund propensity, the
segment assignment that drives it, and so on. Prefix them `_` and enforce an **explicit column
allowlist** in the API serialisation layer. Allowlist, not blocklist: a new latent column added
later must fail closed.

Write a test that asserts no `_`-prefixed key appears in any API response or drop file.

---

## 4. The clock

Two clocks run simultaneously. The operator's pipeline runs on **real** time (hourly cron,
unattended, for months). The world runs on **simulated** time, faster.

```
sim_now = sim_start + (real_now - real_epoch) * ratio
```

Default `ratio`: 1 real hour = 1 simulated day. Over three real months that yields roughly six
simulated years — enough for dozens of fault events, several DST boundaries, and a genuine
incident log.

Persist `sim_start`, `real_epoch`, `ratio` and `paused` in `.state/clock.json` so a service
restart resumes rather than rewinds.

Lab-control endpoints under `/_lab/` (clearly marked as not part of the simulated vendor API):

- `GET /_lab/clock` — current simulated time, ratio, paused state
- `POST /_lab/pause`, `POST /_lab/resume`
- `POST /_lab/ratio` — change acceleration
- `POST /_lab/advance?days=N` — jump forward (records the jump; the grader reports elapsed
  simulated time so a skipped horizon is visible)

---

## 5. Faults

### Two categories, and the distinction matters

**Ambient hostility** is the environment. Always present, tuned by a level in config, *not*
recorded in the ledger, because it is not a puzzle — it is the baseline the pipeline must be
robust to from day one. Rate limiting, transient errors, pagination quirks.

**Sampled faults** are the puzzle. Each fires once at a scheduled simulated time, is recorded
in the sealed ledger, and is what the grader scores.

### Sampled fault catalogue

*Data family* — port all 17 from the operator's existing `CATALOG` in `generate.py`:
`dup_exact_payments`, `dup_near_orders`, `mojibake_city`, `id_leading_zeros`,
`field_rename_midstream`, `tz_mixed`, `late_arriving_events`, `dst_duplicate_hour`,
`unit_switch_price`, `category_rename`, `new_category_late`, `sentinel_values`,
`covariate_shift`, `mnar_missing`, `leakage_column`, `survivorship_customers`,
`label_censoring`.

Re-plumb them through `knowledge_time` rather than applying them to a static materialised
dataset. `late_arriving_events` and `label_censoring` become properties of the knowledge
timeline rather than post-hoc mutations.

*Operational family* — new:

| Fault | Behaviour |
|---|---|
| `backfill_restatement` | **Always on.** Historical rows are revised weeks later, silently. |
| `late_partition` | A simulated day's drop file arrives one or two days late |
| `missing_delivery` | A day's file never arrives. Looks like a quiet Sunday. |
| `duplicate_delivery` | Same payload delivered twice under different filenames |
| `partial_write` | File truncated mid-row |
| `column_added` / `column_reordered` | Schema drift at a date boundary |
| `encoding_change` | Drop file switches from UTF-8 to cp1252 |
| `compression_change` | Drop file starts (or stops) being gzipped, extension unchanged |
| `clock_skew` | Source timestamps drift by minutes, then correct |
| `sustained_outage` | API returns 503 for a multi-hour simulated window |
| `silent_row_deletion` | Rows disappear from the source with no tombstone |
| `id_reuse` | A retired identifier is reassigned to a different entity |

`backfill_restatement` is mandatory in every run. Without it the operator cannot practise
reconstructing what the system believed on a past date, which is the central skill this lab
exists to teach.

### Selection, scheduling and sealing

At `lab init`:

1. Draw the seed from `os.urandom(16)`. **Never print it.** Print only a short run-id
   (`sha256(seed)[:8]`).
2. Sample 8–14 faults (configurable), always including `backfill_restatement` and
   `label_censoring`.
3. Schedule each at a random simulated time across the horizon, allowing some to cluster —
   real incidents arrive in bunches and a cluster teaches alert triage.
4. Write the ledger: for each fault, `id`, `family`, `sim_start`, `sim_end`, `params`,
   `affected_keys`.
5. Seal it: `base64(zlib(json))`, same pattern as the operator's existing `.manifest.b64`.
   Print a `sha256` of the sealed blob so tampering or regeneration is detectable.

### Operational security of the fault system

These are as important as the code:

- The service log the operator tails must show **only what a real vendor would show**. No fault
  names, no "injecting X", no debug lines.
- No endpoint lists active faults. No `--verbose` reveals them.
- No comment in `api.py` or `drop.py` explaining which fault causes which behaviour. Put all
  such commentary in `faults/`, which the operator is instructed not to open.
- Exception messages must not name faults.

---

## 6. HTTP API

Base path `/v1`. Bearer token auth, token in config. JSON.

### Endpoints

| Endpoint | Params |
|---|---|
| `GET /v1/orders` | `since`, `until`, `as_of`, `cursor`, `limit` |
| `GET /v1/customers` | `since`, `as_of`, `cursor`, `limit` |
| `GET /v1/events` | `since`, `until`, `as_of`, `cursor`, `limit` |
| `GET /v1/orders/{id}` | `as_of` |
| `GET /v1/health` | — (lies convincingly during data-quality faults) |

`since`/`until` filter on `valid_time`. `as_of` filters on `knowledge_time`, defaulting to
simulated now. Cursor pagination, opaque cursor.

### Ambient hostility levels

Level configurable 0–3. **Level 0 must be clean** — a golden-data test compares full
extraction at every level and asserts byte-identical results. Hostility changes *delivery*,
never *data*. An unfair lab teaches nothing.

- **Level 1:** 429 with `Retry-After` at ~2% of requests; occasional 500; `limit` silently
  capped at 500 regardless of what was asked.
- **Level 2:** adds — 429 *without* `Retry-After` sometimes; 5s+ latency spikes; responses
  sometimes gzipped, sometimes not, `Content-Encoding` occasionally wrong; nulls encoded
  variously as `null`, `""` and `"None"` across endpoints; auth token expires every ~6 simulated
  hours requiring re-auth mid-pull.
- **Level 3:** adds — cursor occasionally repeats a page or skips one (~0.5%); `200 OK` with an
  error body (`{"error": "temporary failure"}`) instead of a 5xx; connections that hang and
  time out; `total_count` in the envelope that is subtly wrong.

Default level 1 for the first phase of the operator's work, rising to 3. Expose it in config,
not over the API.

---

## 7. File drop channel

A directory (default `./drop/`), written by a background task on simulated-day boundaries.

```
drop/payments/payments_YYYY-MM-DD.csv.gz
drop/catalog/product_catalog_YYYY-MM-DD.csv
```

The daily payments extract is the second ingestion pattern and the natural home of the
delivery faults — late, missing, duplicated, truncated, re-encoded, schema-drifted. No
manifest, no checksum, no notification. Files are simply there, or not.

Write files atomically (temp then rename) *except* when `partial_write` is firing, which is the
whole point of that fault.

---

## 8. Grader

`python grade.py --incidents ../refund-lab-pipeline/incidents.jsonl [--summary | --reveal]`

Operator's incident log contract, one JSON object per line:

```json
{"detected_at_sim": "2027-03-14T08:00:00", "detected_at_real": "2026-11-02T14:20:00",
 "symptom": "payments row count for 2027-03-12 dropped 40%",
 "suspected_family": "operational", "suspected_fault": "missing_delivery",
 "confidence": "medium", "resolution": "reprocessed from API"}
```

Matching: an incident matches a ledger fault if `suspected_fault` is correct **or** the symptom
window overlaps the fault window and the family is right (partial credit — recognising that
*something* broke is most of the skill).

Two output modes, and both are needed:

- `--summary`: counts only. Detected *n* of *m*, per-family detection rate, MTTD in simulated
  time, false-positive count. **Reveals nothing about what was missed.** Safe to run weekly.
- `--reveal`: full unsealing, every fault with its schedule and parameters, matched and
  unmatched. A one-way door — the README should say so plainly.

MTTD is measured in simulated time from `sim_start` of the fault to `detected_at_sim`.

---

## 9. Configuration

`lab.toml`:

```toml
[world]     scale = 1.0          horizon_sim_days = 2200
[clock]     sim_start = "2026-01-01"   sim_days_per_real_hour = 24
[api]       port = 8088   hostility = 1   token = "..."
[drop]      path = "./drop"
[faults]    n_sampled = 11
```

Changing `hostility`, `sim_days_per_real_hour` and `drop.path` must not require re-init.
Changing `world` or `faults` requires a fresh run-id and voids the ledger.

---

## 10. Build phases

Do not proceed to the next phase until the acceptance test passes.

| # | Deliverable | Acceptance test |
|---|---|---|
| 0 | Repo skeleton, config, `lab init`, sealed-seed machinery | Run-id printed; seed appears nowhere in plaintext (grep the repo); ledger sealed; re-running init without `--force` refuses |
| 1 | Bitemporal world store, populated by refactored generator | `as_of=T2` returns a superset of `as_of=T1` for `T2>T1`; no `_`-prefixed column reachable by any accessor; row counts within tolerance of the original `generate.py` at equivalent scale |
| 2 | Clock | `sim_now` advances at the configured ratio; pause/resume works; service restart resumes position rather than rewinding |
| 3 | Read-only API, hostility 0 | Full extraction via pagination returns every row exactly once; `as_of` honoured; allowlist test passes; naive sequential client succeeds |
| 4 | Ambient hostility 1–3 | Golden test: full extraction at levels 0/1/2/3 yields byte-identical data. A naive client fails at level 2; a retrying, idempotent, cursor-validating client succeeds |
| 5 | File drop + operational faults | Files appear on simulated-day boundaries; each operational fault can be force-fired via a test-only harness and produces its documented signature; forced faults do not touch the sealed ledger |
| 6 | Data defects re-plumbed through knowledge_time | Each of the 17 force-fires and produces its expected signature; with forcing off, only the sealed subset occurs |
| 7 | Grader | A synthetic perfect incident log scores 100%; an empty log scores 0%; a log with one spurious entry reports exactly 1 false positive; `--summary` output contains no fault name absent from the operator's own log |
| 8 | README + operator runbook | Fresh clone starts the service in one documented command; README states the prime directive, the two-repo boundary, and that `--reveal` is one-way |

---

## 11. Prohibitions

1. Do not write, scaffold or exemplify pipeline code — no ingestion client, no dbt project, no
   warehouse DDL, no orchestration DAG, no serving layer. This extends to the anticipated
   application and governance layers noted in §1: mentioning them is not authorisation to
   build, stub or design around them.
2. Do not print the seed, fault names or schedule at runtime.
3. Do not add any endpoint, flag or log line that reveals active faults.
4. Do not let hostility level alter the underlying data.
5. Do not use a blocklist where an allowlist is specified.
6. Do not "helpfully" make the source well-behaved. Undocumented behaviour, inconsistent
   nulls and silent caps are features.

---

## 12. Reuse from `generate.py`

The operator will supply their existing 610-line generator. Keep, in order of value:

- `build_world` — the heterogeneous marketplace world, Pareto customer activity, the
  multi-country city/character-set mix. Refactor to emit a longer horizon with
  `knowledge_time` assigned per fact rather than a single 180-day materialisation.
- `CATALOG` and `Planter` — the 17 defect implementations, largely intact. Adapt each to act on
  the bitemporal store.
- The sealing pattern from `main()` — `base64(zlib(json))` with a do-not-decode header.
- The `ALWAYS_ON` concept — extend it from `label_censoring` alone to
  `[label_censoring, backfill_restatement]`.

Discard: the one-shot `main()` flow, the required `--seed` CLI argument (now hostile to the
exercise), and the direct file writers, which are superseded by the API and drop channel.
