# refund-lab-source

A training gym for data engineering. This repository is **the source system**: an
HTTP API and a daily file drop, operated by a vendor that does not care about
you, changes things without notice, and occasionally breaks.

It is the thing your pipeline consumes. It is not the exercise.

---

## The prime directive

> **This repo builds the source. It never builds the pipeline.**

The consumer side — ingestion, landing zone, warehouse models, tests,
orchestration, serving, and later an application layer and a governance layer —
is the exercise. None of it belongs here, not even as an example. If you want
help with pipeline code, do it in a session that does not have this repository
in context.

## The two-repo boundary

```
refund-lab-source/          this repo: the vendor
  .state/ledger.sealed      the answer key, sealed
refund-lab-pipeline/        your repo: everything you build
drop/                       shared directory, configurable
```

- No symlinks between the repos, no shared virtualenv, no shared package.
- The pipeline repo must never contain lab source, and the pipeline session must
  never be given lab source as context.
- The lab is a long-lived local service. Start it once and leave it running.

## What not to read

Reading these ends the exercise. They are the answer key in source form.

| Path | Why |
|---|---|
| `lab/faults/schedule.py` | how this run was drawn and sealed |
| `lab/faults/data.py`, `ops.py`, `effects.py` | which behaviour comes from which fault |
| `lab/faults/harness.py` | force-fires a named fault |
| `tests/` | asserts the exact signature of every fault |
| `.state/ledger.sealed` | the plan itself |

`lab/faults/catalog.py` **is** public and worth reading. It lists all 30 faults
that could be drawn, with their family. Knowing what can happen is not the same
as knowing what did.

---

## Running it

Python 3.11+.

```bash
pip install -r requirements.txt
python lab.py up
```

`up` draws a run if there is not one already, then serves. It will never
replace an existing run. To do the two steps separately:

```bash
python lab.py init          # draws the run, builds the world, seals the ledger
python lab.py serve
```

`init` prints a **run-id** and the **sha256 of the sealed ledger**. Write both
down. If the sha256 ever changes, the ledger was regenerated or edited, and any
score computed against it means nothing.

Re-running `init` refuses unless you pass `--force`. Forcing discards the world,
the clock, the drop directory and every result you have measured.

### What init leaves behind

```
.state/ledger.sealed     the sealed plan (compressed, base64; do not decode)
.state/world.db          the bitemporal world, ~250 MB at default scale
.state/clock.json        simulated-time position, survives restarts
.state/run.json          run-id, fingerprint, digest (safe to read)
../drop/                 the file drop channel, a sibling of both repos
```

At default scale `init` takes about fifteen seconds and produces roughly 150k
orders, 145k payments, 730k events and 50k customers across a six-year horizon.

### Getting a feel for the source

`./explore.sh` is a curl cookbook with the arguments filled in — a set of single
requests that show `as_of`, `since`/`until`, cursors, label maturation and the
drop files. It does not retry, re-page or validate anything; that part is yours.

```bash
./explore.sh scratch          # build a throwaway world in /tmp/labplay
./explore.sh probe -a 300     # probe a running instance, after a 300-day jump
```

Explore on the scratch world, not your sealed run: `--advance` spends horizon and
is recorded in the jump log the grader reports.

---

## Time

Two clocks run at once. Your pipeline runs on real time — hourly, unattended,
for months. The world runs on simulated time, faster:

```
sim_now = sim_start + (real_now - real_epoch) * ratio
```

The default ratio is **1 real hour = 1 simulated day**. That is chosen so one
hourly pipeline run corresponds to exactly one new simulated day, which is one
new file per drop feed — the cadence the whole lab is built around. The
2200-day horizon then takes roughly three real months of uptime, after which
the clock stops at the end of it.

Raising the ratio spends the horizon proportionally faster: at 24 it is gone in
under four real days, and each hourly run has 24 days of files waiting.

**Downtime counts.** The clock is anchored to wall time, so simulated time keeps
passing while the service is stopped. If you are putting the exercise down for
more than a day, `POST /_lab/pause` first.

Lab control endpoints, which are *not* part of the vendor surface:

| Endpoint | Effect |
|---|---|
| `GET /_lab/clock` | simulated now, ratio, paused, elapsed, recorded jumps |
| `POST /_lab/pause` / `POST /_lab/resume` | stop and restart simulated time |
| `POST /_lab/ratio?sim_days_per_real_hour=N` | change the acceleration |
| `POST /_lab/advance?days=N` | jump forward |

Jumps are recorded. The grader reports how much of the horizon actually elapsed,
so skipping most of it is visible in your own result.

---

## Configuration

`lab.toml`. Two classes of setting:

**Free to change between restarts** — `[api].hostility`,
`[clock].sim_days_per_real_hour`, `[drop].path`. These change how the source
behaves towards you, not what it contains.

**Part of the run's identity** — `[world].scale`, `[world].horizon_sim_days`,
`[clock].sim_start`, `[faults].n_sampled`. The service refuses to start if these
no longer match the sealed ledger. Changing them means `init --force`, a new
run-id, and a clean slate.

`[api].hostility` runs 0–3 and is the single most useful dial you have.
Level 0 is a clean, well-behaved source. Level 3 is a bad day at a bad vendor.
**Hostility only ever changes delivery, never the data**: a complete extraction
at level 3 contains exactly the same records as one at level 0. Start at 1 and
raise it when your ingestion stops being the interesting part of the problem.

---

## Vendor documentation

*What follows is the documentation the vendor publishes. Like most vendor
documentation it is accurate about the happy path, silent about the limits, and
out of date in places. Undocumented behaviour is not a bug report.*

### Authentication

```
POST /v1/auth/token          Authorization: Bearer <token from lab.toml>
  -> {"access_token": "...", "token_type": "bearer", "expires_in": 900}
```

Use the returned access token on read endpoints. Tokens expire; a request with
an expired token returns `401` with `{"error": "token_expired"}`, and the fix is
to request another one and carry on.

### Read endpoints

Base path `/v1`. JSON. All collection endpoints take the same parameters.

| Endpoint | Parameters |
|---|---|
| `GET /v1/orders` | `since`, `until`, `as_of`, `cursor`, `limit` |
| `GET /v1/customers` | `since`, `until`, `as_of`, `cursor`, `limit` |
| `GET /v1/events` | `since`, `until`, `as_of`, `cursor`, `limit` |
| `GET /v1/labels` | `since`, `until`, `as_of`, `cursor`, `limit` |
| `GET /v1/orders/{order_id}` | `as_of` |
| `GET /v1/health` | — |

- `since` and `until` filter on **valid time** — when the thing happened.
  `since` is inclusive, `until` exclusive.
- `as_of` filters on **knowledge time** — when this system could first have told
  you. It defaults to the current simulated moment. A read with `as_of` in the
  past reproduces what we would have told you then.
- `limit` defaults to 200.

Response envelope:

```json
{"data": [ ... ],
 "as_of": "2027-04-02T00:00:00",
 "cursor": "<the position this page was served from, null on the first page>",
 "next_cursor": "<pass this back, or null at the end>",
 "total_count": 48213}
```

Cursors are opaque and pin the `as_of`, `since` and `until` they were created
with, so a pull that spans a long time still returns a consistent picture.

### Records

`customers` is a slowly-changing dimension: one row per validity interval, so a
customer id appears more than once. `valid_to` is null on the open interval.

`products` is the current range. `active_from` is when a SKU was listed and
`discontinued_from`, when set, is when it stopped being sold; delisted SKUs stay
in the catalogue.

`labels` carries the refund outcome for an order. A refunded order gets a label
once the refund settles. A **non-refunded order gets no label row at all until
the 30-day refund window closes**, at which point a negative one appears. Near
the edge of any `as_of`, the absence of a label is not a negative.

### File drop

A directory, default `../drop`, written on simulated-day boundaries.

```
drop/payments/payments_YYYY-MM-DD.csv.gz
drop/catalog/product_catalog_YYYY-MM-DD.csv
```

The payments extract for day D contains what settlement told us about on day D,
which is not the same as what happened on day D. The catalogue file is a full
snapshot of the current range: a SKU appears once it launches, and a delisted
one stays in the file with `discontinued_from` set rather than disappearing.
There is no manifest, no checksum and no notification. Files are simply there,
or not.

---

## What the world looks like

Baseline behaviour, documented so you don't file it as a fault. None of this is
in the ledger; all of it is always on.

- **Trade is weekday-heavy.** Weekends run at about three quarters of a weekday.
- **The business grows**, around 18% a year, compounding across the horizon.
- **There is an annual cycle**, with a Q4 peak, a January hangover and a summer
  bump.
- **A handful of days a year are not like the others.** The Friday after the
  fourth Thursday in November runs at roughly 3x, the Monday after at 2.3x,
  and 25 December and 1 January nearly stop. These are calendar-predictable.
- **Good and bad weeks persist.** Day-to-day demand carries an autocorrelated
  shock rather than fresh noise each morning, so a quiet fortnight is normal.
- **The catalogue turns over.** Roughly half the range predates the horizon and
  the rest launches during it; about a fifth is eventually delisted. Demand is
  concentrated — the top tenth of SKUs carry about a third of units — but no
  single SKU dominates.

All of that shapes **volume**. Basket value is deliberately left flat, so a step
change in order value is never the seasonality talking.

### A second exercise, if you want one

Because the volume series carries a trend, an annual cycle, holiday spikes and
genuine autocorrelation, this same source supports a **revenue or demand
forecasting** exercise instead of, or alongside, refund scoring. Per-customer
spend is also predictable: activity is persistent, and a customer's web events
in one period predict their orders in the next.

Two warnings if you go that way. `customers.lifetime_value_cents` is drawn
independently of the order stream and correlates with actual spend at about
-0.01 — it is a decoy, not a target. And `label_censoring` is in every ledger:
if you never query `/v1/labels` you will never find it, so change `ALWAYS_ON`
in `lab/faults/catalog.py` before you `init` (it has no effect on a run already
sealed).

---

## Keeping an incident log

This is the deliverable that gets scored. One JSON object per line, anywhere you
like — `../refund-lab-pipeline/incidents.jsonl` by convention.

```json
{"detected_at_sim": "2027-03-14T08:00:00",
 "detected_at_real": "2026-11-02T14:20:00",
 "symptom": "payments row count for 2027-03-12 dropped 40%",
 "suspected_family": "operational",
 "suspected_fault": "missing_delivery",
 "confidence": "medium",
 "resolution": "reprocessed from API"}
```

`detected_at_sim` and one of `suspected_family` / `suspected_fault` are
required; the rest is for you. Log the moment you *noticed*, not the moment you
understood — that is what mean-time-to-detect measures.

Naming the fault correctly is full credit, whenever you noticed it. Getting only
the family right is partial credit if you logged it while the symptom was still
live — recognising that *something* broke is most of the skill.

"Still live" means **60 simulated days from the fault starting, or as long as
the fault actually lasts, whichever is longer**. It never shrinks for short
faults: a six-hour outage still gives you 60 days to file it. And the two
always-on faults run for the whole horizon, so they stay creditable for the
whole horizon.

A claim dated before the fault started is a false positive, not a detection.

Families are listed in `lab/faults/catalog.py`; `data` and `operational` are
always acceptable answers for the family field.

## Grading

```bash
python grade.py --incidents ../refund-lab-pipeline/incidents.jsonl --summary
```

Counts only: how many of the drawn faults you caught, how many you named
outright, false positives, mean time to detect in simulated days, and per-family
rates **restricted to families you yourself claimed**. It names nothing you did
not name first, and tells you nothing about what is still out there. Safe to run
weekly.

```bash
python grade.py --incidents ../refund-lab-pipeline/incidents.jsonl --reveal
```

Unseals everything: every fault drawn for this run, when it fired, with what
parameters, matched and unmatched.

**`--reveal` is a one-way door.** You cannot un-know it, the run is over as an
exercise, and the fact that you ran it is recorded in `.state/revealed` and
printed at the top of every later summary. Do it when you are finished, not when
you are stuck.

Ambient hostility — rate limits, transient errors, pagination quirks — is not in
the ledger and is not scored. It is the environment, always on, and your pipeline
is expected to be robust to it from the first day.

---

## Layout

```
lab.py            CLI: up / init / serve
grade.py          scoring and unsealing
explore.sh        curl probes for hand-testing the API
lab/
  config.py       lab.toml
  clock.py        simulated time
  generator.py    the world
  world.py        bitemporal store, serialisation allowlists
  api.py          HTTP surface and ambient hostility
  drop.py         the file drop channel
  faults/
    catalog.py    public: the 30 faults that could be drawn
    ...           private: which were, and when
generate.py       the original one-shot generator this world grew out of,
                  kept because a test checks the new one still matches its
                  densities. Not used at runtime.
```

Run the lab's own tests with `python -m pytest`. They will tell you the answers,
so run them only if you are changing the lab.
