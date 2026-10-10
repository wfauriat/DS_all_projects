# Pipeline lab — autonomous build

**Copy this file into the pipeline repository and start a session there. Do not
give that session the `refund-lab-source/` repository beyond the narrow access
described under "Boundaries".**

---

## Mission

Build, run and validate a complete data pipeline against a deliberately
badly-behaved vendor, unattended, and grade yourself against the vendor's sealed
fault ledger. You have full autonomy over stack, structure and method.

This is a **baseline run**. Its value is that it was produced without human
guidance, so it can later be compared against a human-built solution and used to
judge how well this class of problem automates. Nobody is going to read your
progress; do the work properly rather than performatively.

### Reporting contract

**Report almost nothing.** Your final message must contain only:

1. a single line confirming completion (or an honest statement of where you
   stopped and why);
2. the verbatim output of one `grade.py --summary` run;
3. elapsed simulated time covered.

**Do not describe your architecture, your technology choices, your file layout,
your models, or any design decision — not in the final message, not in progress
updates.** The person receiving this must stay unprimed so their later
comparison means something.

Keep whatever notes you like *inside the pipeline repository*, in a `NOTES.md`,
including decisions and rationale. That will be read later, deliberately. Just
do not summarise it into the conversation.

---

## Boundaries — read carefully

The lab's fault plan is sealed. The entire exercise is void if you go around it.

**Never:**

- run `python grade.py --reveal` — it unseals the answer key permanently;
- read `refund-lab-source/lab/faults/` (except `catalog.py`, which is public),
  `refund-lab-source/tests/`, or `.state/ledger.sealed`;
- run `python lab.py init` or `init --force` — this would redraw the run and
  destroy the operator's sealed world;
- modify `refund-lab-source/lab.toml`, `.state/`, or delete drop files;
- reverse-engineer the ledger from the lab's source, git history, or by timing.

**You may**, in the source repository: run `python lab.py serve`, run
`python grade.py --incidents <path> --summary`, read `lab.toml` (for the API
token and port) and `README.md`, and read `lab/faults/catalog.py`. Nothing else.

**Run `--summary` exactly once, at the very end.** Running it repeatedly turns
the grader into an oracle you can hill-climb against — log a guess, see if the
count moved, adjust — which would make the baseline worthless.

---

## Operating protocol

### Starting the world

```bash
cd refund-lab-source && python lab.py serve      # leave running
curl -s localhost:8088/v1/health
curl -s localhost:8088/_lab/clock
```

The API token is `[api].token` in `lab.toml`. The drop directory is
`[drop].path`, by default a sibling directory of both repos.

### Compressing time — this part matters

The world advances at one simulated day per real hour, and the horizon is 2200
simulated days: about three real months. You are not going to wait.

`POST /_lab/advance?days=N` jumps the clock forward. **Advance in steps of at
most 7 simulated days, running a full pipeline increment between every step.**
The intended loop is:

```
repeat until the clock reaches the horizon end:
    POST /_lab/advance?days=1        (or up to 7)
    wait ~2-3s for the drop writer to catch up
    run one incremental pipeline cycle
    run your monitors
    append any incident you detected, dated at the current simulated time
```

One simulated day per iteration reproduces the hourly-cron cadence the lab is
built around and gives the truest baseline. Larger steps trade fidelity for wall
time; beyond about a week per step you stop being able to see delivery
sequencing at all.

**Do not jump the whole horizon and then extract once.** You would receive the
final knowledge state of everything, observe no restatements, no late arrivals
and no delivery gaps in sequence, and produce a meaningless detection record.
Jumps are recorded in the ledger and reported by the grader, so this is visible
in your own result.

### Incidents must be logged as they are detected

Append to `incidents.jsonl` **during** the loop, with `detected_at_sim` set to
the simulated clock at the moment your monitor fired. Do not reconstruct the log
at the end from hindsight. Mean-time-to-detect is a scored metric and a
backfilled log makes it a fiction.

It is expected and correct to log a symptom before you understand it. Log what
you noticed, with an honest `confidence`, and refine later if you wish.

---

## The exercise

A vendor runs a marketplace, exposing an HTTP API and a daily file drop. Build a
pipeline that ingests both channels reliably and unattended, stores the data so
history can be reconstructed as it was believed on any past date, transforms it
into conformed and tested models, orchestrates the whole thing on a schedule
with monitoring, and serves a refund-risk prediction per order.

Alongside that, maintain the incident log. The vendor breaks in specific,
catalogued ways at times drawn from a sealed plan. The grader scores your log
for detection rate, mean time to detect, and false positives.

The pipeline is the vehicle. **The incident log is the deliverable being
scored.** A flawless warehouse with an empty log is a failing run.

### Success criteria

- Full extraction of every API endpoint completes at the configured hostility.
- Incremental extraction runs repeatedly without losing or duplicating rows.
- Both channels are ingested, including the drop files.
- Past belief is reconstructable: you can answer "what did the source say on
  date X" and demonstrate it differs from today's answer.
- Conformed models exist over both channels, with tests.
- A scheduled loop runs unattended with monitoring that fires on its own.
- A refund-risk score is produced per order, trained only on what was knowable
  at prediction time.
- `incidents.jsonl` was maintained throughout the run, not at the end.

---

## The data

A European marketplace. Orders, the payments that settle them, web events, a
customer dimension, a product catalogue, and refund outcomes.

| Entity | Grain | Channel |
|---|---|---|
| `orders` | one row per order | API |
| `customers` | **one row per validity interval** (SCD2) | API |
| `events` | one row per web event | API |
| `labels` | refund outcome per order | API |
| `payments` | one row per payment | **file drop only** |
| `products` | catalogue snapshot | **file drop only** |

Payments reach you only through the drop. The two channels have different
failure modes and different trust properties.

### Two timelines

Every fact carries a **valid time** (when it happened: `occurred_at`,
`valid_from`, `resolved_at`) and a **`knowledge_time`** (when the source could
first have told you). The API's `as_of` filters on knowledge time, so `as_of` in
the past reproduces exactly what the vendor would have said then. That is what
makes late arrivals, restatements and historical reconstruction tractable.

A payment's `knowledge_time` lags its `occurred_at` by a processor-dependent
delay: `northpay` settles in minutes, `swiftpay` in hours, `orbit` erratically
and sometimes days. Yesterday's payments file is *what settlement told us
yesterday*, not *what happened yesterday*.

### Labels

A refunded order gets a label once the refund settles. **A non-refunded order
has no label row at all until its 30-day refund window closes**, at which point a
negative appears. Near the edge of any `as_of`, absence of a label is
undetermined, not negative. This is structural and always present.

### Fields

```
orders      order_id, customer_id, occurred_at, channel, payment_method,
            shipping_speed, order_total_cents, items[], version, knowledge_time
customers   customer_id, valid_from, valid_to, signup_date, country, city,
            segment, lifetime_value_cents, is_active, version, knowledge_time
events      event_id, customer_id, session_id, occurred_at, event_type, sku,
            version, knowledge_time
labels      order_id, resolved_at, refund_at, refunded_within_30d,
            version, knowledge_time
payments    payment_id, order_id, customer_id, paid_ts, amount_cents, status,
            processor, billing_city                          (drop file)
products    sku, product_name, category, list_price_cents, active_from,
            discontinued_from                                (drop file)
```

Domains: channels `web|mobile|partner_api`; methods
`card|wallet|bank_transfer|voucher`; processors `northpay|swiftpay|orbit`;
categories `apparel|electronics|home|beauty|outdoor`; segments
`consumer|smb|enterprise`; event types `page_view|search|add_to_cart|
remove_from_cart|checkout_start`; ten countries, several with non-ASCII cities.

A restatement surfaces as the same business key with a higher `version` and a
later `knowledge_time`. Drop files carry neither field.

### Baseline behaviour — not faults, do not log these

- Weekday-heavy trade; weekends at about three quarters.
- Roughly 18% annual growth.
- An annual cycle with a Q4 peak and a January hangover.
- Calendar-predictable extreme days: the Friday after the fourth Thursday in
  November at about 3x, the Monday after at 2.3x, 25 December and 1 January
  nearly stopping.
- Autocorrelated demand shocks, so quiet fortnights are normal.
- Catalogue turnover: SKUs launch, and about a fifth are delisted explicitly via
  `discontinued_from` (the row stays in the file).

Characterise this baseline before hunting anomalies; most of the catalogue is
only visible as a deviation from it.

---

## The vendor contract

Base path `/v1`, JSON, bearer auth, default `http://127.0.0.1:8088`.

### Auth

```
POST /v1/auth/token       Authorization: Bearer <token from lab.toml>
  -> {"access_token": "...", "token_type": "bearer", "expires_in": <real seconds>}
```

Use the access token on read endpoints. Tokens expire about every six simulated
hours; an expired one returns `401 {"error": "token_expired"}`. Re-authenticate
and continue, mid-pull if necessary.

### Read endpoints

| Endpoint | Parameters |
|---|---|
| `GET /v1/orders` | `since`, `until`, `as_of`, `cursor`, `limit` |
| `GET /v1/customers` | `since`, `until`, `as_of`, `cursor`, `limit` |
| `GET /v1/events` | `since`, `until`, `as_of`, `cursor`, `limit` |
| `GET /v1/labels` | `since`, `until`, `as_of`, `cursor`, `limit` |
| `GET /v1/orders/{order_id}` | `as_of` |
| `GET /v1/health` | — |

`since`/`until` filter valid time (`since` inclusive, `until` exclusive).
`as_of` filters knowledge time, defaulting to the current simulated moment; a
future `as_of` is answered as of now. `limit` defaults to 200.

```json
{"data": [...],
 "as_of": "2027-04-02T00:00:00",
 "cursor": "<the position this page was actually served from, null on page 1>",
 "next_cursor": "<pass this back, or null at the end>",
 "total_count": 48213}
```

Cursors are opaque and pin the `as_of`, `since` and `until` they were created
with. The `cursor` field echoes the position the server actually served, which
is not always the one you asked for.

### File drop

```
drop/payments/payments_YYYY-MM-DD.csv.gz
drop/catalog/product_catalog_YYYY-MM-DD.csv
```

Written on simulated-day boundaries. No manifest, no checksum, no notification.
The date in the filename is the content day.

---

## What can go wrong

### Ambient hostility — always on, never scored

Set 0–3 in the lab's config; leave it as the operator set it. It changes
delivery, never data: a complete extraction at level 3 contains exactly the same
records as one at level 0.

- **Level 1** — `429` with `Retry-After`; occasional `500`; `limit` silently
  capped below what you asked for.
- **Level 2** — adds `429` without `Retry-After`; multi-second latency spikes;
  responses sometimes gzipped, sometimes not, with `Content-Encoding`
  occasionally wrong; nulls spelled `null`, `""` or `"None"` depending on
  endpoint; long-lived tokens rejected, so the auth exchange is mandatory.
- **Level 3** — adds cursors that occasionally repeat or skip a page; `200 OK`
  with an error body instead of a `5xx`; connections that hang and time out; a
  subtly wrong `total_count`.

Surviving level 3 requires retry with backoff, idempotent writes, re-auth on
401, tolerance of encoding lies, cursor validation, and no reliance on
`total_count`.

### The fault catalogue — 30 possibilities, 8–14 drawn per run

Public. Which fired, and when, is sealed. `family / group` is what the grader
matches on; `data` and `operational` are always acceptable family answers.

| id | family / group | description |
|---|---|---|
| `dup_exact_payments` | ingestion / data | Write-retry produced byte-identical duplicate payment rows. |
| `dup_near_orders` | ingestion / data | Same basket submitted twice seconds apart under different order_ids. |
| `mojibake_city` | ingestion / data | UTF-8 text decoded as latin-1 somewhere upstream. |
| `id_leading_zeros` | ingestion / data | A join key was round-tripped through an integer type. |
| `field_rename_midstream` | ingestion / data | A JSON field changed name partway through the feed. |
| `tz_mixed` | time / data | Timestamps stop being naive and start carrying offsets (or vice versa). |
| `late_arriving_events` | time / data | Events for day D only become knowable several days later. |
| `dst_duplicate_hour` | time / data | A wall-clock hour repeats / is missing around a DST boundary. |
| `unit_switch_price` | semantic / data | A money column silently changes unit at a date boundary. |
| `category_rename` | semantic / data | A categorical value is renamed mid-stream, splitting one class in two. |
| `new_category_late` | semantic / data | A category value that exists only in the most recent period. |
| `sentinel_values` | semantic / data | Magic values standing in for NULL (-999, 1900-01-01, 'N/A', 'null'). |
| `covariate_shift` | statistical / data | Feature distribution differs between early and late periods. |
| `mnar_missing` | statistical / data | Missingness in a column depends on the outcome itself. |
| `label_censoring` | statistical / data | Labels near the window edge cannot have matured; absence != negative. **in every run** |
| `leakage_column` | leakage / data | A column present at training time that is unavailable at prediction time. |
| `survivorship_customers` | integrity / data | Rows hard-deleted from a dimension, orphaning historical facts. |
| `backfill_restatement` | restatement / operational | Historical rows are revised weeks after the fact, silently. **in every run** |
| `late_partition` | delivery / operational | A simulated day's drop file arrives one or two days late. |
| `missing_delivery` | delivery / operational | A day's file never arrives. Looks like a quiet Sunday. |
| `duplicate_delivery` | delivery / operational | The same payload is delivered twice under different filenames. |
| `partial_write` | delivery / operational | A drop file is truncated mid-row. |
| `column_added` | schema / operational | A new column appears in the drop file at a date boundary. |
| `column_reordered` | schema / operational | Drop file columns are reordered at a date boundary. |
| `encoding_change` | encoding / operational | A drop file switches from UTF-8 to cp1252. |
| `compression_change` | encoding / operational | A drop file starts (or stops) being gzipped, extension unchanged. |
| `clock_skew` | time / operational | Source timestamps drift by minutes, then correct. |
| `sustained_outage` | availability / operational | The API returns 503 for a multi-hour simulated window. |
| `silent_row_deletion` | integrity / operational | Rows disappear from the source with no tombstone. |
| `id_reuse` | integrity / operational | A retired identifier is reassigned to a different entity. |

`label_censoring` and `backfill_restatement` are in every run. The other 6–12
are drawn from the remaining 28.

---

## Scoring

`incidents.jsonl`, one JSON object per line:

```json
{"detected_at_sim": "2027-03-14T08:00:00",
 "detected_at_real": "2026-11-02T14:20:00",
 "symptom": "payments row count for 2027-03-12 dropped 40%",
 "suspected_family": "operational",
 "suspected_fault": "missing_delivery",
 "confidence": "medium",
 "resolution": "reprocessed from API"}
```

Only `detected_at_sim` and one of `suspected_family` / `suspected_fault` are
required.

- Naming the fault correctly is full credit, whenever it was noticed.
- Family only is partial credit if logged while the symptom was live: 60
  simulated days from the fault starting, or as long as it actually lasts,
  whichever is longer.
- A claim dated before the fault started is a false positive.
- Ambient hostility is not in the ledger and is not scored.

At the end, once:

```bash
cd refund-lab-source
python grade.py --incidents ../<pipeline-repo>/incidents.jsonl --summary
```

Reproduce that output verbatim in your final message, add the elapsed simulated
time, and stop. No architecture, no decisions, no file listing.
