# refund-lab-autom — technical synthesis

> The **consumer side** of a data-engineering "gym". A sibling repo (`refund-lab-source`, not
> documented here) plays a badly-behaved vendor: an HTTP API plus a daily file drop, with a *sealed*
> ledger of ~11 faults drawn from a public catalogue of 30 and scheduled at unknown points in a
> 2200-day simulated horizon. This repo ingests it unattended to the horizon and writes
> `incidents.jsonl`.
>
> **The pipeline is the vehicle; the incident log is the deliverable.** That single framing explains
> almost every design decision in the codebase, and makes this project structurally different from the
> other three: it is a *detection* system with a warehouse attached, not a warehouse with monitoring bolted on.

---

## 1. Identity

| Item | Value |
|---|---|
| Domain | e-commerce orders / events / customers / labels (API) + payments / product catalogue (file drop) |
| Shape | single Python package, one process, one SQLite file; hand-rolled everything |
| Dependencies | `requests`, `pandas`, `numpy`, `scikit-learn`. **No** DuckDB, no dbt, no Airflow/Prefect, no Docker, no FastAPI |
| Size | 25 files, ~5.9k lines; `pipeline/` ≈ 3.2k lines, docs ≈ 1.0k lines |
| Docs | `NOTES.md` (646 lines — the real artefact: measurements, false positives, retractions, coverage table, post-mortem), `AUTONOMOUS.md` (the exercise protocol), `README.md` |
| Scale reached | 2200/2200 simulated days, 2147 cycles, 146k orders / 733k events / 48k customers / 141k payments / 4,397 drop files, 143,936 orders scored, 14,862 data-test passes, **9 incidents, 4 retractions** |

The scoring regime is what drives the engineering: detections are matched against the sealed ledger,
MTTD is scored, and unmatched claims are **false positives**. So the project optimises for
*specificity* and for *in-the-loop* detection with an honest timestamp.

---

## 2. Topology

```
refund-lab-source  (separate process, separate repo)
   ├── HTTP API  /v1/{orders,customers,events,labels}  + /v1/health  + /_lab/{clock,advance}
   │      paginated, cursored, filters on valid time (since/until) AND knowledge time (as_of)
   │      hostile: 429s, 401 token expiry, 503 windows, lying Content-Encoding, cursor anomalies,
   │               200 OK bodies containing {"error": ...}, a total_count that may lie
   └── file drop  drop/payments/payments_YYYY-MM-DD.csv[.gz], drop/catalog/product_catalog_*.csv
          no manifest, no checksum, no notification

run.py / loop.sh
   └── pipeline.orchestrator.Pipeline.cycle()   one cycle == one simulated day
         advance world (sampled in 3 steps, probing availability)
         → start background drop-watch thread
         → API channel: rolling re-pull + count reconciliation (bisected) + rotating deep audit
         → drop channel: bytes-first decoding, landing, bitemporal load
         → conformed models over dirty partitions only
         → data tests (partition-scoped each cycle, full every 50)
         → monitors  ──► incidents.jsonl (appended the moment a detector fires)
         → model retrain (every 60 sim days) / score new orders
         → checkpoint state

landing/         raw bytes + raw JSONL.gz, exactly as delivered, never rewritten
warehouse/wh.db  src_* (append-only, bitemporal) · cur_* (current belief) · dim_/fct_/agg_ · ops tables
incidents.jsonl  the deliverable    retractions.jsonl  withdrawn claims, with reasons
```

---

## 3. Entry points

| Command | What it does |
|---|---|
| `./loop.sh` | unattended driver: re-runs `run.py run` up to 200 times, exits 0 at the horizon |
| `python run.py bootstrap` | one full walk of every endpoint from the start of the world |
| `python run.py cycle N` | N pipeline increments |
| `python run.py run` | until the horizon |
| `python run.py status` | row counts per table, model history, incident list |
| `python run.py tests` | the full data-test suite now |
| `python asof.py order\|customer\|day\|diff` | historical-belief reconstruction and restatement diffing |
| `python score.py <order>\|--day\|--summary` | the served score with its inputs; decile calibration |
| `python evaluate.py` | **prospective** evaluation of the scores actually written at ingest time |
| `python -m pytest tests/` | detector unit tests against synthetic corruption |

---

## 4. The bitemporal warehouse (`pipeline/storage.py`)

The load-bearing idea. Two layers in one SQLite file:

| Layer | Contract |
|---|---|
| `src_<entity>` | **append-only**, PK `(business key…, version)`, `WITHOUT ROWID`. One row per version ever served, carrying the source's `knowledge_time` and our `first_seen_sim`. Never updated, never deleted |
| `cur_<entity>` | current belief, maintained incrementally by a **version-guarded upsert** (`ON CONFLICT … DO UPDATE … WHERE excluded.version > cur.version`), with `restated` counter and a `deleted_sim` tombstone column |

Consequences that fall out of it:
- *"What did the source say on date X?"* is a query, not an archive restore:
  `Store.belief_as_of(entity, as_of, key=, day=)` selects from `src_*` the max version whose
  `knowledge_time <= as_of`. That is the whole of `asof.py`.
- Half the fault catalogue (restatements, late arrivals, silent deletion, censoring) is **only
  visible** by comparing current belief with past belief.
- Rebuilding "latest version per key" from history each cycle would have made 2200 cycles
  unaffordable; the incremental `cur_*` makes each cycle O(rows that changed).
- A row the source stops serving is **tombstoned (`deleted_sim`), never deleted** — historical facts
  must still join.

The same treatment is extended to the drop channel (`src_payments` keeps every row of every file as
delivered, duplicates included, because *the duplicate is the evidence*; `cur_payments` is the
deduplicated truth). `NOTES.md` §11 records that applying it *only* to the API was the blind spot
that hid 375 stale revised payment amounts — "every channel restates, not just the versioned one".

Operational tables in the same DB: `pull_log`, `day_counts`, `metrics` (a tiny generic
`(sim_day, metric, key) → value` series store), `field_seen`, `value_seen`, `alerts`, `test_results`,
`incident_log` (the dedupe table), `predictions`, `model_runs`, `expected_drop`, `drop_files`, `state`.
SQLite pragmas: WAL, `synchronous=OFF`, 120 MB cache — chosen for an unattended single-writer run.

**Why SQLite** (stated): no DuckDB available, the working set fits, one file, crash-safe, no service
to babysit for a multi-hour unattended run.

---

## 5. The hostile-transport client (`pipeline/vendor.py`)

Everything hostile about the transport is absorbed here so no other module has to think about it:

- bearer-token exchange, cached, re-issued on `401` **and** on a `200` body saying `token_expired`;
- retry with jittered exponential backoff honouring `Retry-After`, on 429 / 5xx / connection reset /
  read timeout / unparseable body / `200 OK` carrying an error document;
- **content encoding sniffed from the bytes** (`raw[:2] == b"\x1f\x8b"`), never trusted from the
  header — and a disagreement is *counted* (`encoding_lies`);
- pagination that validates cursors: stops on a repeated cursor, a cursor that does not move, an empty
  page that still offers a successor, or a page whose keys were already seen in this pull;
- `total_count` used **only as a change hint**, never as a completeness check;
- a separate `probe()` that is deliberately **not** retried, because "is the source up right now" is a
  different question from "get me this data" — retrying a 503 for 17 s is correct for the latter and
  wrong for the former;
- a `TransportStats` tally per cycle (requests, retries, status histogram, reauths, timeouts,
  body_errors, encoding_lies, cursor_anomalies, count_mismatch, sleep_seconds,
  `unavailable_streak_s`).

**Ambient hostility is counted, not reported.** The explicit rule: transport noise is the environment,
not a finding; only a *sustained* 503 window becomes an incident.

---

## 6. Extraction strategy (`pipeline/extract_api.py` + `orchestrator`)

The central problem is stated plainly: the API can filter valid time and knowledge time, but there is
**no way to ask "what became knowable since I last looked"**. Four mechanisms, each with a reason:

| Mechanism | Cost | Catches |
|---|---|---|
| **rolling** re-pull of the last 3–5 valid-time days every cycle | 4 pulls | ordinary late arrivals, fresh restatements |
| **count reconciliation** over coarse buckets, **bisected 4-way** to the offending day | 1 request per bucket | rows appearing far in the past; rows *disappearing* |
| **rotating deep audit** — full re-pull of individual historical days, weighted to the 15–75-day band | 2 days × 2 entities | restatements, which change no count and are invisible to reconciliation |
| **bootstrap** — one full walk of every endpoint | once | everything at t₀ |

Buckets rotate (`RECON_BUCKETS = [(0,14,1), (14,60,2), (60,240,3), (240,10000,5)]`) so a cycle costs
~20 requests instead of ~50 — which matters because ~3 % of requests cost a ~1 s `Retry-After` sleep.
Every conclusion drawn from `total_count` is **confirmed by a real pull** before it reaches the
warehouse, which keeps the design honest even where the count is documented to lie.

`_bisect()` is the interesting routine: compare remote vs local count for a span; if equal, return; if
the span is one day, re-pull it, compute the set difference of ids, tombstone what vanished, record
`day_counts`; otherwise split into ≤4 pieces and recurse only into the ones that disagree. A
`probe_budget` per entity bounds the recursion.

---

## 7. Normalisation: "repair, but record" (`pipeline/normalize.py`)

Three stated principles: **never drop a field**, **repair but record**, **never raise**.

- Unknown fields → `extra_json` + a schema-drift signal. "A field we throw away is a fault we cannot see."
- A **renamed field** is matched to the missing expected field by `difflib.SequenceMatcher` ratio
  + 0.35 bonus for a shared underscore token, threshold 0.55 — so downstream keeps working *and* the
  mapping is reported.
- An identifier that lost its leading zeros is re-padded to the known width (`customer_id` = 7) so the
  join survives, and the repair is counted **because the repair is the evidence**.
- **Sentinels** (`-999`, `1900-01-01`, `N/A`, `TBD`…) become NULL and are reported; bare null
  spellings (`""`, `null`, `none`) deliberately **do not** count — "those are how a source says
  nothing, and at higher hostility levels also how the transport says it."
- **Mojibake** detection requires the *pair* (`Ã`/`Â`/… followed by a high byte), because a lone
  accented capital occurs in genuine European text (`Åland` is a real place) but `Ã³` is not. Verified
  against real accented city names.
- A `NormStats` accumulator carries the evidence (`unexpected_fields`, `missing_fields`, `renamed`,
  `padded_ids`, `id_width_violations`, `tz_offsets`/`tz_naive`, `sentinels`, `mojibake`,
  `unparseable_ts`) and is what the monitors actually read.

## 8. The file channel (`pipeline/extract_drop.py`)

No manifest, no checksum, no notification, so every fact is established **from the bytes**:
compression from the magic number (not the extension); encoding by trying UTF-8 strict and recording
the cp1252 fallback; truncation from a short gzip stream (salvaged with `zlib.decompressobj`), a
missing trailing newline, or a final row with the wrong field count; header compared against the
previous file of the same kind (added / removed / reordered columns detected at the boundary);
sha256 of every file kept so the same payload under two names is obvious.

`expected_drop(kind, file_date, due_sim, due_cycle, arrived_sim, arrived_cycle, status)` is the
lateness ledger — see §11 for why it records *cycles*, not days.

---

## 9. Conformed layer and data tests (`pipeline/transform.py`)

`dim_customer` (SCD2, `is_current`, with `city` demojibaked and `city_raw` retained), `dim_product`
(with `category_conformed`), `fct_orders` (order ⋈ payment ⋈ label, with `is_near_dup`/`dup_of`,
`has_label`, `refunded`, `label_knowledge_time`, `window_closed_at`), `fct_payments`, `agg_daily`,
and a `conformance_rules(rule, key, value, created_sim)` table so learned repairs (unit switch,
category rename mapping) are themselves data.

Repairs here are explicitly **conformance, not concealment**: every one is also reported by a monitor,
and `src_*` still holds the delivered form.

**Incremental by partition.** Only order-days that actually changed are rebuilt. The late-arriving-
dimension trap is documented as a bug that was hit and fixed: the order fact was only rebuilt for
partitions dirtied by *orders*, so a label settling today never refreshed the order it belonged to —
matured label coverage read 22 % instead of 100 % and the apparent refund rate 44 % instead of 17 %.
Late labels and late payments now mark their **order's** partition dirty. "The data was correct
throughout — only the conformed layer was stale."

`Transformer.test()` is a hand-rolled dbt-tests equivalent: a local `check(name, ok, observed, detail)`
closure writing to `test_results`, partition-scoped every cycle (not-null keys, completeness ratio,
closed-domain membership, referential integrity ratio, non-negative amounts, `knowledge_time` not in
the future) and whole-table every 50 cycles (payment uniqueness, orphan rate, **SCD2 non-overlap**,
label booleanness). Thresholds are ratios, not absolutes, where the baseline is legitimately noisy.

---

## 10. The monitors (`pipeline/monitors.py`, 926 lines — the heart)

17 detector methods, run inside `Monitors.run(ctx)` where `ctx` is one cycle's evidence bundle
(`transport`, `norm`, `drop`, `count_gaps`, `old_restatements`, `late_event_days`, `domain_values`,
`near_dup_orders`, `test_failures`). Each is wrapped in try/except: **a monitor must never stop the loop**.

Three families, by what they rest on:

| Family | Fires on | Examples |
|---|---|---|
| **deterministic** | one observation that cannot happen by chance | truncated file, header added/reordered, encoding change, compression-vs-extension mismatch, byte-identical duplicate rows, value outside a closed domain, timestamp offset in a naive feed, sentinel values, mojibake, identifier width violation, two files for one content day |
| **reconciliation** | a disagreement with the source | rows we hold that the source no longer serves, rows appearing for settled past days, version bumps on old rows |
| **statistical** | a distribution moved more than growth/seasonality can explain — deliberately slow | unit switch (≥20× move in a median that is flat by design), covariate shift by PSI, MNAR by outcome rate across missingness, label maturation by coverage across order-age cohorts, hourly-profile anomalies vs a 28-day trailing profile |

Supporting primitives, all hand-written: `robust_baseline` (median + MAD, excluding the most recent
points), `psi(a, b, bins)`, `auc(scores, labels)`, `_dst_boundaries(year)` / `near_dst(day)` (last
Sunday of March and October).

**Distinguishing confusable pairs was most of the design work**, and is tabulated in `NOTES.md` §6:
`category_rename` vs `new_category_late` (did an established value *stop* at the same moment?);
`late_partition` vs `missing_delivery` (arrived after being observed absent vs still absent 4+ days);
`silent_row_deletion` vs `survivorship_customers` (which entity lost the rows); `clock_skew` vs
`late_arriving_events` (minutes and all timestamps and `knowledge_time < occurred_at`, vs days and
specific partitions); `id_reuse` vs `category_rename` (immutable attributes of a few keys vs one
categorical across the whole feed); `field_rename_midstream` vs `leakage_column`.

**Undocumented domains are learned**, not assumed: `LEARN_DAYS = 90` before any claim,
`LEARNED_MIN_SHARE = 2 %` and ≥10 occurrences so a rare-but-legitimate value cannot look like a fault.

---

## 11. Detection discipline — the most transferable part

1. **Specificity over sensitivity, stated as a budget.** An unmatched claim costs a mark and buys
   nothing, so detectors are built on facts that *cannot* happen by chance rather than thresholds over
   noisy series. The documented baseline (weekday seasonality, 18 % annual growth, Q4 peak, Black
   Friday at 3×, catalogue turnover, autocorrelated demand shocks) is treated as a list of things a
   detector must *not* fire on.
2. **Detection must happen in the loop.** MTTD is scored, so every monitor runs inside the cycle and
   appends the instant it fires. Nothing is reconstructed at the end.
3. **One line per named fault.** `IncidentLog.raise_incident` dedupes on `key or fault or
   f"family:{family}"`; a repeat increments `n_repeats` in the warehouse and appends nothing.
   "A fault reported five times is not five detections, and repeated claims can only add false positives."
4. **Lateness is counted in cycles the pipeline actually watched, not elapsed days.** Two false
   positives were caused by the naive version: files already in the drop at first start looked late
   (*we* were late), and a restart gap made an on-time file look overdue. `expected_drop.due_cycle`
   fixes both. The warehouse was reset and the run restarted so no hand-edited entries exist.
5. **Retractions are a first-class, auditable act.** Four claims were withdrawn into
   `retractions.jsonl` *with reasons*. The rule held: **an entry is removed only when the detector that
   produced it is found mis-specified and is fixed in the same change** — never because it turned out
   not to match, and nothing was ever added after the fact.
   - `dst_duplicate_hour`: a night hour holding 1.5 % of a ~500-event day is empty by chance ~1 in
     2000, and the horizon has ~50,000 hour-checks → ~25 such hours expected from Poisson noise alone.
     The detector was generating false positives *by construction*. Narrowed to days within one day of
     a European DST boundary.
   - `id_reuse`: the evidence was four payment ids with two `(order_id, amount)` pairs — same order,
     same customer, same processor, only the amount revised five weeks later. That is a **restatement
     in the drop channel**, already reported as `backfill_restatement`. Narrowed to require the id to
     point at a *different* order or customer. The investigation also exposed a real defect (stale
     `cur_payments`).
   - `covariate_shift` (×2): PSI computed over the **union of levels** let a newly arrived category
     (`bnpl`, already reported five weeks earlier as `new_category_late`) contribute a near-zero
     denominator and dominate the statistic. Over the common support the same comparison gives 0.0003.
     Two lessons, the second being the general one: **a detector that re-reports another detector's
     finding under a second name is not extra coverage — it is a guaranteed false positive** whenever
     the second name is not the one that was drawn. Aggregate-statistic detectors must be told about
     the events their deterministic siblings already own.
6. **A coverage table written so gaps are explicit** (`NOTES.md` §13): all 30 catalogue faults × the
   detector that would catch it × the signal × *the risk of missing it*. Honest summary: 22 are
   deterministic/reconciliation-based, 5 rest on thresholds calibrated without a clean baseline, 3
   have a structural sampling or ambiguity gap left open on purpose because closing it would cost more
   in false positives than it buys.
7. **A self-inflicted incident recorded too**: a retraction script rewrote `incidents.jsonl` with a
   literal `"\\n"` separator, collapsing the log to one line. Caught in three minutes, recovered.
   "The lesson is not about the escape. It is that the scored deliverable was being edited by ad-hoc
   scripts with no read-back check."

---

## 12. Point-in-time ML (`pipeline/ml.py`, `evaluate.py`, `score.py`)

`HistGradientBoostingClassifier` on refund risk, retrained every 60 simulated days, scoring each order
once as it lands. Two things handled explicitly:

- **Censoring.** A non-refunded order has *no label row at all* until its 30-day window closes.
  Training on "no label = not refunded" would teach the model that recent orders are never refunded.
  Training is restricted to orders whose window closed **and** which carry a label row.
- **Point-in-time features.** `featurize()` computes customer history from prior orders only, and
  counts only the labels whose `label_knowledge_time <= order.occurred_at` — a refund that settled
  afterwards cannot inform the prediction for the order it followed. Loading is itself as-of:
  `SELECT … FROM fct_orders WHERE … AND knowledge_time <= ?`.
- **A univariate AUC leakage screen at every retrain**: any single numeric feature with
  `max(auc, 1-auc) > 0.90` is treated as leakage, **raised as an incident**, and dropped from the model.
  (It has dropped nothing, which is the correct outcome if `leakage_column` was not drawn — and the
  note says so honestly, including that it would be a miss if it was.)
- `lifetime_value_cents` is kept despite the vendor documenting it as a decoy: "letting the model
  discover it is uninformative is more honest than pre-filtering on the vendor's say-so".
- Models are pickled per version with their `cat_levels` and feature list; unseen categorical levels
  map to `-1` at scoring time.
- **`evaluate.py` is prospective, not a holdout self-report**: it scores each order with the number
  that was *actually written* at ingest by whichever model was live then, and compares it to the label
  that arrived weeks later. Hand-rolled `auc()` (rank-based, tie-aware) and `ece()` (expected
  calibration error, equal-count bins).
- Observed in flight: holdout AUC 0.66–0.70 against a base rate drifting 0.17 → 0.15; decile
  calibration monotonic, over-confident in the top decile (predicted 0.39, actual 0.28) — diagnosed as
  the usual GBM tail over-separation, with isotonic calibration named as the fix.

---

## 13. Loop economics and unattended operation

Per-cycle cost decides feasibility at 2200 cycles. Measured ~1.9–2.2 s/cycle; the dominant term was
the **1.6 s drop-writer delay**, pure dead time, now overlapped by running the wait in a background
thread while the API channel is extracted (`start_drop_watch` — and note that only the filesystem is
touched from the thread; nothing is parsed or written until the main thread joins).

Other cost controls: rotating reconciliation buckets; audit of 2 entities per day rather than 4;
partition-scoped transforms; full test suite every 50 cycles.

**Unattended-operation mechanics**
- **The clock is advanced by the runner, not by the cycle**, so a cycle that fails is retried against
  the *same* simulated day rather than skipping one (`_retry_pending`).
- `advance_sampled(days, steps=3)` splits the day advance into thirds and probes availability at each
  step, because a compressed one-touch-per-day loop would step straight over a multi-hour outage
  window that an hourly pipeline would have sampled 24 times. Two of three 503 probes raises
  `sustained_outage` — an explicitly acknowledged structural sampling gap, stated rather than hidden.
- A sustained outage is *also* raised from the exception handler in `run()`, because a cycle that dies
  on the transport never reaches its monitors and the fault would otherwise never be reported.
- `ensure_source_up()` distinguishes the two failure modes the exercise conflates: **a multi-hour 503
  is a fault to be logged; a dead process is an operating problem to be fixed.** It checks whether the
  port is free before restarting the vendor process.
- `loop.sh` retries `run.py run` up to 200 times, relying on idempotency; every write is either
  `INSERT OR IGNORE` into an append-only table or a version-guarded upsert.

---

## 14. Testing

`tests/test_detectors.py` and `tests/test_drop_to_monitor.py` — no pytest fixtures, no framework
ceremony: a module-level `check(name, cond, detail)` accumulating results, driving the **deterministic**
detectors against synthetic corruption built in a `tempfile` directory (a healthy gzipped file, a
gzip stream cut mid-row, a plain truncation, a `.gz` name with plain bytes, encoding changes, header
edits, mojibake, id widths). The statistical detectors are deliberately not unit-tested — they are the
part `NOTES.md` calls the weakest.

`merge_drop_evidence` is a static method rather than four inline lines, with the reason stated:
"those four lines are the wiring, the wiring is what failed once already, and wiring you cannot call
from a test is wiring you are not testing."

---

## 15. Idioms worth extracting

1. **Bitemporality as the foundation, not a feature.** `src_*` (append-only, versioned, with the
   source's `knowledge_time`) + `cur_*` (incremental current belief). "What did we believe on date X"
   becomes one `WHERE knowledge_time <= X`. The retrospective note is unambiguous: *this should have
   been the first thing built, not the second.*
2. **Tombstones, never deletes.** A row the source stops serving gets `deleted_sim` set, so historical
   facts still join and the disappearance is itself queryable.
3. **Landing zone = raw bytes, exactly as delivered, never rewritten.** Everything else is derived.
4. **Push all transport hostility into one module** with a per-cycle stats object, so the rest of the
   code reads as if the source were well-behaved.
5. **Sniff, don't trust**: compression from magic bytes, encoding by attempting the strict decode,
   page completeness from a real pull rather than a reported count.
6. **Repair-but-record** as a normalisation contract, with the repair counter *being* the evidence.
7. **Idempotency as an architectural property**, which is what makes a 200-attempt restart loop safe.
8. **Retry semantics differ by question**: `get_json` retries a 503 for 17 s; `probe()` does not,
   because availability sampling and data extraction are different questions.
9. **Dedupe the log, count the repeats in the warehouse.**
10. **Measure the source before designing against it.** `NOTES.md` §2 is a table of nine empirical
    findings (page cap 500, ~2.7 % 429 rate, token TTL ~988 s, drop-writer latency ~1.6 s, no file for
    `sim_start`, `customer_id` is 7 zero-padded digits, consecutive catalogue snapshots are frequently
    byte-identical) each with its design consequence. The last one alone prevented a guaranteed false
    positive in `duplicate_delivery`.
11. **Write the notes during the run, not after**, and record the mistakes at full length — including
    the ones caused by your own tooling.
12. **Censoring and point-in-time correctness are engineering problems, not modelling footnotes.**

## 16. Known weaknesses (self-identified)

- The statistical detectors are calibrated against a baseline described in *prose* rather than measured
  over a quiet period, because the run and the calibration had to happen simultaneously. A two-pass
  design (characterise the first 200 days, then arm the thresholds) is named as the better build.
- `sustained_outage` detection samples the simulated day three times, not 24 — a short outage window
  can fall between probes.
- `dup_near_orders`, `mnar_missing`, `clock_skew`, `covariate_shift` rest on blind thresholds.
- A duplicated *catalogue* delivery is only caught by the same-date rule, because content-hash dedup
  must exclude the catalogue (consecutive snapshots are legitimately identical).
- A one-cycle-delayed drop ingest would hide the writer latency completely, at the cost of more
  complicated lateness accounting — judged not worth it at this horizon.
- Top-decile probability calibration needs an isotonic layer.
