# Build notes

Working notes for the autonomous baseline run: what was built, why it was built
that way, what was measured about the source before deciding, and what I would
do differently. Written during the run, not after it.

---

## 1. What the exercise actually is

The pipeline is the vehicle; `incidents.jsonl` is the deliverable. That framing
drove almost every decision below. Concretely it means:

- **Specificity beats sensitivity.** An unmatched claim is a false positive. A
  detector that fires on the documented baseline (weekday seasonality, 18%
  annual growth, Q4 peak, Black Friday at 3x, catalogue turnover, autocorrelated
  demand shocks) costs a mark and buys nothing. So most detectors are built on
  facts that *cannot* happen by chance — a value outside a closed domain, a
  truncated gzip stream, a timestamp offset in a naive feed — rather than on
  thresholds over noisy series.
- **Detection must happen in the loop.** MTTD is scored, so every monitor runs
  inside the cycle and appends the moment it fires. Nothing is reconstructed at
  the end.
- **The warehouse exists to make detection possible.** Bitemporal storage is not
  decoration here: half the catalogue (restatements, late arrivals, silent
  deletion, censoring) is only visible if you can compare what the source says
  now with what it said before.

---

## 2. What I measured before designing anything

Guessing at the vendor's behaviour would have been the expensive mistake, so
these were established empirically first:

| Question | Finding | Consequence |
|---|---|---|
| Max page size | `limit` is capped at **500**; asking for 1000 silently returns 500 | `PAGE_LIMIT = 500`, and never trust a requested limit |
| Ambient error rate | ~2.7% of requests return 429, sometimes with `Retry-After: 1` | retry with backoff; budget for it in the cycle-time estimate |
| Latency | p50 10 ms, p95 13 ms on localhost | the loop is bound by the drop writer, not the API |
| Auth | token TTL ~988 real seconds (≈6 simulated hours) | re-auth on 401 mid-pull is mandatory, not optional |
| Error shape | `GET /v1/labels` returned `{"error": "internal_error"}` with no 5xx | a 200 body has to be validated, not assumed |
| Drop timing | files appear **~1.6 s after** the advance that crosses the boundary; crossing into day D+1 writes the file *for day D* | the wait is real and had to be overlapped |
| Drop bootstrap | no file is ever written for `sim_start` (2026-01-01) | never report it as a missing delivery |
| Identifier shape | `customer_id` is a 7-char zero-padded string (`0000191`) | gives `id_leading_zeros` a precise signature |
| Catalogue files | consecutive daily snapshots are frequently **byte-identical** | content-hash duplicate detection must exclude the catalogue |

That last one would have been a guaranteed false positive: `duplicate_delivery`
looks exactly like a quiet catalogue day if you only compare sha256.

---

## 3. Architecture

```
run.py            CLI: bootstrap / cycle N / run / status / tests
asof.py           historical-belief reconstruction and restatement diffing
pipeline/
  config.py       everything environmental, incl. the two keys read from lab.toml
  vendor.py       HTTP client: auth, retry, encoding sniffing, cursor validation
  normalize.py    schema-on-read, repair-but-record
  extract_api.py  landing, bitemporal load, count reconciliation, audits
  extract_drop.py the file channel, decoded from the bytes rather than the name
  storage.py      SQLite warehouse: src_* (append-only) + cur_* (current belief)
  transform.py    conformed models and the data tests over them
  monitors.py     the detectors
  incidents.py    the scored log
  ml.py           point-in-time refund risk
  orchestrator.py the cycle
landing/          raw bytes and raw JSON, exactly as delivered
warehouse/wh.db   the warehouse
```

**Why SQLite.** No DuckDB in the environment, and the working set (≈250k orders,
≈1.2M events over the horizon) fits comfortably. One file, crash-safe, no
service to babysit for an unattended multi-hour run. Everything the models need
is an indexed lookup on a day column.

**Two layers, deliberately.** `src_*` is append-only, one row per
(business key, version) ever served, carrying the source's `knowledge_time` and
our `first_seen_sim`. `cur_*` is current belief, maintained incrementally by a
version-guarded upsert. Rebuilding "latest version per key" from history on
every cycle would have been the thing that made 2200 cycles unaffordable; this
way each cycle is O(rows that changed).

**Idempotency.** Every write is either `INSERT OR IGNORE` into an append-only
table or an upsert guarded by `WHERE excluded.version > current.version`. A
cycle can be re-run after a crash without duplicating or losing a row, which is
what makes the loop safe to leave alone.

---

## 4. Extraction strategy

The API filters valid time (`since`/`until`) and knowledge time (`as_of`), but
there is no way to ask "what became knowable since I last looked". That is the
central problem, and it is solved with four mechanisms rather than one:

1. **Rolling** — re-pull the last 3–5 valid-time days every cycle. Catches
   ordinary late arrivals and fresh restatements cheaply.
2. **Count reconciliation** — ask the source for `total_count` over a few coarse
   time buckets and compare with our own count. One request per bucket. A
   mismatch is bisected (4-way) down to the offending day, which is then
   re-pulled. This is how rows appearing far in the past and rows *disappearing*
   are found without re-reading six years of history every hour.
3. **Rotating deep audit** — a full re-pull of individual historical days,
   weighted towards the 15–75 day band where "revised weeks after the fact"
   lands. Catches restatements, which change no count and are therefore
   invisible to reconciliation.
4. **Bootstrap** — one full walk of every endpoint at the start.

`total_count` is used only as a *change hint*; every conclusion drawn from it is
confirmed by a real pull before it reaches the warehouse. That keeps the design
honest at hostility 3, where the count is documented to lie, while still getting
the enormous cost saving at hostility 1.

**Cost.** Buckets rotate (recent every cycle, deep history every 2/3/5 cycles)
and the audit covers 2 entities per day rather than 4. That holds a cycle to
about 20 requests instead of about 50, which matters because ~3% of requests
cost a ~1 s `Retry-After` sleep.

**The customer dimension predates the horizon.** Most customers have a
`valid_from` in 2024–2025, so anything keyed on `sim_start` misses nearly the
whole dimension. The first build of this got that wrong: 100% of orders had no
customer row and the referential-integrity test failed on every cycle. Fixed by
extracting and reconciling customers from `DIM_HISTORY_START = 2018-01-01`.

---

## 5. Normalisation: repair, but record

The rule is that nothing is ever silently dropped or silently fixed.

- Unknown fields go to `extra_json` and raise a schema-drift signal. A field
  thrown away is a fault you cannot see.
- A renamed field is followed by name similarity (shared token + sequence ratio)
  so downstream models keep working — **and** the mapping is reported.
- An identifier that lost its leading zeros is re-padded to the modal width so
  the join survives, and the repair is counted, because the repair *is* the
  evidence.
- Sentinels (`-999`, `1900-01-01`, `N/A`) become NULL in the conformed layer and
  are reported. Bare `null`/`""`/`"None"` deliberately do **not** count as
  sentinels: those are how a source says "nothing", and at hostility 2 they are
  also how the *transport* says it. Treating them as a data fault would be a
  false positive waiting to happen.
- Mojibake detection requires the *pair* (`Ã` + high byte). A lone accented
  capital occurs in genuine European text — `Ãland` is a real place — but `Ã³`
  is not. Verified against a list of real and corrupted city names.

---

## 6. The monitors

Grouped by what they rest on:

**Deterministic** (fire on one observation): truncated file, header added /
reordered, encoding change, compression-vs-extension mismatch, byte-identical
duplicate payment rows, value outside a closed domain, timestamp offsets in a
naive feed, sentinel values, mojibake, identifier width violation, two files for
one content day.

**Reconciliation** (fire on a disagreement with the source): rows we hold that
the source no longer serves (tombstoned, never deleted — historical facts must
still join), rows appearing for settled past days, version bumps on old rows.

**Statistical** (trailing baseline, robust threshold, deliberately slow): unit
switch (≥20x move in a daily median that the flat-basket-value design says
cannot happen), covariate shift by PSI comparing like-for-like periods, MNAR by
comparing outcome rates across a column's missingness, label maturation by
comparing label coverage across order-age cohorts, hourly-profile anomalies
against a 28-day trailing profile with the European DST boundary dates known.

**Distinguishing the confusable pairs** was most of the design work:

| Pair | How they are told apart |
|---|---|
| `category_rename` vs `new_category_late` | rename = a novel value appears *while* an established one stops; new-late = novel value appears with every previous value still present |
| `late_partition` vs `missing_delivery` | late = arrives after being observed absent; missing = still absent 4+ days later |
| `silent_row_deletion` vs `survivorship_customers` | which entity lost the rows |
| `clock_skew` vs `late_arriving_events` | skew = minutes, affects all timestamps, and shows as `knowledge_time < occurred_at`; late arrival = days, affects specific partitions |
| `id_reuse` vs `category_rename` | id_reuse changes *immutable* attributes of a few keys (signup date, product name); rename changes one categorical value across the whole feed |
| `field_rename_midstream` vs `leakage_column` | rename = one field disappears as another appears; leakage = a field that separates the outcome almost perfectly |

**Deduplication policy: one line per named fault.** A fault reported five times
is not five detections, and repeated claims can only add false positives. The
warehouse counts repeats; the log does not repeat.

---

## 7. Two false positives caught before the run proper

Both are worth recording because both were *my* bug, not the vendor's.

1. **`late_partition` on the pipeline's own backlog.** Files already sitting in
   the drop when the pipeline first started looked one day late. They were not:
   we were late, not the vendor.
2. **`late_partition` across a restart gap.** While the loop was stopped, the
   clock kept moving; on resume, a day's file that had been written on time
   looked overdue.

The fix for both is the same idea, and it is the right one generally: lateness
is measured in **cycles the pipeline actually watched the day be missing**, not
in elapsed simulated days. `expected_drop` records the cycle at which a day
became due; a day we never had the chance to observe as missing cannot be
reported late. The warehouse was reset and the run restarted cleanly after this,
so no reconstructed or hand-edited entries exist in `incidents.jsonl`.

---

## 8. Model

Refund risk per order, `HistGradientBoostingClassifier`, retrained every 60
simulated days, scoring each order once as it lands.

Two things are handled explicitly rather than by convention:

- **Censoring.** A non-refunded order has no label row at all until its 30-day
  window closes. Training on "no label = not refunded" would teach the model
  that recent orders are never refunded. Training is restricted to orders whose
  window has closed *and* which carry a label row; everything younger is
  undetermined and excluded.
- **Point-in-time features.** Customer history counts only prior orders and only
  the labels whose `knowledge_time` preceded the order being scored, so a refund
  that settled afterwards cannot inform the prediction for it.

A univariate AUC screen runs at every retrain: any single feature separating the
outcome better than 0.90 is treated as leakage, reported as an incident, and
dropped from the model.

`lifetime_value_cents` is kept as a feature even though the vendor documentation
warns it is a decoy for spend — it is cheap, and letting the model discover it
is uninformative is more honest than pre-filtering on the vendor's say-so.

**Bug found and fixed here:** the order fact was only rebuilt for partitions
dirtied by *orders*, so a label settling today never refreshed the order it
belonged to. Matured label coverage read 22% when it should have read 100%, and
the apparent refund rate was 44% instead of 17%. Late-arriving labels and
payments now mark their order's partition dirty. This is the classic
late-arriving-dimension trap and it is worth noting that the *data* was correct
throughout — only the conformed layer was stale.

---

## 9. Loop economics

At one simulated day per cycle the run is 2200 cycles, so per-cycle cost
decides whether the exercise is feasible at all.

- Measured ~1.9–2.2 s per cycle early on.
- The dominant term was the **1.6 s drop-writer delay**, which is dead time. It
  now runs in a background thread while the API channel is extracted, so the two
  overlap.
- Reconciliation buckets rotate rather than all firing every cycle.
- Transform is partition-scoped; the full test suite runs every 50 cycles and
  the partition tests every cycle.

Estimated total wall clock: 1.5–2.5 hours, growing modestly as the tables do.
`STEP_DAYS` is configurable; it stayed at 1 because the protocol says one day
per cycle is the truest baseline and the run fits in the time either way.

---

## 10. What I would do differently

- **Bitemporal storage should have been the first thing built, not the second.**
  Everything else fell out of it easily; the parts I built before it (the
  extractor's window logic) needed revisiting once it existed.
- **The customer-dimension history bug should have been caught by design, not by
  a failing test.** I keyed everything to `sim_start` without checking whether
  the dimension predated the world. Ten seconds of `curl` would have shown it —
  and in fact the very first probe I ran *did* show `valid_from: 2024-02-02`.
  I had the evidence and did not use it.
- **A one-cycle-delayed drop ingest** would hide the writer latency completely
  rather than partially, at the cost of more complicated lateness accounting. If
  the horizon were 10x longer that trade would be worth making.
- **The statistical detectors are the weakest part.** They are calibrated
  against a baseline described in prose rather than one measured over a long
  quiet period, because the run and the calibration had to happen at the same
  time. A two-pass design — characterise the first 200 days, then arm the
  thresholds — would be better and is what I would build with more time.

---

## 11. Retractions during the run

Two claims were written by monitors and then withdrawn. Both were my detector
being wrong, not the vendor behaving well, and both are recorded verbatim with
their reason in `retractions.jsonl` so the edit is auditable rather than quiet.

The rule I held myself to: **an entry is removed only when the detector that
produced it is found to be mis-specified and is fixed in the same change.** No
entry was ever removed because it turned out not to match, and nothing was ever
added after the fact — that would be exactly the backfilled log the exercise
warns against.

**1. `dst_duplicate_hour` on 2026-08-04.** The hourly-profile monitor fired on a
night hour that happened to contain no events. August 4th is not near a DST
boundary. The arithmetic: a night hour holding 1.5% of a ~500-event day expects
7.5 events, so it is empty by chance about once in 2000 — and the horizon
contains ~50,000 hour-checks, so ~25 such empty hours are *expected* from
Poisson noise alone. The detector was generating false positives by
construction. Corrected to claim only on days within one day of a European DST
boundary, where the named fault can actually live; anomalies on ordinary days
are still recorded as alerts.

**2. `id_reuse` on 2026-12-12.** The evidence was four payment ids that each had
two distinct `(order_id, amount_cents)` pairs. Looking at the rows:

```
PAY-00000020  file 2026-01-02  ORD-00000021  0005951  33388  swiftpay  Besançon
PAY-00000020  file 2026-02-06  ORD-00000021  0005951  33905  swiftpay  Besançon
```

Same payment, same order, same customer, same `paid_ts`, same processor — only
the amount revised, re-delivered five weeks later. That is a **restatement in
the drop channel**, the same signature already seen on `orders.order_total_cents`
and already reported as `backfill_restatement`. It is not identifier reuse. The
detector now requires the id to point at a *different order or customer*, which
is what reuse actually means.

That investigation also exposed a real defect in the conformed layer: the
payments upsert kept the *first* delivery of a payment and only incremented a
delivery counter, so 375 revised amounts were sitting stale in `cur_payments`.
Current belief now takes the newer delivery, `src_payments` keeps every version
as delivered, and a re-delivered payment marks its order's partition dirty. The
existing rows were repaired and the facts rebuilt.

**The general lesson**, and the one I would carry into the next build: *every*
channel restates, not just the versioned one. The API tells you it has restated
by incrementing `version`; the drop channel tells you nothing at all, and the
only way to see it is to keep every delivered row and compare. Having built the
bitemporal layer for the API and a plain upsert for the drop, I had exactly the
blind spot that design implies — and it first surfaced disguised as a different
fault entirely.

## 12. Model behaviour observed in flight

Retrained every 60 simulated days on point-in-time data. Holdout AUC settles
around 0.66–0.70 against a base rate that drifts down from 0.17 to 0.15. Decile
calibration is monotonic and conservative in the middle, over-confident in the
top decile (predicted 0.39, actual 0.28) — the usual gradient-boosting
over-separation at the tail, which isotonic calibration on a held-out slice
would fix and which I would add given more time.

The leakage screen has dropped nothing so far, which is the correct outcome if
`leakage_column` was not drawn, and would be a miss if it was. It only inspects
features the pipeline itself constructs; a leaking column arriving as a *new
field* on the orders feed is caught by the schema-drift monitor first, and the
screen would then see it once it entered the feature set.

---

## 13. Coverage: every catalogue fault, and what would catch it

Written so the gaps are explicit rather than implied. "Signal" is what the
pipeline would actually observe; "risk" is why it might not.

| Fault | Detector | Signal | Risk of missing it |
|---|---|---|---|
| `dup_exact_payments` | drop loader | identical row hashes inside one file | none — deterministic |
| `dup_near_orders` | order fact | same customer + identical basket + identical total, ≤300 s apart | a single-line basket repeated by one customer could be genuine; needs ≥2 lines or ≥3 pairs |
| `mojibake_city` | normaliser | `Ã`+high-byte pair in any text field | none — verified against real accented city names |
| `id_leading_zeros` | normaliser | `customer_id` not 7 digits | none — the width is fixed |
| `field_rename_midstream` | normaliser | one expected field missing while an unknown one appears | a rename to a name too dissimilar to match still shows as missing+new |
| `tz_mixed` | normaliser | any timestamp with an offset in a naive feed | none |
| `late_arriving_events` | rolling window + reconciliation | events materialising for a settled day; ≥10 rows at age >3 | baseline is exactly zero for events, so specific |
| `dst_duplicate_hour` | hourly profile | missing/doubled hour vs 28-day profile, on a DST boundary day only | if the lab puts the switch on a non-European date it is only an alert |
| `unit_switch_price` | daily medians | ≥20x step in a median that is flat by design | none if it is a real unit change |
| `category_rename` | closed domains | novel value appears as an established one stops | none for documented domains |
| `new_category_late` | closed domains | novel value, every old value still present | as above |
| `sentinel_values` | normaliser | `-999`, `1900-01-01`, `N/A` and friends | deliberately excludes bare null spellings, so a sentinel of literally `"null"` is missed on purpose |
| `covariate_shift` | PSI, like-for-like periods | PSI > 0.4 on a feature growth cannot move | threshold set blind; a subtle shift is missed |
| `mnar_missing` | outcome rate by missingness | ≥50% relative gap, n≥120 | needs the column to actually go missing |
| `label_censoring` | maturation cohorts | label coverage by order age | none — always on, and detected on day 46 |
| `leakage_column` | new-field fill rate by order age + univariate AUC screen | present on mature orders, absent on fresh ones | a leaking column that is *always* populated is only caught by the AUC screen |
| `survivorship_customers` | count reconciliation | source count for a past day drops below ours | none — the whole dimension is reconciled, pre-horizon included |
| `backfill_restatement` | version bumps + drop re-delivery | higher version, later knowledge time, old valid time | none — always on, detected on day 65 |
| `late_partition` | expected-vs-arrived | file arrives in a later cycle than the one that watched it become due | none, now that lateness is counted in cycles watched |
| `missing_delivery` | expected-vs-arrived | still pending 4+ days later | none |
| `duplicate_delivery` | content hash / two files per day | payments sha256 collision, or two files for one date | catalogue snapshots excluded (they are legitimately identical), so a duplicated *catalogue* delivery is only caught by the same-date rule |
| `partial_write` | drop reader | short gzip stream, or a final row with the wrong field count | none |
| `column_added` | header diff | new column at a boundary | none |
| `column_reordered` | header diff | same set, different order | none |
| `encoding_change` | drop reader | body is not valid UTF-8 | none |
| `compression_change` | magic number vs extension | disagreement either way | none |
| `clock_skew` | knowledge-lag series | `knowledge_time < occurred_at`, or a minutes-scale step in the median lag | the weakest statistical detector; a small skew inside normal lag variance is missed |
| `sustained_outage` | availability probes | 503 at ≥2 of 3 probes spread across the simulated day | the honest risk: a compressed loop samples the day a few times, not 24; a short window can still fall between probes |
| `silent_row_deletion` | count reconciliation | source count for a settled day drops | deep history rotates every 5 cycles, so detection can lag a few days — well inside the 60-day window |
| `id_reuse` | immutable attributes | signup date + country both change; product name changes; payment id points at a different order | narrowed deliberately after it false-positived on payment restatement |

**Honest summary:** 22 of the 30 are deterministic or reconciliation-based and
should not be missed if they fire. Five (`covariate_shift`, `mnar_missing`,
`clock_skew`, `dup_near_orders`, `dst_duplicate_hour`) rest on thresholds
calibrated without a clean baseline period. Three (`sustained_outage`,
`leakage_column`, `duplicate_delivery` on the catalogue) have a structural
sampling or ambiguity gap that I chose not to close because closing it would
have cost more in false positives than it bought in detections.

**3. `covariate_shift` on 2031-03-16.** Fired with PSI(payment_method) = 1.06.
The cause was arithmetic, not drift: the divergence was computed over the union
of levels, so the arrival of a new payment method — `bnpl`, which the
closed-domain monitor had already reported five weeks earlier as
`new_category_late` — contributed a term with a near-zero denominator and
dominated the statistic. Measured over the common support the same comparison
gives **0.0003**, and the other two categoricals give 0.0004 and 0.0002. The
shared distribution had not moved at all; renormalised over the four original
methods, `card` goes 0.592 → 0.587.

Two lessons, and the second is the one worth keeping:

- A population-stability metric must be computed on the common support. A new
  level is a *schema* event, not a distribution shift, and it belongs to a
  different detector.
- More generally: **one underlying event was about to be claimed twice under two
  different fault names.** With eleven faults drawn from thirty, a detector that
  re-reports another detector's finding under a second name is not extra
  coverage — it is a guaranteed false positive whenever the second name is not
  the one that was drawn. Detectors that key off aggregate statistics need to be
  told explicitly about the events their deterministic siblings already own.

**A self-inflicted one worth recording.** The script that performed the third
retraction rewrote `incidents.jsonl` with `"\\n"` as the record separator — a
literal backslash-n, not a newline — collapsing the whole log onto one line. It
was caught within three minutes, on the next read, and all ten records were
recovered by splitting on both the real and the literal separator; the file was
rewritten sorted, deduplicated by fault, and every line re-validated as JSON.

The lesson is not about the escape. It is that **the scored deliverable was
being edited by ad-hoc scripts with no read-back check.** The retraction path
deserved the same care as the write path: parse-after-write, verify the record
count, verify the dedupe table still agrees with the file. It now does, but only
because it broke first.

---

## 14. Final state

The loop ran to the end of the horizon unattended: **2200.0 of 2200 simulated
days**, 2147 cycles at one simulated day each, exit code 0.

| | |
|---|---|
| orders / events / customers | 146,665 / 733,326 / 48,634 current; 147,129 / 733,326 / 48,710 versions held |
| payments / products | 141,485 deduplicated from 144,277 delivered rows; 400 SKUs |
| drop files ingested | 4,397 |
| orders scored | 143,936 |
| data tests | 14,862 pass / 335 fail |
| incidents logged | 9, all at `high` confidence |
| retractions | 4, all recorded with reasons |

**The 335 test failures are the pipeline working, not failing.** Every one is
`fct_orders.method_domain` from 2031-02-07 onwards: the accepted-values test
rejecting `bnpl`, which is exactly the signal that produced the
`new_category_late` incident. Leaving the test red was the right call — the
domain genuinely did change and silently widening the allow-list would have
thrown the evidence away. In a production repo this is where you would branch
the model on a dated domain version.

**829 customer dimension rows were tombstoned** by reconciliation, and **zero
orders were orphaned** as a result, because a row the source stops serving is
marked deleted rather than removed. That is the whole reason for keeping
`survivorship_customers` recoverable: the facts still join to the dimension row
that existed when they were recorded.

Against the stated success criteria:

- full extraction of every endpoint at the configured hostility — yes, bootstrap
  walked all four, and the final row counts match the world the source
  documents (≈150k orders, ≈145k payments, ≈730k events, ≈50k customers);
- incremental extraction repeatedly without loss or duplication — yes; every
  write is idempotent and every partition is reconciled against the source's own
  counts;
- both channels including the drop — yes, 4,397 files;
- past belief reconstructable and demonstrably different from today's — yes,
  `asof.py diff` shows restated keys with both beliefs and the fields that moved;
- conformed models over both channels with tests — yes;
- a scheduled loop running unattended with monitoring that fires on its own —
  yes; every incident in the log was written by a monitor inside the loop;
- refund risk per order trained only on what was knowable at prediction time —
  yes, 143,936 orders scored, holdout AUC ≈ 0.69;
- `incidents.jsonl` maintained throughout — yes, and never backfilled.

The honest weakness of the run is visible in the shape of the log: **nine
claims, every one of them at `high` confidence, and eight of the nine are
deterministic or reconciliation-based.** The statistical detectors contributed
exactly one (`label_censoring`, which is structural and always on) and produced
all four of the retractions. If faults like `clock_skew`, `mnar_missing`,
`dup_near_orders` or `covariate_shift` were drawn in this run, this pipeline
most likely did not see them. That is the thing I would fix first with more
time, and §10 says how: characterise a long quiet baseline period before arming
any threshold, rather than calibrating and detecting simultaneously.

---

## 15. Post-mortem, after `--reveal`

The ledger was unsealed at the operator's explicit request *after* the run had
finished and been graded. Everything below is post-hoc: the two code fixes in
this section did not exist during the run and did not contribute to the score.

**Result: 9 of 11 drawn faults detected, all 9 named correctly, 0 false
positives. MTTD 13.5 simulated days mean, 1.1 median, 64.9 worst.**

The mean is carried entirely by the two always-on faults, which cannot be
detected before enough history exists to see them: `label_censoring` at +45.9
days (you need cohorts on both sides of the 30-day window) and
`backfill_restatement` at +64.9 days (you need a row old enough to have been
revised "weeks after the fact"). The seven episodic faults were caught at +0.3,
+0.5, +0.7, +1.0, +1.1, +1.7 and +5.3 days — that is, essentially the next
cycle, except `missing_delivery`, which by design waits four days before
concluding a file is never coming.

### The two misses, and why §13 was wrong about them

In §13 I predicted the weakness would be in the five detectors resting on
thresholds I had calibrated without a clean baseline. **That prediction was
wrong.** Both misses were deterministic detectors that could not have failed on
statistical grounds, and the statistical detectors I distrusted missed nothing.
My distrust was aimed correctly at *false positives* — they produced all four
retractions — and entirely missed the false-negative risk, which was living in
the code I trusted most.

**`id_leading_zeros`** (fired 2031-02-09, drop/payments, `customer_id` emitted
as int64). The detector ran. It counted 68 violations in the very first affected
file and kept counting for eleven months. It re-padded every id so the joins
survived. And nothing ever heard about it: `DropIngestor.ingest` builds its own
`NormStats`, hands it back as `findings["norm"]`, and the orchestrator's monitor
context `ctx["norm"]` was built from the four *API* entities only. The evidence
was collected, acted on, and dropped on the floor.

The repair was so effective it erased the symptom from the warehouse. Querying
`src_payments` for `LENGTH(customer_id) <> 7` returns **zero rows** across the
whole run; the raw landing zone still shows `PAY-00108580,ORD-00112559,319,...`.
That gap between the landing zone and the warehouse is the entire fault, and it
was invisible from the warehouse alone.

> **The lesson, and it is the main one from this run.** "Repair, but record"
> (§5) is only two thirds of a principle. It has to be *repair, record, and
> route*. A repair that succeeds silently is indistinguishable from no fault
> having occurred — and a pipeline that heals cleanly is, for exactly that
> reason, the one most likely to hide what it is supposed to report. The better
> the ingestion, the louder the reporting has to be.

**`dst_duplicate_hour`** (fired 2031-03-30, 52 orders piled into the 02:00
hour). Right calendar, right boundary day, right idea, wrong table: the monitor
read hourly counts from `cur_events`, and the fault was injected into
`cur_orders`. Events that day were entirely normal — 24 hours present, median 20
per hour — so there was nothing to see where I was looking. A second, quieter
bug sat behind it: the expected-share floor of 1.5% would have skipped hour 02
even if the monitor *had* read orders, because 02:00 is a quiet hour on an
ordinary day. That floor was protecting against the very first false positive I
had to retract, and in fixing that I over-corrected into a blind spot.

### Both fixed, both regression-tested

- The drop channel's `NormStats` is now merged into the monitor context, so
  re-padded ids, mojibake, sentinels and timestamp offsets from the file channel
  reach the same detectors as the API's.
- The hourly profile runs over orders *and* events, and judges a pile-up without
  the expected-share floor — a civil-time transition repeats into a quiet hour,
  so requiring that hour to be busy filters out the signal by construction.

Verified against the real run data: with the fix, `id_leading_zeros` fires with
the exact evidence (`{'payments.customer_id': 68}`) and is silent under the
pre-fix context; `dst_duplicate_hour` fires on 2031-03-30 reporting *hour 02
doubled (expected 0.2%, saw 57.1%)*, and stays silent on an ordinary day and on
a DST day with no fault. `tests/test_detectors.py` covers both, 28/28 passing.

### What I would tell the next builder

1. **Test the wiring, not just the component.** My suite tested
   `id_leading_zeros` at the normaliser and it passed — while the fault went
   undetected for eleven simulated months. The unit test and the production path
   disagreed and nothing checked that they agreed.
2. **Every detector should be asked "which tables does this fault live in?"**,
   not "which table am I already reading?". One of my two misses is that
   question never being asked.
3. **Reconcile the landing zone against the warehouse.** A periodic check that
   raw values still resemble stored values would have caught `id_leading_zeros`
   independently of any detector, because the two disagreed on every row for
   eleven months.
4. The retraction discipline was worth it. Four claims were withdrawn, all four
   after finding the detector was wrong rather than after finding the claim
   unmatched, and the final result has zero false positives. But three of the
   four came from statistics re-reporting an event a deterministic detector
   already owned — with 11 faults drawn from 30, aggregate detectors need to be
   told explicitly what their deterministic siblings have already claimed.

---

## 16. Does the refund model actually predict?

Evaluated with `evaluate.py`, which ignores the training holdout (a self-report)
and scores every order by the number that was *written when it was ingested*, by
whichever model was live at that moment, against the outcome that arrived weeks
later. 141,362 orders qualify: scored, window closed, label served.

| | |
|---|---|
| prospective AUC | **0.691** |
| ECE (calibration error) | **0.008** |
| Brier | 0.1083 vs 0.1140 for predicting the base rate — **5.0% skill** |
| lift @ top 10% | **2.2x** (22% of all refunds in 10% of orders) |
| base rate tracked | 0.150 → 0.124 over the horizon; mean prediction 0.141 → 0.129 |

**Ranking: good. Strength: modest. Calibration: good except the tail.** Deciles
track actuals within a percentage point everywhere except the top, which is
over-confident (predicted 0.322, actual 0.291) — the usual gradient-boosting
tail over-separation. Isotonic calibration on a held-out slice fixes it and is
the single highest-value thing left undone.

**No detectable leakage.** Across 35 retrains, mean(holdout AUC − realised AUC)
is +0.0037 with sd 0.016 — within noise. A leaking feature makes a model's own
holdout sit systematically above what it achieves in production; this one does
not, which is independent evidence that the point-in-time discipline held.

**The uncomfortable finding: most of the signal is basket value.**

```
refund rate by order-value decile:  2.7% → 25.8%, monotone
log(order value) alone, same rows:  AUC 0.665
served model, same rows:            AUC 0.693
4-feature logistic:                 AUC 0.663
categoricals (target-rate encoded): 0.50–0.56
```

The whole apparatus — customer history, web activity, processor, category,
timing, SCD2 attributes — buys about **+0.03 AUC** over a single monotone
function of order value. And customer history is not what it appears: orders
from customers with three or more prior known refunds refund at 10.5%, against
17.8% for customers with no prior order at all. That feature is a *tenure* proxy
pointing the opposite way to intuition, not a propensity signal.

So the honest reading is that this world's refund process is close to
`f(basket value) + noise`, and 0.69 is not far below the ceiling for the
features available. That is worth stating plainly because the opposite
conclusion — "the model is weak, add more features" — is the expensive one, and
the evidence here points away from it. The cheap wins are calibration of the top
decile and backfilling the 2,729 orders that were ingested before any model
existed and were never scored at all.

**Caveat on the caveat:** this is an argument from three converging measurements
(flat categoricals, a confounded history effect, value-alone within 0.03 of the
tuned model), not a proof. A genuinely different feature — sequence of events
within the session, say, or basket composition rather than basket size — could
still be sitting there unexploited.
