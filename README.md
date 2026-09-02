# refund-lab pipeline

Consumer side of the refund lab: ingests a badly-behaved vendor's HTTP API and
daily file drop, stores both bitemporally, conforms and tests them, scores
refund risk per order, and keeps an incident log of everything the vendor did
wrong — unattended, on a schedule, for a 2200-day simulated horizon.

Design decisions and rationale are in [`NOTES.md`](NOTES.md).

## Running it

```bash
./loop.sh                       # unattended to the horizon, restarts on failure
python run.py bootstrap         # full extraction of every endpoint, once
python run.py cycle 10          # ten pipeline increments
python run.py status            # where everything has got to
python run.py tests             # the full data-test suite now
python -m pytest tests/         # detector unit tests (synthetic corruption)
```

## Asking what the source used to say

The warehouse keeps every version the source ever served, so past belief is a
query, not an archive restore.

```bash
python asof.py order ORD-00000873 --as-of 2026-02-01T00:00:00
python asof.py order ORD-00000873                      # ... and what it says now
python asof.py day 2027-03-01 --entity orders --as-of 2027-03-05T00:00:00
python asof.py diff                                    # keys whose story changed
```

`asof.py diff` prints, for every restated key, both beliefs and the fields that
moved between them.

## Serving the score

```bash
python score.py ORD-00012345      # the score as it was served, with its inputs
python score.py --day 2027-04-02  # a day's orders ranked by risk
python score.py --summary         # model history and decile calibration
```

## Layout

| Path | What it is |
|---|---|
| `pipeline/` | the pipeline itself; see `NOTES.md` §3 |
| `landing/` | raw bytes and raw JSON exactly as delivered, never rewritten |
| `warehouse/wh.db` | `src_*` append-only history, `cur_*` current belief, `dim_*`/`fct_*`/`agg_*` conformed models |
| `incidents.jsonl` | the incident log, appended as monitors fire |
| `retractions.jsonl` | claims withdrawn after a detector was found mis-specified, with reasons |
| `logs/` | pipeline log and the unattended run's stdout |
| `models/` | pickled refund-risk models, one per retrain |

## Operational notes

- Every write is idempotent, so a cycle interrupted anywhere can simply be
  re-run; `loop.sh` does exactly that.
- The clock is advanced by the runner, not by the cycle, so a cycle that fails
  is retried against the same simulated day rather than skipping one.
- If the source process disappears entirely the runner restarts it; a source
  that is up but answering 503 is treated as a fault and logged, not restarted.
