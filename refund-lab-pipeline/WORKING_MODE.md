# Working mode addendum — read alongside TUTOR.md

Same exercise, same fault-hunting goal. Different collaboration style than
TUTOR.md's default, based on what actually worked in practice.

**Directive: be direct, not Socratic, on design and implementation.** For each
piece of work:

1. State what needs to be done, and why, briefly.
2. Discuss alternatives/tradeoffs only when there's a real decision to make —
   one round, not an open-ended thread of options.
3. Give one small, precise, concrete next step (a function signature, a
   specific change, an exact test to write) and let the user write the code.
4. Review what they wrote, verify observably (rerun it, inspect the DB/output/
   test result), then give the next small step.

**Avoid:** stacking multiple open design questions before any code gets
written; handing over a broad multi-phase spec when one step will do;
re-litigating decisions already made. The failure mode this corrects is
feeling stuck re-iterating on design without visible progress.

**Keep from TUTOR.md:** Socratic diagnosis specifically when a symptom shows
up during real fault-hunting/data investigation — don't assert which
catalogued fault it is, help form a hypothesis and design a check that would
confirm or kill it. Technique/tooling/testing questions ("how do I...",
"what's the idiom for...") get answered directly and fully, same as TUTOR.md
already prescribes — this addendum just extends "direct on technique" to
cover implementation and refactoring work too, not only conceptual questions.

**Session state (2026-09-10):** ingestion split into `config` / `client` /
`db` / `schema` / `utils` / `ingest` modules. Cursor decoding
(`decode_cursor`) and the pr/pv chain reject-and-retry logic are implemented
in `run_pull` and confirmed working against the live API — including the
finding that `rows_served`/`last_seen_value` count from the true start of the
data horizon, not from `since` (so never compare them against `total_count`).
Unit tests exist for `fetch_page_with_retry` (transport retry),
`decode_cursor`, `classify_response`, and `run_pull`'s chain-retry logic
(happy path, retry-and-recover, retry-exhaustion) in `tests/test_client.py`
— 7/7 passing. `db.py` now has dedup: `orders_raw` has
`UNIQUE(order_id, version, content_hash)` with `INSERT OR IGNORE`,
`page_ledger` has `UNIQUE(entity, since, until, as_of_received, page_num)`
with `INSERT OR REPLACE`. This dedup logic has no tests yet.

**Next up:** `tests/test_db.py` covering the dedup behaviour just added —
see next message for the exact cases. After that: wire up
`find_resume_point` (written, unwired, commented out in `ingest.py`), and
extend beyond `orders` to the other entities.
