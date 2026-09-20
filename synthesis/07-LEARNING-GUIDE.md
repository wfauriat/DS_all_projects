# A learner's guide to data engineering, using four codebases as pretexts

This document explains the *ideas*. The four projects in this repository are used as examples — as
pretexts — because each one had to solve a real version of a general problem, and you can go and read
the solution afterwards.

It assumes you can program, and that you have seen a database and an HTTP request before. It does not
assume you know what a watermark, a slowly-changing dimension, a dead-letter queue, or a data contract
is. Where a term matters, it is explained the first time it appears. Where it is only a name for
something, it is explained once and then used.

A companion document, **[`08-TOOLS-AND-CONCEPTS.md`](08-TOOLS-AND-CONCEPTS.md)**, is a long index: for
each tool ("what is dbt?") and each term ("what is idempotency?"), a plain-language entry saying what
it is, what problem it solves, and where in these four projects to see it working.

---

## Contents

**Part 0 — [Four worlds](#part-0--four-worlds)**

**Part 1 — [Seven ideas that everything else rests on](#part-1--seven-ideas-that-everything-else-rests-on)**
1.1 A pipeline is a chain of promises · 1.2 Time is not one thing · 1.3 Keep the original ·
1.4 Doing it twice must be safe · 1.5 Where a program remembers where it got to ·
1.6 Truth changes: belief, restatement and history · 1.7 What "data quality" actually means

**Part 2 — [Walking the pipeline](#part-2--walking-the-pipeline)**
2.1 Getting the data in · 2.2 Making sense of it · 2.3 Keeping it · 2.4 Reshaping it ·
2.5 Making it happen by itself · 2.6 Streaming, the special case · 2.7 Watching it ·
2.8 Learning from it · 2.9 Giving it to people

**Part 3 — [The craft around the data](#part-3--the-craft-around-the-data)**
3.1 Configuration · 3.2 Environments and packaging · 3.3 Testing · 3.4 Version control, CI/CD and
infrastructure · 3.5 Cost and running unattended · 3.6 Writing it down

**Part 4 — [Twenty things worth remembering](#part-4--twenty-things-worth-remembering)**

---

# Part 0 — Four worlds

Four projects. Each one is a complete little world, and each one puts the difficulty somewhere
different. That is why reading all four teaches more than reading four versions of the same thing.

### World 1 — `DE_pipeline_stream`: the live feed

A city bike-share operator publishes a continuous feed of everything happening in its network: a bike
leaves a station, a bike arrives, every station reports how many bikes it has every five minutes. The
feed never stops. You want to watch it in real time, store all of it, and — this is the interesting
part — catch the moments when the feed is *wrong*: a station reporting more bikes than it has docks, a
trip that ends impossibly far from where it started, a station that quietly stops reporting.

The operator here is fake — it is a simulator written for the project — which means the project *knows
the right answer* and can grade its own detection. That is rare and extremely useful.

**Where the difficulty lives:** in the plumbing. Seven different technologies have to be wired
together correctly, and the project's explicit goal is that the wiring be readable.

### World 2 — `mediaElection27`: the public product

You want to know how often each candidate in the 2027 French presidential election is mentioned in the
national press. Four times a day, fetch the headline feeds of about fifteen newspapers, store the
articles, look for candidate names in the titles, and publish an interactive dashboard anyone can open
in a browser.

It is genuinely deployed on Amazon Web Services and genuinely public. And there is a hard rule: the
whole thing must cost **less than five dollars a month**.

**Where the difficulty lives:** in the deployment and the cost. That single constraint reshapes the
entire architecture, in a way you can trace step by step.

### World 3 — `energy_predML`: the model that has to keep working

Predict how much electricity a US grid region will consume in a given hour. Fetch real historical
demand from a government API, build features, train a model, serve predictions over an API, and then
— the part most tutorials skip — *keep noticing whether the model is still any good*, and retrain it
automatically when it is not.

**Where the difficulty lives:** in the machine-learning lifecycle, and especially in not accidentally
cheating (using information you would not have had at prediction time).

### World 4 — `refund-lab-autom`: the supplier who lies

A supplier sends you order data over an API and payment data as daily files. The supplier is
deliberately badly behaved: it renames fields without warning, truncates files, changes character
encodings, restates old records, silently deletes rows, goes down for hours. Eleven specific
misbehaviours were drawn at random from a published catalogue of thirty and scheduled at unknown
points across six simulated years — and **the list is sealed**. Nobody knows which ones were drawn.

Your job is to run unattended for the whole six years and produce one file: a log of every
misbehaviour you detected, with the time you detected it. You are scored on how many you found, how
fast, and **how many things you claimed that were not real**.

**Where the difficulty lives:** in the data itself, and in the discipline of not crying wolf.

---

# Part 1 — Seven ideas that everything else rests on

## 1.1 A pipeline is a chain of promises

Draw any data pipeline and you get boxes and arrows:

```
something produces data  →  you fetch it  →  you keep it  →  you reshape it  →  someone uses it
```

The boxes are the easy part. **The arrows are where the work is**, because every arrow is a promise,
and in a real system every promise is eventually broken. The network drops. The process is killed
mid-write. The supplier sends the same file twice. The disk fills up.

So the first habit to build is: for each arrow, *name the promise*. There are only a few possibilities,
and they have standard names:

| Promise | Meaning | What it costs you |
|---|---|---|
| **at most once** | you might lose data, you will never duplicate it | data loss |
| **at least once** | you will never lose data, you might duplicate it | you must be able to remove duplicates downstream |
| **exactly once** | neither loss nor duplication | machinery: transactions, stored positions, or both |
| **best effort** | no promise at all | fine for logs, never for money |

Almost everything in practice is **at-least-once plus deduplication**, because it is cheap and it
degrades gracefully. Exactly-once is achievable, but only at specific hops and only by construction —
§1.5 explains how.

`DE_pipeline_stream` writes this down as a table in its documentation, hop by hop, and it is the single
most useful page in the project:

```
source → bridge          at least once (resume from a saved position)
bridge → Kafka           at least once (save the position only after delivery is confirmed)
Kafka → Spark → alerts   at least once (each alert carries a deterministic id, so repeats are visible)
Kafka → Spark → files    exactly once  (the file writer keeps its own commit log)
Kafka → warehouse        exactly once  (the position is written in the same transaction as the rows)
warehouse → marts        idempotent    (rebuilding produces the same result)
marts → serving copy     atomic        (build a new file, then swap it in one instruction)
```

Notice that the promises are *different at different hops*, and that is normal and correct. You do not
need exactly-once everywhere. You need to know where you have it and where you do not.

> **Jargon check.** *Hop* just means "one arrow" — one transfer of data from one component to the next.

---

## 1.2 Time is not one thing

This is the idea that separates people who have been burned from people who have not.

Take a single bike trip. It has at least five different timestamps attached to it, and they are all
legitimate, and they all mean something different:

| Name | When it is | Who sets it |
|---|---|---|
| **event time** | when the bike actually left the station | the world (here: the simulator) |
| **emission time** | when the operator's system published the event | the source |
| **ingestion time** | when your first component received it | your ingestion process |
| **landing time** | when it was written into your database | your loading process |
| **processing time** | when some computation looked at it | whatever computed it |

And in `refund-lab-autom`, which deals with a supplier that *changes its mind*, there is a sixth
distinction that is even more important:

| Name | Meaning |
|---|---|
| **valid time** | when the fact was true in the world ("this order was placed on 3 March") |
| **knowledge time** | when the source knew it, or knew this version of it ("we learned the corrected total on 8 April") |

Why this matters, concretely. Three questions that look the same and are not:

- *"How many orders were placed on 3 March?"* — valid time.
- *"How many 3-March orders did we know about on 5 March?"* — valid time filtered by knowledge time.
- *"How many rows did we load on 5 March?"* — landing time.

If your table has one column called `date`, you cannot answer all three, and worse, you will answer the
wrong one without noticing.

**Three concrete consequences that appear across these projects:**

1. **Business logic uses event time.** "Trips per hour of day" is about when trips happened, not when
   you heard about them. *(`DE_pipeline_stream`'s analytics models.)*

2. **Incremental loading uses landing time.** If you only reprocess rows whose *event time* is recent,
   a record that arrived three hours late will never be reprocessed — you will miss it forever. If you
   reprocess rows whose *landing time* is recent, late data is picked up automatically. This one line
   is the difference between a correct pipeline and a quietly lossy one:

   ```sql
   -- in DE_pipeline_stream's fct_trips model
   where loaded_at > (select max(loaded_at) from {{ this }})
   --    ^^^^^^^^^ landing time, NOT event time
   ```

3. **"What day is it" is a question with a timezone in it.** `mediaElection27` groups articles by day,
   and a "day" for a French newspaper is a Paris day, not a UTC day. So the timezone is a *parameter of
   the query*:

   ```sql
   date_trunc('day', published_at AT TIME ZONE :tz)::date AS day
   ```

**Three of the four projects had a dated bug caused by getting a clock wrong**, and all three wrote it
down. In `DE_pipeline_stream`, station snapshots were stamped with the moment they were *scheduled*
rather than the moment their values were *read* — a ten-second difference — and it made a downstream
quality check produce 1,002 false alarms instead of 238. In `refund-lab-autom`, "how late is this
file?" was measured in elapsed days rather than in *cycles during which the pipeline was actually
watching*, which made files look late when in fact the pipeline had been stopped.

> **Habit to build:** for every timestamp column you create, write in a comment whose clock it is on
> and what it is for. It costs ten seconds and saves days.

---

## 1.3 Keep the original

When data arrives, you are tempted to parse it, clean it, and store the clean version. Don't — or
rather, do, but keep the dirty version too.

The pattern has a name: **ELT** (Extract, Load, Transform) as opposed to **ETL** (Extract, Transform,
Load). The difference is one letter and a large change in philosophy:

- **ETL**: clean it, *then* store it. Storage is expensive, so store only what you need.
- **ELT**: store it exactly as it arrived, *then* clean it inside the database. Storage is cheap now,
  and the transformation is code you can re-run.

ELT is the modern default, and the reason is not storage prices. It is this:

> **Schema drift should break your model, never your ingestion.**

If a supplier renames a field, an ELT pipeline still lands every row; only the transformation fails,
loudly, and you fix the transformation and re-run it. An ETL pipeline drops the rows, or crashes, and
the data is gone.

Look at how far `DE_pipeline_stream` takes this. Its landing table looks like:

```sql
CREATE TABLE raw.trip_events (
    topic       VARCHAR, "partition" INTEGER, "offset" BIGINT,  -- where it came from
    msg_key     VARCHAR, msg_ts TIMESTAMPTZ, headers VARCHAR,   -- metadata
    value       VARCHAR NOT NULL,   -- the message itself, as TEXT
    batch_id    BIGINT, loaded_at TIMESTAMPTZ,
    PRIMARY KEY (topic, "partition", "offset")
);
```

The comment in the source is the lesson: the message is kept as **text**, not as the database's JSON
type, *"so that a malformed message can never block landing"*. Even the act of checking "is this valid
JSON?" is deferred.

All four projects do a version of this:

| Project | What is kept, untouched |
|---|---|
| `DE_pipeline_stream` | the exact bytes the source published, forwarded through the message queue and into the database as text |
| `mediaElection27` | the raw feed XML in cloud storage, *and* the whole parsed entry in a JSON column |
| `energy_predML` | the raw API response as a timestamped JSON file |
| `refund-lab-autom` | the raw bytes of every delivered file and every API page, in a `landing/` directory that is *never rewritten* |

The three things this buys you:

1. **Replay.** When you fix a parsing bug, you can re-derive everything from the original without
   asking the supplier for the data again (which they may not still have).
2. **Debugging.** "The number is wrong" becomes answerable: you can look at what actually arrived.
3. **Evidence.** In `refund-lab-autom`, keeping every delivered row — *including duplicates* — is what
   makes duplicate detection possible at all. The duplicate *is* the finding.

---

## 1.4 Doing it twice must be safe

An operation is **idempotent** if doing it twice leaves the world exactly as doing it once did.
Pressing a lift call button is idempotent. Adding sugar to coffee is not.

Why you need it: everything retries. The network drops mid-request and the client retries. A scheduled
task fails and is re-run. A machine is killed and something restarts. A person runs the script twice
because they weren't sure the first one worked. If "run it again" can corrupt your data, you cannot
operate the system, and you certainly cannot leave it running unattended.

Idempotency is not a property you hope for. It is a property you *design in*, and there are only a few
techniques:

### Technique 1 — a natural key plus a conflict rule

A **natural key** is the combination of columns that identifies a row in the real world — not an
auto-generated id, but something meaningful. Then you tell the database what to do on collision.

```sql
-- mediaElection27: an article is identified by (which feed, the feed's own id)
INSERT INTO articles (source_id, guid, ...) VALUES (...)
ON CONFLICT (source_id, guid) DO NOTHING
RETURNING id;
```

The `RETURNING id` matters: if a row comes back, you inserted; if nothing comes back, it was already
there. That difference is itself a metric — it tells you how much of what the feed served you was new.

```sql
-- energy_predML: a demand reading is identified by (hour, region)
INSERT INTO demand SELECT ... FROM incoming i
WHERE NOT EXISTS (SELECT 1 FROM demand d WHERE d.timestamp = i.timestamp AND d.region = i.region);
```

### Technique 2 — a version guard

When rows *change*, "do nothing on conflict" is wrong — you want the newer version to win, and only
the newer one.

```sql
-- refund-lab-autom
INSERT INTO cur_orders (...) VALUES (...)
ON CONFLICT (order_id) DO UPDATE SET ... 
WHERE excluded.version > cur_orders.version;
--    ^^^^^^^^ "excluded" is the row you tried to insert
```

Re-delivering an old version does nothing. Re-delivering the current version does nothing. Only a
genuinely newer version writes. You can replay the whole feed and end in the same state.

### Technique 3 — replace the whole thing

For small reference tables that change in place, don't try to be clever. Delete and re-insert inside a
transaction. `DE_pipeline_stream` does this for its station and bike lists; `mediaElection27` does it
for candidate aliases, and the reason is worth noting: **deleting all of a candidate's aliases and
re-inserting from the config file means that removing a line from the file removes it from the
database.** With an upsert-only approach, deleted config lines would live on forever.

### Technique 4 — build it elsewhere, then swap atomically

For files, the technique is:

```python
build_everything_into(path + ".tmp")
os.replace(path + ".tmp", path)      # one atomic filesystem operation
```

`os.replace` is atomic on every operating system that matters. A reader either sees the old file or
the new one; never half of each. And on Linux, a reader who already has the old file open **keeps
reading the old file** until it closes — the old data stays alive as long as someone is using it.

`DE_pipeline_stream` uses this three separate times: publishing the reporting database, saving the
simulator's state, and saving the ingestion checkpoint. Each time the comment says the same thing:
*"atomic: a crash never leaves a half-written file"*.

### Technique 5 — one transaction

A **transaction** is a group of database operations that either all happen or none do. This is the
oldest idea in databases and it is still the most useful one. §1.5 shows the single most valuable
application of it.

---

## 1.5 Where a program remembers where it got to

Any process that consumes a stream of things has to remember its position, or it will either miss
things or re-read them forever. The position has different names in different contexts — a
**checkpoint**, an **offset**, a **cursor**, a **high-water mark** — but it is always the same idea.

The interesting question is not *what* you store, it is **where**, and this is where most bugs live.

`DE_pipeline_stream` has three different consumers of the same message stream, and deliberately gives
each one a different answer, so you can compare:

| Consumer | Where it stores its position | Consequence |
|---|---|---|
| the bridge (feed → queue) | a small JSON file on disk, written **only after** the queue confirms delivery | if it crashes, it resumes slightly *behind* where it was → duplicates, never loss |
| the stream processor | its own checkpoint directory (positions + internal state) | managed by the framework |
| the warehouse loader | **a table in the warehouse, written in the same transaction as the data** | exactly once |

That third one deserves unpacking, because it is the cleanest example of a general technique.

### Exactly-once, explained

You cannot have exactly-once *delivery* over a network — that is a genuine impossibility result. What
you can have is exactly-once **effect**, and the trick is to make "record the data" and "record that I
have consumed the data" a single indivisible act.

`DE_pipeline_stream`'s loader does this (the comment in the source is the whole argument):

```
1. start:  for each partition, read the position stored IN THE WAREHOUSE
2. end:    for each partition, snapshot the current end of the queue
           → this bounds the run: messages arriving during it simply wait for the next run
3. read:   consume [start, end)
4. write:  ONE transaction:  the rows  +  the new positions  +  an audit row
5. show:   only after the commit, also report the position to the queue —
           purely so a monitoring UI can display consumer lag; never used to resume
```

Why it works, in one sentence: if you crash before step 4 commits, *neither* the rows nor the position
moved, and the next run re-reads exactly the same slice; if you crash after, *both* moved.

There is no window in which they can disagree, because a transaction has no inside.

And a second line of defence, because belt and braces is correct here: the table has a primary key on
`(queue, partition, position)`, so even a bug that tried to insert the same message twice would fail
loudly rather than silently duplicating.

> They tested this by killing the scheduler process in the middle of a load. No row lost, none
> duplicated. **A guarantee you have not tried to break is a hope, not a guarantee.**

### The trap nobody expects

Positions are *relative to a specific stream*. If a stream is deleted and recreated with the same name,
it starts numbering from zero again — and your stored position, say 4,194, now points past the end of
a brand-new stream. Your loader will silently skip everything below 4,194 and never tell you.

`DE_pipeline_stream` hit this and wrote the general rule:

> **Resetting a job means resetting its progress *and* its outputs, together.**

Its reset command deletes the processor's checkpoints, the files it wrote, the stream it published to,
*and* the warehouse rows that came from that stream — in one transaction, and only after the queue
confirms the old stream is really gone.

---

## 1.6 Truth changes: belief, restatement and history

Here is a scenario. On 3 March, your supplier tells you order ORD-873 was €334. On 6 February of the
following year, they send it again, and now it says €339.

What do you do?

The naive answer is `UPDATE orders SET total = 339 WHERE id = 'ORD-873'`. And now:

- You cannot answer "what did we think this order was worth in March?"
- You cannot explain why last quarter's revenue report no longer reproduces.
- You cannot tell whether the supplier corrects one order a year or ten thousand.
- A model trained in April used €334; you can no longer reconstruct what it saw.

The information was not wrong. It was **superseded**. And "superseded" is not the same as "wrong", and
it is certainly not the same as "never existed".

### The technique: keep both axes

`refund-lab-autom` builds its entire warehouse around this, with two layers per entity:

```
src_orders     APPEND ONLY.  One row per (order_id, version) ever served.
               Carries the source's own knowledge_time and the time we first saw it.
               Never updated. Never deleted.

cur_orders     Current belief. One row per order_id.
               Maintained by an upsert guarded by "WHERE excluded.version > current.version".
```

This is called **bitemporal** storage — two time axes: when a thing was true (valid time) and when you
knew it (knowledge time). The word sounds academic; the payoff is completely concrete:

```
"What did the source say about this order, as of 1 February 2026?"
  → SELECT ... FROM src_orders
    WHERE order_id = ? AND knowledge_time <= '2026-02-01'
    AND version = (the highest version satisfying that)
```

Past belief becomes **a query**, not an archive restore. That one line is the whole of the project's
`asof.py` tool, which can show you what was believed on any date and diff the two beliefs.

### Why this is not optional in that project

Roughly half of the supplier misbehaviours `refund-lab-autom` is graded on are *invisible* without it:

- a record silently revised weeks later (no count changes, no error, nothing);
- rows appearing for a day you already processed;
- rows *disappearing* from the source;
- labels that only materialise once a waiting period expires.

You cannot detect "this changed" if you did not keep what it was before.

### Two hard-won lessons from that project

**First: build it early.** The retrospective is blunt — *"Bitemporal storage should have been the first
thing built, not the second. Everything else fell out of it easily; the parts I built before it needed
revisiting once it existed."*

**Second, and more subtle: every channel restates, not just the one that tells you.** The API announces
a correction by incrementing a version number. The file drop announces nothing at all — a corrected
payment simply reappears in a later file. Having built the versioned store for the API and a plain
overwrite for the files, the project had exactly the blind spot that design implies: 375 corrected
payment amounts sat stale, and the bug first surfaced *disguised as a completely different fault*
(it looked like identifier reuse). Taking a day to work that out produced the general rule.

### A related idea: never delete, tombstone

When the source stops serving a row you already hold, the tempting move is to delete your copy. Don't.
Mark it: `deleted_sim = <the time we noticed>`. Now:

- historical facts that referenced it still join (your old reports still work);
- the disappearance is itself queryable — and in `refund-lab-autom`, "rows the source stopped serving"
  is one of the things you are graded on detecting.

A **tombstone** is a marker that says "this used to exist here". The concept appears everywhere from
distributed databases to filesystems, for the same reason: deletion destroys the evidence of deletion.

### The everyday version of the same idea: SCD2

You will not always need full bitemporality. But you will very often need the small version of it,
which has an ugly name: **slowly changing dimension, type 2** — SCD2.

The scenario, from `DE_pipeline_stream`: a bike station has 36 docks. Six months later it is expanded
to 40. A quality check asks "did this station ever report more bikes than it has docks?"

- If you overwrite the capacity, you compare *old readings* against *today's capacity*, and a reading
  of 38 bikes from last year looks fine when it was actually impossible.
- If you keep history, you compare each reading against the capacity that was valid *at that moment*.

SCD2 keeps one row per *version* of the thing, each with a validity window:

| station_id | capacity | valid_from | valid_to |
|---|---|---|---|
| ST-024 | 36 | 2025-03-01 | 2026-05-14 |
| ST-024 | 40 | 2026-05-14 | 9999-12-31 |

And then the check is a join with a time condition — an **as-of join**:

```sql
join snap_stations v
  on  v.station_id = s.station_id
  and s.event_time >= v.valid_from
  and s.event_time <  v.valid_to
where s.bikes_available > v.capacity
```

One small design detail worth copying: the current row's `valid_to` is `9999-12-31`, not `NULL`. With
`NULL` you need `(valid_to IS NULL OR event_time < valid_to)` everywhere; with a far-future date, "was
valid at time t" is a plain range comparison, and range comparisons can use an index.

> **The comparison this sets up.** In `DE_pipeline_stream` the same check exists twice: the real-time
> processor looks up each station's capacity from a table it refreshes every minute, while the batch
> layer does the as-of join. Live, they agree. But when the project replayed its whole history through
> the real-time path, the real-time version *missed* four genuine violations, because it judged
> year-old readings against today's larger capacity. The batch version, using as-of joins, got them
> all. That is a beautifully concrete demonstration of why history matters.

---

## 1.7 What "data quality" actually means

"Data quality" is a phrase that means nothing until you break it into questions. Here is a workable
breakdown, with the kind of check that answers each.

| Question | Name people use | Example check |
|---|---|---|
| Is it here at all? | **completeness / freshness** | "the most recent row landed less than 15 minutes ago" |
| Is it shaped the way we agreed? | **schema / validity** | "every record has these fields, with these types" |
| Are the values individually possible? | **domain / range checks** | "bikes available ≥ 0"; "payment method is one of these four" |
| Do the pieces agree with each other? | **referential integrity / reconciliation** | "every order references a customer that exists"; "our count for that day matches the source's" |
| Is it plausible, given what we've seen? | **statistical / drift** | "today's median order total is not twenty times yesterday's" |
| Has the *meaning* changed? | **semantic drift** | "a category we've never seen appeared, while an old one stopped" |

Four practical points that these projects make better than most explanations.

### Point 1 — checks belong at different places, and that is deliberate

`DE_pipeline_stream` has an explicit rule:

> **Syntax at ingestion, semantics downstream.**

- A station reporting **−3 bikes** is a *shape* violation. It can be judged from the record alone. It
  is rejected at the front door.
- A station reporting **45 bikes when it has 40 docks** is a *meaning* violation. You need the station's
  capacity to know, so it cannot be judged at the front door. It is caught later, where reference data
  is available.

Getting this wrong in either direction is costly: check too much at the door and you need to load
reference data into your ingestion path (slow, fragile, and now your ingestion depends on another
system being up); check too little and bad records poison everything downstream.

### Point 2 — a check is only useful if someone acts on it

And a check that fires falsely trains people to ignore it. So false alarms are not merely annoying;
they actively destroy the value of the checks that *are* correct.

`refund-lab-autom` is the extreme case, because it is literally scored on this: a claim that matches
no real fault is a **false positive** and costs a mark. The resulting discipline is stated up front:

> **Specificity beats sensitivity.** A detector that fires on the documented normal behaviour
> (weekday seasonality, 18% annual growth, a Q4 peak, Black Friday at three times normal volume)
> costs a mark and buys nothing. So most detectors are built on facts that **cannot happen by
> chance** — a value outside a closed set, a truncated compressed file, a timestamp carrying a
> timezone offset in a feed where every other timestamp is naive — rather than on thresholds over
> noisy series.

That is a rule you can carry into any alerting system: **prefer checks whose false-positive rate is
zero by construction over checks that need a threshold.**

### Point 3 — you can measure how good your checks are

If you know the right answer, "did we catch it?" becomes measurable. Two standard measures, in plain
language:

- **Precision** — of the things you flagged, what fraction were real? *(Are you crying wolf?)*
- **Recall** — of the real things, what fraction did you flag? *(Are you missing things?)*

They trade off. A detector that flags everything has perfect recall and terrible precision. One that
flags nothing has perfect precision (vacuously) and zero recall.

`DE_pipeline_stream` builds a whole reporting table for this, because its simulated source records
every fault it injected. The table has one row per (check × fault it targets) and gives precision,
recall, and the counts behind them. Reading it is genuinely instructive — for example:

| fault | precision | recall | what it tells you |
|---|---|---|---|
| over-capacity report | 100% | 100% | a deterministic check on a closed rule; nothing to tune |
| teleporting trip | 100% | 98% | ditto, the 2% are edge cases |
| frozen station (real-time) | 86% | 40% | a windowed *heuristic*: confident when it fires, misses a lot |
| frozen station (batch) | — | 100% | the exact sequential check finds all of them, but later |
| orphan trip | 66% | 100% | catches everything, but a third of its "false" alarms have another cause |

Two refinements in that table are worth stealing:

**"Explained by another fault."** Some of the orphan-trip check's apparent false positives were not
mistakes at all: the missing half of the trip had been rejected at the front door because of a
*different* injected fault. So one fault showed up disguised as another. Rather than hiding this, the
report has a column for it. **A false positive caused by a real, different problem is a finding, not
noise.**

**A judging horizon.** A fault injected thirty seconds ago has not been missed; it simply hasn't been
detected *yet*. The report only judges faults older than a threshold, and only up to the point where
the ground-truth record is itself complete. Without this bound one check appeared 82% precise; with
it, 100%. **Deciding what you are entitled to judge is part of measuring.**

### Point 4 — telling similar problems apart is most of the work

This is `refund-lab-autom`'s deepest lesson. Detecting *that* something is wrong is often easy;
saying *what* is wrong is the hard part, and getting it wrong is a false positive.

| These two look the same | The thing that distinguishes them |
|---|---|
| a category was renamed / a genuinely new category appeared | did an **established value stop** at the same moment? |
| a file arrived late / a file never arrived | did it arrive after you **observed** it missing, or is it still missing days later? |
| rows were deleted from orders / rows were deleted from customers | which entity lost them |
| clocks are skewed / events are arriving late | skew is **minutes** and affects **all** timestamps; late arrival is **days** and affects **specific** days |
| an identifier was reused / a category was renamed | reuse changes **immutable** attributes of a few keys; a rename changes one field across the whole feed |

The project also learned the inverse rule the hard way, and it is the single most transferable
sentence in that repository:

> **A detector that re-reports another detector's finding under a second name is not extra coverage —
> it is a guaranteed false positive** whenever the second name is not the right one.

What happened: a statistical detector flagged "the distribution of payment methods has shifted". It
had, arithmetically. But the *cause* was that a brand-new payment method had appeared five weeks
earlier — which a different, deterministic detector had already correctly reported as "a new category
appeared". One real event, about to be claimed twice under two different names. Measured properly
(over the categories present in *both* periods) the distribution had not moved at all.

Two lessons came out of it: a population-comparison statistic must be computed over the **common
support** (the values present on both sides), and a new value is a *schema* event, not a *distribution*
event, and belongs to a different detector.

---

# Part 2 — Walking the pipeline

## 2.1 Getting the data in

### Push or pull, batch or stream

Two independent axes:

|  | **you ask** (pull) | **they send** (push) |
|---|---|---|
| **a chunk at a time** (batch) | `energy_predML` calling a government API; `mediaElection27` fetching news feeds | a supplier dropping a file on a shared directory (`refund-lab-autom`) |
| **continuously** (stream) | — | `DE_pipeline_stream`'s live event feed |

Pull is easier to operate: you control when, you can retry, you can go back. Push is lower-latency and
lower-effort for the sender, but you must be available when they send, and you must notice when they
*don't*.

The file-drop case is the sneaky one, and `refund-lab-autom` shows why: there is no manifest, no
checksum, no notification. **Nothing tells you a file is missing except your own expectation.** The
project therefore maintains a table of what it *expects* to arrive and when, and compares. That table
is the whole missing-file detector.

### Talking to an HTTP API

Most data still arrives over HTTP, so a few terms:

- An **API** is just a set of URLs a program can call. A **REST API** is a common style where a URL
  names a *thing* (`/v1/stations`) and the HTTP verb names the action (`GET` to read, `POST` to create).
- An **endpoint** is one such URL.
- A **status code** is the three-digit number in the reply: `200` fine, `404` no such thing,
  `401` you're not authenticated, `429` you're going too fast, `503` I'm temporarily broken.
- **Pagination**: a big result is returned in pages. Two common styles — *offset* ("give me rows
  5000–9999") and *cursor* ("here is a token; hand it back for the next page").
- **A token** is a string proving who you are, usually obtained from a login endpoint and sent in an
  `Authorization` header. Tokens expire.

Now: **the naive way to call an API is to call it.** The professional way is to assume every one of
these will happen, and `refund-lab-autom`'s client is a catalogue of them, all handled in one module so
that no other part of the codebase has to think about it:

| What goes wrong | What the client does |
|---|---|
| `429 Too Many Requests` | wait and retry, honouring the server's `Retry-After` header if present, with randomised ("jittered") delays so retries don't synchronise |
| `503` temporarily down | retry with backoff; count how long the outage lasted |
| `401` token expired mid-pull | fetch a fresh token and continue — the token's lifetime was *measured* (≈988 seconds) rather than assumed |
| `200 OK` with `{"error": "internal_error"}` in the body | **a success code is not a success.** Inspect the body too |
| the reply says it is compressed but isn't (or vice versa) | **look at the bytes**, not the header: gzip data always starts with the two bytes `1f 8b` |
| the pagination cursor repeats, or doesn't move, or an empty page claims there's another | stop, and count it as an anomaly, rather than looping forever |
| the server reports a total row count | treat it as a *hint that something changed*, never as proof you got everything |

Two refinements in that client are worth internalising because they generalise:

**Retry semantics depend on the question you are asking.** The main request function retries a `503`
for seventeen seconds — correct when you want data. But there is a separate, deliberately
*un-retried* function used to ask "is the server up right now?", because for that question, retrying
gives you the wrong answer. Same HTTP call, different purpose, different behaviour.

**Ambient noise is counted, not reported.** About 2.7% of requests return `429`. That is the
environment, not an incident. It is tallied in a per-cycle statistics object, and only a *sustained*
outage becomes something a human should see. Knowing the difference between "the weather" and "an
event" is most of what makes monitoring bearable.

### Doing many requests at once, but not too many

`mediaElection27` fetches about fifteen news feeds. Doing them one after another wastes time (most of
it is waiting); doing all fifteen at once may get you rate-limited or banned.

The standard shape, in Python's async style:

```python
sem = asyncio.Semaphore(8)          # at most 8 in flight at any moment
async with httpx.AsyncClient(timeout=30, headers={...}) as client:
    async def bounded(spec):
        async with sem:              # wait here if 8 are already running
            return await fetch_one(client, spec)
    return await asyncio.gather(*(bounded(s) for s in specs))
```

Three details that matter more than the concurrency:

1. **One client, reused.** Creating a fresh HTTP client per request throws away connection reuse.
2. **A timeout.** Without one, a single hung server hangs your whole run, forever.
3. **`fetch_one` never raises.** A failure becomes a *result object* carrying `status=0` and an error
   message. This is the important one: **one dead feed must not abort the batch of fifteen.** If
   errors are exceptions, one bad supplier takes down your whole ingestion.

### The hardest question in ingestion: "what's new?"

You would think "give me everything since last time" is a standard API feature. Very often it is not.

`refund-lab-autom`'s source lets you filter by *when things happened* and by *what the source knew as
of a date* — but there is no way to ask *"what became knowable since I last looked"*. And that is the
crux, because records get added late, corrected late, and removed.

There is no single fix. The project uses four mechanisms and explains the role of each:

| Mechanism | How it works | Cost | Catches |
|---|---|---|---|
| **rolling window** | re-fetch the last 3–5 days, every single run | small and constant | ordinary late arrivals; fresh corrections |
| **count reconciliation** | ask the source how many records it thinks exist in a few coarse date ranges; compare with your own count; if they differ, **split the range in four and recurse** until you find the offending day, then re-fetch just that day | one request per range | rows appearing far in the past, **and rows disappearing** |
| **rotating deep audit** | fully re-fetch a small number of individual historical days each run, weighted towards the 15–75-day band where corrections cluster | two days per run | corrections that change *no count* and are therefore invisible to the reconciliation above |
| **bootstrap** | one full walk of everything, at the start | once, expensive | the initial state |

Several things here are worth carrying away:

- **The bisection.** Comparing counts over a wide range costs one request; finding *which day* differs
  by re-fetching every day would cost hundreds. Binary (here: four-way) search over date ranges gets
  you there in a handful.
- **Corrections are invisible to counting.** If a record's value changes but the number of records
  doesn't, no count will ever disagree. That is why the audit exists and why it is separate.
- **Rotate the expensive checks.** Recent history is checked every run (that's where detection latency
  matters); deep history every third or fifth run. Total cost per run fell from ~50 requests to ~20.
- **The count is a hint, never a conclusion.** Every conclusion drawn from a count is confirmed by an
  actual fetch before it reaches the database. The source is documented as capable of lying about its
  counts; this design survives that.

### Files, when nothing tells you anything about them

`refund-lab-autom`'s file channel is a good short lesson in defensive reading, because *every* fact
about a file has to be established from the bytes:

| Fact | How it is established | Why not the obvious way |
|---|---|---|
| Is it compressed? | first two bytes are `1f 8b` | the `.gz` extension lies, and detecting that lie is itself one of the faults to be found |
| What encoding? | try strict UTF-8; on failure fall back and **record the fallback** | there is no header; and a silent fallback would hide a real change |
| Is it complete? | a compressed stream that ends early; a file not ending in a newline; a last row with the wrong number of fields | there is no checksum and no length |
| Did the columns change? | compare the header line against the previous file of the same kind | a new column shifts everything if you read by position |
| Is this the same file twice? | a SHA-256 hash of the contents | the filename can differ |

And one nice piece of hard-won knowledge: **the duplicate-file check deliberately excludes the product
catalogue**, because consecutive daily catalogue snapshots are frequently *byte-identical* when nothing
changed. Without that exclusion, every quiet day would have produced a false "duplicate delivery"
alarm. They knew this because they measured it before writing the detector.

### Server-Sent Events: streaming over plain HTTP

`DE_pipeline_stream`'s source publishes a continuous feed using **SSE** (Server-Sent Events), which is
worth knowing because it is the simplest possible streaming protocol — it is just an HTTP response that
never ends:

```
id: 42
event: trip_started
data: {"event_id": "...", "payload": {...}}
                              ← a blank line ends one message
: keepalive                   ← a line starting with ':' is a comment, ignored
```

Three fields, a blank line as separator, comments for keeping the connection alive when nothing is
happening. The client is about forty lines of code.

The clever part is the **resume** mechanism, which is built into the protocol: each message has an
`id`, and if the connection drops, the client reconnects sending a `Last-Event-ID` header, and the
server continues from there.

But a server cannot remember everything forever, so `DE_pipeline_stream`'s source keeps the most recent
50,000 messages in memory and, if you ask to resume from a point that has already fallen out, sends you
a **gap notice** saying where its memory now starts. That is the source being honest about its own
delivery guarantee: *at-least-once from your resume point, as long as your resume point is still in the
buffer*. Compare that to a source that silently starts you from "now" and lets you discover the hole
yourself, months later, in a report.

---

## 2.2 Making sense of it

### Schemas and contracts

A **schema** is a description of shape: these fields, of these types, some required. A **contract** is
a schema plus a promise — an agreement between a producer and a consumer that the shape will be this,
and that breaking it is the producer's fault.

`DE_pipeline_stream` does something that looks like duplication and isn't. The source defines its event
shapes in one module; the consumer defines *the same shapes again* in its own module. Why not share
the class?

> **Because producer and consumer share a contract, not code.**

If they shared the class, a change to the producer would silently become a change to the consumer —
the consumer would simply accept the new shape, and the fact that the contract was broken would
disappear. With two copies, the producer changing means the consumer *rejecting*, loudly, which is
exactly what should happen.

Strictness matters too. The consumer's models are configured with:

- `extra="forbid"` — an unexpected field is an error (so a *renamed* field is caught, since the old
  name is now missing and a new name is present);
- `strict=True` — no helpful automatic conversions, so the string `"754s"` is not silently accepted
  where an integer was promised.

Those two settings are the difference between detecting schema drift and absorbing it.

### Two valid strategies when a record breaks the contract

This is the place where `DE_pipeline_stream` and `refund-lab-autom` do **opposite** things, and both are
right. Understanding why is more valuable than either strategy on its own.

**Strategy A — reject, and keep the evidence.** *(`DE_pipeline_stream`)*

The invalid record is sent to a **dead-letter queue** — a separate stream for things that could not be
processed — carrying:

- a classification of the error (`unparseable` / `unknown type` / `contract violation`),
- the list of specific validation failures,
- the position it came from,
- **the original text**, so it can be replayed once the problem is fixed.

Everything downstream can now assume records are well-formed, which simplifies every later stage.

**Strategy B — repair, but record.** *(`refund-lab-autom`)*

Rejecting is not an option when you don't own the supplier and cannot afford to lose rows. So:

1. **Never drop a field.** Unknown fields go into an `extra_json` column and raise a schema-drift
   signal. The reasoning, verbatim: *"A field we throw away is a fault we cannot see."*
2. **Repair, but record.** An identifier that arrived as `191` instead of `0000191` is padded back to
   seven digits so the join still works — **and the repair is counted, because the repair is the
   evidence.**
3. **Never raise.** A row that cannot be parsed is written with nulls and counted.

**Which to choose:** Strategy A when you own or can influence the producer, and losing a record is
acceptable because you can ask for it again. Strategy B when the producer is external and indifferent,
and losing records is not acceptable. Note that Strategy B *only works* because of the counting — the
repairs are turned into metrics that a monitor reads. Repair without recording is just data corruption
that you performed yourself.

### Four real-world messes you will meet

**Mojibake** — text that has been encoded once and decoded with the wrong character set, so `Gijón`
becomes `GijÃ³n`. The tempting detector is "does the text contain `Ã`?" — but `Åland` is a real place
and `Ångström` is a real word. `refund-lab-autom` requires the **pair**: an accented capital *followed
by* another high-range character. That pattern occurs in corrupted text and essentially never in real
text, and they verified it against a list of genuine accented city names. **A detector for a text
pathology must be tested against real text that looks similar.**

**Sentinels** — a value standing in for "missing": `-999`, `1900-01-01`, `N/A`, `TBD`. These are a data
fault: they will silently poison any average you compute. They should become NULL, and be reported.

But — and this is a beautifully precise distinction — bare spellings of nothing (`""`, `null`, `none`)
are **not** sentinels:

> *"those are how a source says 'nothing', and at higher hostility levels they are also how the
> transport says it. A sentinel is a value pretending to be data."*

Treating `""` as a fault would produce alarms on every normal optional field. The distinction between
"the source said nothing" and "the source said something meaningless" is the whole detector.

**Renamed fields.** A field disappears and an unfamiliar one appears in the same feed. You could fail;
you could ignore it. `refund-lab-autom` does a third thing: match them by name similarity (string
similarity ratio, plus a bonus if they share an underscore-separated word), and if the match is
confident, **carry the value across so downstream keeps working, and report the mapping**. Continuity
*and* visibility.

**Timestamps with and without timezones.** A feed where every timestamp is naive ("2026-03-02 08:14")
and suddenly one carries an offset ("2026-03-02 08:14+02:00") is a genuine signal — something upstream
changed. So the parser returns not only the parsed time but a flag saying *whether it had an offset*,
and that flag is stored. `mediaElection27`'s parser does the complementary thing: naive timestamps from
a French newspaper are **assumed to be Paris time**, converted to UTC, and then sanity-checked — reject
anything before 1990 or more than seven days in the future, because feeds do occasionally contain
nonsense dates and a single year-3000 article ruins every chart.

---

## 2.3 Keeping it

### What kind of database, and why

A quick map, since these four projects use four different answers.

**Relational databases** store tables with typed columns, enforce constraints, and support
transactions. The two properties that matter most:

- **A transaction** is all-or-nothing: either every statement in it takes effect or none does. This is
  what makes exactly-once (§1.5) possible.
- **Constraints** — primary keys, uniqueness, foreign keys — are rules the database enforces so your
  code doesn't have to. A `PRIMARY KEY (topic, partition, offset)` makes duplicate loading not merely
  unlikely but *impossible*.

Then two orthogonal distinctions:

**Server versus embedded.** A server database (PostgreSQL) is a separate process you connect to over
the network; many clients can read and write at once. An embedded database (SQLite, DuckDB) is a
*library* plus a *file* — no process to run, no port, no credentials. The trade-off is concurrency:

> DuckDB allows only **one writer at a time**, because the "lock" is a lock on the file.

That single sentence drives five separate design decisions in `DE_pipeline_stream` — see §2.9.

**Transactional versus analytical** (jargon: OLTP vs OLAP). A transactional database is optimised for
"fetch this one row, update this one row" — a web application. An analytical database is optimised for
"scan ten million rows and average a column". They store data differently: transactional databases
store rows together, analytical databases store **columns** together, so averaging one column reads
only that column off disk.

| Project | Choice | Why |
|---|---|---|
| `mediaElection27` | PostgreSQL (managed) | it *is* a web application — many concurrent readers, small lookups, a browser at the end |
| `DE_pipeline_stream`, `energy_predML` | DuckDB | analytical queries over local data, no server to operate |
| `refund-lab-autom` | SQLite | stated plainly: DuckDB wasn't available, the data fits, one file, crash-safe, nothing to babysit during an unattended multi-hour run |

**Parquet** deserves a mention: it is a *file format*, not a database — columnar, compressed, and
readable by almost every analytics tool. `DE_pipeline_stream` has its stream processor write Parquet
files into a directory, and then its database reads them **in place**, with no loading step at all:

```sql
read_parquet('/data/lake/station_metrics/*/*.parquet', hive_partitioning = true)
```

Two tools, one set of files, no copy. That is the practical meaning of the word **data lake**: files in
a directory structure, queryable where they lie. ("Hive partitioning" just means the directory names
encode column values — `date=2026-03-02/` — so a query for one date can skip the other directories
without opening them.)

### Layers: raw, staging, marts

Every serious warehouse organises tables into layers. The names vary; the roles don't:

| Layer | Contains | Rule |
|---|---|---|
| **raw** (landing) | exactly what arrived | never edited; the only thing that writes here is the loader |
| **staging** | one cleaned-up view per raw table: parsed, typed, deduplicated | **no business logic** — only "what does this record say" |
| **intermediate** | reusable building blocks that several outputs need | the place joins and heavy work live |
| **marts** | what people actually query | named for the business, not the source |

The value is not tidiness. It is that **each layer has one job, so a bug has one home.** A wrong number
in a report is either a parsing problem (staging), a logic problem (intermediate) or a presentation
problem (mart), and you can tell which by querying each layer.

`DE_pipeline_stream` goes one step further and makes each layer a **schema** in the database — so the
layer is visible in every table name (`staging.stg_trip_events`, `marts.fct_trips`).

### Facts and dimensions

An old vocabulary that is still the clearest way to organise analytical tables:

- A **fact** table holds *things that happened*: one row per event or transaction. It is long and it
  grows forever. `fct_trips`, `fct_orders`.
- A **dimension** table holds *things that are*: one row per entity, describing it. It is comparatively
  short and changes slowly. `dim_stations`, `dim_customer`.

Facts reference dimensions by key. Drawn out, facts in the middle with dimensions around them, it looks
like a star — hence **star schema**.

Why bother: it makes the questions obvious. "Departures per zone per hour" is a fact table grouped by a
column you get from a dimension. And it locates change: a fact is never edited (it happened); a
dimension changes over time — which is exactly why SCD2 (§1.6) exists.

### Partitioning

**Partitioning** means splitting a table (or directory) by the value of a column, usually a date. Two
payoffs:

1. **Pruning.** A query for March only reads March.
2. **Targeted rebuilds.** You can recompute *one day* instead of everything.

That second one is what makes `refund-lab-autom`'s 2,147-run loop affordable: each run rebuilds only
the days that changed.

And it contains a trap that the project hit, and that is worth knowing in advance:

> A label (the refund outcome) settling *today* belongs to an order placed *weeks ago*. If you only
> rebuild partitions that today's *orders* touched, that older order's day is never rebuilt, and your
> conformed table quietly goes stale.

The symptom was that matured label coverage read 22% when it should have read 100%, and the apparent
refund rate read 44% instead of 17%. The note on it is the memorable part: **"the data was correct
throughout — only the conformed layer was stale."** This is the classic *late-arriving dimension* trap.
The fix: a late label or a late payment marks **its order's** partition dirty, not today's.

---

## 2.4 Reshaping it

### Why SQL

Because the data is already in a database, and moving ten million rows into Python to filter them and
move them back is slow and pointless. Modern analytical SQL is also far more expressive than most
people realise — these projects use `QUALIFY`, `FILTER`, `GROUP BY ALL`, list aggregation, JSON
extraction and time bucketing, none of which existed in the SQL most people learned.

### What dbt is, and what it adds

`dbt` (data build tool) is the most important tool in `DE_pipeline_stream`'s transformation layer, and
the easiest to misunderstand. The shortest accurate description:

> **dbt is SQL files plus a build system.**

You write `SELECT` statements. Each file is a **model**. dbt wraps it in the right `CREATE TABLE` or
`CREATE VIEW`, figures out the order to build them in, runs the tests, and generates documentation.

What it adds to plain SQL:

**References that create a graph.** Instead of naming another table, you write `{{ ref('stg_trip_events') }}`.
dbt substitutes the real name *and* learns that this model depends on that one. From all the `ref`s it
builds a dependency graph and runs everything in the right order, in parallel where it can. You also
get a lineage diagram for free — which is the answer to "if I change this, what breaks?"

**Materialisation as a configuration, not a rewrite.** The same `SELECT` can be a view (computed on
every query, always current, free to store) or a table (computed once per build, fast to query) by
changing one setting. You are not rewriting the query to change that decision.

**Incremental models.** A table that, after the first build, only processes new rows:

```sql
{{ config(materialized='incremental', unique_key='trip_id', incremental_strategy='delete+insert') }}

select ... from {{ ref('int_trips') }}
where pairing = 'complete'
{% if is_incremental() %}
  and loaded_at > (select max(loaded_at) from {{ this }})
{% endif %}
```

Read that filter again — it is **landing time**, per §1.2. That one choice is what makes late-arriving
data safe. And `delete+insert` on `trip_id` means a trip that is reprocessed *replaces* its old row
rather than joining it.

**Tests as part of the build.** dbt's tests are queries that should return **zero rows**. If a query
returns rows, those rows *are* the failure, and you can look at them. `DE_pipeline_stream` uses the
whole taxonomy, which is a good checklist:

| Kind | What it is | Example |
|---|---|---|
| built-in generic | ready-made, applied from a YAML file | `unique`, `not_null`, `accepted_values`, `relationships` (foreign key) |
| custom generic | your own reusable test | `within_range: {min_value: 0}` |
| singular | a query written for one specific situation | "station snapshots reporting more bikes than docks" |
| unit test | hand-written input rows and expected output rows | checks the *logic*, independent of today's data |
| contract | "this table must have exactly these columns with these types" | the build fails if it doesn't |
| freshness | "the newest row landed less than N minutes ago" | catches a stopped pipeline |

And a lovely twist: some tests are **expected to find rows**. The source deliberately injects
over-capacity readings, so a test for them will always fail. Rather than deleting the test,
`DE_pipeline_stream` sets it to `severity: warn` with `store_failures: true` and a dedicated `audit`
schema. Now the build stays green, and the offending rows **accumulate in a table you can query.**

> **A test that stores its failures is a monitor with a SQL interface.** That reframing is worth the
> price of admission.

**Snapshots** implement SCD2 (§1.6) for you: point dbt at a table that gets overwritten, tell it which
column marks a change, and it maintains a full history with validity windows.

**Seeds** are small CSV files kept in version control and loaded as tables — perfect for mappings that
are *editorial decisions* rather than data. `DE_pipeline_stream` uses one to say which check targets
which fault; adding a new detector to the quality report means adding a row to a CSV, not rewriting SQL.

**Macros** are reusable SQL, written in a templating language (Jinja) that runs before the SQL does.
The distance formula appears in one macro and is called from several models.

### Window functions: the thing worth learning first

If you learn one piece of advanced SQL, learn **window functions**. They let you look at *other rows*
from within a row, without a self-join.

Deduplication, in one clause:

```sql
qualify row_number() over (partition by event_id order by loaded_at, "partition", "offset") = 1
```

Read as: number the rows within each `event_id`, ordered by arrival; keep number 1. (`QUALIFY` is like
`WHERE` but runs after the window functions — not every database has it, but DuckDB does.)

And comparing a row to the previous one, which is how `DE_pipeline_stream` finds a station that has
frozen:

```sql
lag(bikes_available) over (partition by station_id order by event_time) as prev_bikes
```

Now "identical to the previous reading, even though bikes demonstrably came and went in between" is a
simple condition. A windowed, real-time version of the same check can only approximate it; the
sequential version is exact. That trade-off — *exact but later* versus *approximate but immediate* —
is the central trade-off of §2.6, and here you can see both implemented side by side.

One more small gem, on deduplication. When you remove duplicates, you usually throw away the
information about *why* there were duplicates. `DE_pipeline_stream` keeps two counters:

- `copies` — how many times this event id landed;
- `source_copies` — how many *distinct source sequence numbers* carried it.

If `source_copies > 1`, the **source** sent the same event twice (a data fault). If
`copies > source_copies`, **your own ingestion** re-sent it after a crash (your at-least-once delivery
working exactly as designed). Same symptom, opposite diagnosis, and you can only tell them apart
because someone thought to keep both counts.

---

## 2.5 Making it happen by itself

### Why `cron` is not enough

`cron` runs a command on a schedule. That is all it does. It does not know:

- that job B should run only after job A succeeded;
- that a failed job should be retried twice, thirty seconds apart;
- that you want to re-run last Tuesday;
- whether anything ran at all.

An **orchestrator** adds those. The core vocabulary:

- A **DAG** — directed acyclic graph — is the shape of your dependencies: arrows between tasks, no
  cycles. In Airflow, a DAG is also the unit of scheduling: one file, one pipeline.
- A **task** is one step. A **DAG run** is one execution of the whole thing for one scheduled time.
- **Retries**, **backfill** (running past intervals deliberately), and **catchup** (whether to do that
  automatically) are all first-class settings.

### Four different answers, and why each is right

| Project | Approach | Why |
|---|---|---|
| `DE_pipeline_stream` | **Airflow** — a real scheduler with a database, a web UI and a task queue | the project exists to demonstrate a modern stack correctly wired; and it genuinely needs Airflow's *pools* (below) |
| `mediaElection27` | **a cloud cron trigger plus file events** | serverless: an always-on scheduler costs money. The second hop has no scheduler at all — writing a file *is* the trigger |
| `energy_predML` | **a workflow library, but nothing scheduling it** | the project is about the ML lifecycle; scheduling was the one thing deliberately left undone, and the docs say so plainly |
| `refund-lab-autom` | **a `while` loop and a bash restart script** | it advances a *simulated* clock and must retry the **same simulated day** on failure. No off-the-shelf scheduler has those semantics |

**The transferable question is not "which orchestrator" but "what does one run mean here?"** — a
wall-clock interval, an event, a manual invocation, or a tick of a clock your own code owns.

### Airflow ideas worth knowing even if you never use Airflow

**A pool is a semaphore for tasks.** Recall that DuckDB allows one writer. `DE_pipeline_stream` creates
a pool named `duckdb` with **one slot**, and every task that writes declares `pool="duckdb"`. Tasks now
queue instead of colliding. This is a concurrency primitive expressed as configuration, and the idea
generalises: any time several jobs contend for a scarce resource, a named pool with a slot count is the
clean expression of it.

**Data-aware scheduling.** Rather than "run the transformation at :05, and hope loading finished",
you declare that loading *produces* a dataset and the transformation *consumes* it:

```python
# the loading DAG
@task(outlets=[RAW_STREAM])
def report(summaries): ...

# the transformation DAG — note: no schedule at all
@dag(schedule=(RAW_STREAM | RAW_API))
def dbt_transform(): ...
```

The transformation runs when either upstream announces new data. No clock, no guessing, no gap.

And there is a detail here that is easy to miss and important: when a load brings in **zero** rows, the
task deliberately *skips itself*, because **a skipped task emits no event**, so the downstream
transformation doesn't run for nothing. Small; saves a great deal of pointless work.

**Connections and variables from the environment.** Credentials and addresses can be stored in
Airflow's database and edited in its web UI. `DE_pipeline_stream` refuses to do that and sets them as
environment variables instead:

```yaml
AIRFLOW_CONN_KAFKA_LANDING: '{"conn_type": "generic", "extra": {"bootstrap.servers": "kafka:9092"}}'
AIRFLOW_VAR_WAREHOUSE_PATH: /data/warehouse/bikeshare.duckdb
```

*"Nothing is stored in Airflow's database, and nothing is clicked in the UI."* The entire wiring is
readable in one file and reproducible from scratch. **Configuration that lives only in a UI is
configuration you will lose.**

### The rule that matters most: keep the logic out of the orchestrator

`DE_pipeline_stream`'s loading code lives in a plain Python package with **no Airflow import anywhere**.
The DAG files only schedule it. Three payoffs, all immediate:

1. You can unit-test the logic without installing Airflow.
2. You can run the same code from a command-line script — and the project does, for its inspection tools.
3. If you ever change orchestrator, you change the thin scheduling layer, not the pipeline.

The same pattern appears in `energy_predML`, where the workflow decorators wrap plain functions.

### When you own the clock

`refund-lab-autom` runs against a simulated world, and gets two details right that generalise to any
retry loop:

**Advance the clock in the *runner*, not in the *work*.** A run that fails is retried against the same
simulated day, rather than burning another day on every attempt. (Translated to the real world: the
thing that decides "which interval am I processing" should not be the thing that fails.)

**Overlap dead time.** The single largest cost per run was 1.6 seconds spent waiting for the supplier's
file writer — pure idleness, multiplied by 2,147 runs. It now runs in a background thread *while* the
API is being fetched. The discipline that makes that safe is worth copying: **only the filesystem is
touched from the thread**; nothing is parsed or written until the main thread joins it. Concurrency
confined to the one operation that needs it.

---

## 2.6 Streaming, the special case

Only one of the four projects has a genuine stream, and the concepts are different enough to deserve
their own section.

### What makes a stream different

A batch has an end. You can sort it, count it, join it, and know you are done. A stream does not:

1. **It is unbounded.** "The average of this column" has no answer; you must say *over what window*.
2. **It is out of order.** Events arrive late, and there is no moment at which you know none are left.
3. **Therefore every aggregate is a decision, not a fact.** You must decide when to stop waiting.

### The queue in the middle: Kafka vocabulary

Apache Kafka is a **distributed log**. Not a queue in the "take it and it's gone" sense — a log you
append to, that several independent readers read at their own pace.

| Term | Meaning |
|---|---|
| **topic** | a named stream (`bikeshare.trip-events.v1`) |
| **partition** | a topic is split into partitions; each is an ordered sequence. Order is guaranteed *within* a partition only |
| **key** | each message may have a key; the key decides the partition. Same key → same partition → **order preserved for that key** |
| **offset** | a message's position in its partition. Monotonic, starts at 0 |
| **producer** | something that writes |
| **consumer group** | a set of consumers sharing the work; each partition goes to one member |
| **retention** | how long messages are kept regardless of who read them (7 days here). **This is what makes replay possible** |

The two facts to internalise:

**The key determines ordering.** `DE_pipeline_stream` keys trip events by `bike_id` and station
readings by `station_id`, so all events about one bike stay in order relative to each other. It does
*not* try to keep a global order — that would mean one partition and no parallelism.

**Because Kafka retains messages, consumers are independent.** The stream processor and the warehouse
loader read the same topics without knowing about each other, at different speeds, with separate
positions. And a *new* consumer can start from the beginning and catch up. This is why the project
calls Kafka "the fan-out point": *"replay and backfill therefore come for free."*

Two operational conventions from that project:

- **Declare topics explicitly, disable auto-creation.** Otherwise a typo silently creates a new topic
  with default settings and your messages vanish into it.
- **Name them `<domain>.<dataset>.v<major>`.** A breaking change to the message shape means a *new
  topic* that consumers migrate to, not a silent edit to the old one.

### Windows and watermarks

To aggregate a stream you cut it into **windows** — usually fixed intervals of event time ("every 30
minutes"). Then: when is a window finished?

Never, strictly. Something from 08:00 might arrive at 11:00. So you make a promise called a
**watermark**:

> *"I no longer expect to see anything older than [now − 30 minutes]."*

A watermark is **a decision, not a measurement**. It trades latency against completeness:

- a long watermark waits longer, catches more late data, uses more memory, reports later;
- a short one reports fast and drops more.

Anything arriving behind the watermark is **dropped** by windowed operations — and `DE_pipeline_stream`
counts the drops and logs them per batch, which is the right instinct: *if your system discards data,
count the discards.*

Now the insight that explains the project's whole streaming design. The processor runs **four separate
jobs** rather than one, and the clearest reason is late data:

> The rule that flags "this event arrived very late" **must see** late events.
> The windowed aggregations **must ignore** them past the watermark.

These are incompatible requirements in one job. So the late-event detector runs in a *stateless* job
with no watermark at all, and the aggregations run in *stateful* jobs with watermarks. One job, one
purpose, one output.

### Stateless and stateful

- **Stateless**: each record is judged alone. "Is this reading above capacity?" Cheap, restartable,
  no memory.
- **Stateful**: the answer depends on other records. Windows, deduplication, joins between streams.
  The framework must *remember* things between batches — and that memory has to be stored somewhere
  durable, which is what a streaming **checkpoint** is.

`DE_pipeline_stream` stores that state in RocksDB (an embedded key-value store) rather than in memory,
because it grows. And there is a trap recorded: the *number of partitions used for shuffling* is frozen
into the checkpoint, so changing it later requires deleting the checkpoints and replaying.

### Joining two streams

Pairing "trip started" with "trip ended" is a **stream-stream join**, and it needs two extra things a
batch join doesn't:

1. a **watermark on each side**, so the engine knows when to stop waiting;
2. a **time bound** on the join condition (`ended_at <= started_at + INTERVAL 3 hours`), so it knows
   how far apart a pair can be.

Together these tell the engine when a start can no longer find its end — at which point it can free the
memory *and emit the unmatched row*, which is exactly the "orphan trip" detection.

### The trade-off, measured

The best thing about `DE_pipeline_stream` is that it implements the same check twice — once in the
stream, once in batch SQL — and then compares them in a table. The results are a genuine education:

| | real-time (windowed) | batch (sequential SQL) |
|---|---|---|
| frozen station | fires in ~2 hours of simulated time, 86% precision, **40% recall** | finds **all** of them, but only at the next run |
| how it works | "most readings in this window were identical, and trips happened" | "this reading is identical to the previous one, though bikes demonstrably moved" |
| why it differs | short episodes can't fill a window | it compares actual consecutive readings |

And the tuning is recorded honestly: 30-minute windows gave **8%** precision; 2-hour windows gave
**86%**. The same code, one parameter, a tenfold difference in usefulness. Nobody could have guessed
that; they measured it.

> The conclusion is not "streaming is worse". It is: **a windowed heuristic and an exact sequential
> check are different tools.** You want the fast approximate one for alerting and the slow exact one
> for the record — which is precisely what the project does.

---

## 2.7 Watching it

A pipeline you cannot see is a pipeline you cannot operate. Four layers of visibility, from cheapest to
most valuable.

### Layer 1 — structured logs

Not `print("processed 450 events")` but one machine-readable object per unit of work:

```json
{"ts":"2026-03-02T08:14:22", "level":"INFO", "logger":"bridge", "msg":"throughput",
 "received":450, "per_s":15.0, "by_type":{"trip_started":210,"station_status":240},
 "last_seq":91237, "checkpoint":91230, "source_lag":7}
```

Now `grep` becomes a query. The helper that produces these is six lines
(`event(log, "throughput", received=450, ...)`), and it is worth writing on day one of any project.

`DE_pipeline_stream`'s stream processor does the same thing per micro-batch, emitting rows-in,
rows-per-second, batch duration, current watermark, state size, **rows dropped for being late**, and
**duplicates removed**. Those last two are the ones you will want at 3 a.m.

### Layer 2 — an audit table, in the data's own database

All four projects record what the pipeline did *next to* what the data says, so both are answerable in
SQL:

| Project | Table | One row per |
|---|---|---|
| `DE_pipeline_stream` | `raw._ingest_batches` | load batch: source, rows, ranges, start and end time |
| `mediaElection27` | `ingest_runs` | feed fetch: HTTP status, items seen, inserted, skipped, error, duration |
| `energy_predML` | `model_runs`, `predictions` | training run; served prediction |
| `refund-lab-autom` | `pull_log`, `day_counts`, `test_results`, `metrics` | request, day, test, metric |

Why in the database rather than in a monitoring system: because then "how many rows did we land per
hour, and what was the delay from source to warehouse?" is a `JOIN`, and it can go on the same
dashboard as the business numbers. `DE_pipeline_stream` does exactly that — it has a "pipeline health"
table computed from its own audit log.

`mediaElection27` adds a nice touch: the audit row is written in **its own transaction**, separate from
the data. So when loading a feed fails and rolls back, *the record that it failed still gets written.*
If your failure log is inside the transaction that failed, you have no failure log.

### Layer 3 — freshness and the checks from §1.7

The cheapest high-value check in existence: *how old is the newest row?* dbt has it built in
(`warn_after: 15 minutes, error_after: 60 minutes`). It catches the failure mode that silently
destroys trust — a pipeline that stopped, while every dashboard keeps rendering yesterday's numbers as
if they were today's.

Note the nuance in how `DE_pipeline_stream` configures it: freshness is checked on the tables where
silence is bad, and explicitly *disabled* on the dead-letter table and the alerts table, where **silence
is good news**. Applying a check uniformly where it doesn't apply is how alert fatigue starts.

### Layer 4 — knowing whether your checks are any good

Covered in §1.7. The short version: if you can generate the problems, measure your detection
(precision, recall, time-to-detect). If you cannot, at least write down, honestly, **what you would
miss** — as `refund-lab-autom` does in a table with one row per possible fault, one column for the
detector, and one column headed *"risk of missing it"*, concluding:

> 22 of the 30 are deterministic or reconciliation-based and should not be missed. Five rest on
> thresholds calibrated without a clean baseline. Three have a structural gap I chose not to close
> because closing it would have cost more in false positives than it bought.

**That paragraph is worth more than a claim of completeness**, because it tells a reader exactly where
to be sceptical.

### When you were wrong: retraction

One last practice, specific to `refund-lab-autom` and unusually mature. Four of its claims turned out
to be its own detectors misfiring. Rather than quietly deleting them, it wrote them to a
`retractions.jsonl` file with the reason, under one rule:

> **An entry is removed only when the detector that produced it is found to be mis-specified and is
> fixed in the same change.** No entry was ever removed because it turned out not to match, and
> nothing was ever added after the fact.

That rule is what separates "correcting an error" from "improving your score". The reasoning behind
each retraction is also a small statistics lesson — one of them turned on noticing that a rare-event
detector would produce about 25 false alarms across the run *from Poisson noise alone*, which is the
sort of arithmetic worth doing **before** deploying a rare-event detector, not after.

And a final, humbler one: a script that performed a retraction corrupted the output file because it
wrote a literal `\n` instead of a newline. Caught in three minutes. The recorded lesson is not about
the escape character:

> *"The scored deliverable was being edited by ad-hoc scripts with no read-back check. The retraction
> path deserved the same care as the write path."*

---

## 2.8 Learning from it

Machine learning on operational data is mostly not about models. It is about **not accidentally
cheating**, and there are four specific ways to cheat that you will meet in practice.

### Cheat 1 — using the future (leakage)

**Leakage** is using, at training time, information that will not be available when you actually
predict. It is the most common reason a model that looked excellent in development is useless in
production.

The textbook case in time series is a rolling average:

```python
df["roll_24h_mean"] = df["demand"].rolling(24).mean()          # WRONG
df["roll_24h_mean"] = df["demand"].shift(1).rolling(24).mean() # right
```

The first includes *the current row's own value* in its own feature. The model learns to read the
answer off the feature. Your validation score is magnificent; at prediction time the current value does
not exist yet and the feature is a different thing entirely.

`energy_predML` shifts **every** rolling and difference feature, and has a unit test asserting the
exact index relationship:

```python
assert result.iloc[30]["demand_lag_24h"] == sample_df.iloc[6]["demand_mwh"]
```

That is an odd-looking test until you realise it is the *only* mechanical defence against someone later
"simplifying" the pipeline and reintroducing the bug.

There is a second, subtler kind of leakage: a *column* that quietly contains the answer (an internal
status field only set after a refund was processed, say). `refund-lab-autom` screens for it at every
retraining: compute, for each feature alone, how well it separates the outcome; anything above 0.90 is
treated as leakage, **reported as an incident**, and dropped from the model. An honest single feature
essentially never separates a real outcome that well.

### Cheat 2 — shuffling time

Standard practice in machine learning is to shuffle your data and split it randomly. **For time series
this is invalid**, because it lets the model train on Thursday and Saturday and be tested on Friday —
it interpolates between neighbours rather than predicting forward.

Always split **temporally**: train on the past, validate on the future. For cross-validation, use the
variant that respects order (`TimeSeriesSplit`), which builds a growing window:

```
fold 1:  train [-------]  validate [---]
fold 2:  train [-----------]  validate [---]
fold 3:  train [---------------]  validate [---]
```

### Cheat 3 — mistaking "don't know yet" for "no"

This one is genuinely subtle and it is called **censoring**.

Look at an orders table with a refund flag. Some rows say refunded. The rest say nothing. It is
extremely tempting to read "nothing" as "not refunded".

But an order placed *yesterday* has nothing to say yet — its 30-day refund window has not closed. Half
your "not refunded" rows are actually "undetermined". Train on that and you teach the model that recent
orders are never refunded, which is not a fact about the world; **it is a fact about your calendar.**

`refund-lab-autom` handles it explicitly: the training set is restricted to orders whose window has
closed **and** which carry an actual outcome row. Everything younger is excluded as undetermined. The
same idea appears anywhere outcomes take time: churn, loan default, medical follow-up, conversion.

### Cheat 4 — using knowledge you did not have yet

Closely related, and the reason §1.6's bitemporal storage pays off a second time. When computing a
feature like "how many previous refunds has this customer had?", you must count only the refunds
**whose knowledge time preceded the order you are scoring**:

```python
known = (previous_orders.occurred_at < this_order_time) & \
        (previous_orders.label_knowledge_time <= this_order_time)
prior_refunds = previous_orders.refunded[known].sum()
```

A refund that settled *after* this order cannot have informed a prediction made *before* it. This is
called **point-in-time correctness**, and without a store that records when you knew things, you simply
cannot do it.

### Features for time series

Three families, all visible in `energy_predML`:

**Lags** — the value 1, 24, 48, 168 hours ago. For electricity, "the same hour yesterday" and "the same
hour last week" carry enormous signal, because demand is driven by daily and weekly human rhythms.

**Rolling statistics** — mean and standard deviation over the last 24 hours and 7 days. Shifted (see
above).

**Calendar features, encoded cyclically.** Hour-of-day as an integer 0–23 tells a model that 23 and 0
are 23 apart, when they are adjacent. The fix is to put the hour on a circle:

```python
df["hour_sin"] = np.sin(2 * np.pi * df["hour"] / 24)
df["hour_cos"] = np.cos(2 * np.pi * df["hour"] / 24)
```

Two columns, and now 23:00 and 00:00 are neighbours in feature space. The same treatment applies to
day-of-week, month, wind direction, angles — anything periodic.

### Always have a baseline

`energy_predML`'s baseline model is *a column*: predict that demand will equal demand at the same hour
yesterday. It is logged to the experiment tracker with the same metrics as the real model:

| Model | RMSE (MWh) | R² |
|---|---|---|
| baseline (yesterday, same hour) | 1927 | 0.31 |
| gradient boosting, default settings | 564 | 0.94 |
| the same, tuned | 501 | 0.95 |

"RMSE 501" alone means nothing. That *table* is a result. And note what it also tells you: tuning bought
11%, while *having a model at all* bought 71%. That ratio should inform where you spend your next day.

> **Jargon:** *RMSE* (root mean squared error) is the typical size of an error, in the units of the
> thing you predict — here megawatt-hours. *R²* is the share of variation explained, where 1.0 is
> perfect and 0 is no better than predicting the average. *MAPE* is the average error as a percentage.

### Train/serve skew, and the boring fix

**Train/serve skew** is when features are computed one way during training and a slightly different way
at prediction time. It is a leading cause of "the model works in the notebook and not in production",
and it is usually caused by two implementations of the same logic.

The fix is boring and total: **the serving path imports the same functions the training path uses.**
`energy_predML` does exactly that; it also defines the feature list once
(`NON_FEATURE_COLS = {"timestamp", "region", "demand_mwh"}` — everything else is a feature) and imports
that one constant into training, serving, monitoring and the dashboard.

### Tracking experiments and deploying models

**MLflow** does two jobs here (see the tool index for more):

1. **Experiment tracking** — every training run records its parameters, its metrics and the model file
   itself. You can compare runs later instead of relying on memory and filenames like
   `model_final_v2_REAL.pkl`.
2. **A model registry** — named, versioned models.

The registry is what turns retraining into deployment. `energy_predML`'s API, at startup:

```python
versions = client.search_model_versions("name='energy_demand_xgboost'")
latest = max(versions, key=lambda v: int(v.version))
model = mlflow.xgboost.load_model(f"models:/energy_demand_xgboost/{latest.version}")
rmse = mlflow.get_run(latest.run_id).data.metrics.get("rmse")   # ← note this
```

Retraining registers a new version; restarting the API picks it up. No configuration change, no file
copying.

The last line is the elegant bit: it fetches the *validation error of that specific model* and uses it
to size the prediction interval (`prediction ± 1.96 × RMSE`, the usual ~95% range under a normal-error
assumption). **The model and its uncertainty travel together** — you cannot deploy one without the
other. (The project is also honest that this assumes constant error variance, and names quantile
regression as the proper fix.)

### Knowing when the model has gone stale

The world changes; a model trained on last year's world quietly degrades. Watching for this is called
**drift monitoring**, and there are three different things people mean by it:

| Kind | What moved | How you'd notice |
|---|---|---|
| **covariate drift** | the inputs | the distribution of a feature has shifted |
| **label drift** | the outcomes | the refund rate has fallen |
| **concept drift** | the *relationship* between them | the same inputs now produce different outcomes |

Only the last one really matters, and it is the only one you cannot see without outcomes. So the honest
loop, which `energy_predML` implements, has four steps:

1. **Log every prediction you serve**, to a table keyed by what it was about.
2. **When the real outcome arrives, join it to the prediction.** Now you have *measured error*, not a
   proxy. Require a minimum number of matched pairs (48 here) before reporting anything, so you don't
   publish conclusions about noise.
3. **Define "reference" and "current" precisely.** The project writes the maximum data timestamp at
   training time to a file, and uses it as the split: reference = what the model was trained on;
   current = what has arrived since. Under this definition, running the monitor immediately after
   retraining correctly reports *no drift* — which is the right answer, and which a naive "last 20% of
   the data" split gets wrong.
4. **Act.** If the share of drifted features exceeds a **configurable** threshold, write a flag file and
   attempt a retrain.

That flag file is a small design gem. The monitoring container is deliberately built without the
training libraries (it doesn't need them and they are large). So the code does:

```python
try:
    from src.workflows.train import run_training   # present locally, absent in the slim container
    run_training(use_tuning=False)
    RETRAIN_FLAG_PATH.unlink()                     # done, clear the flag
except ImportError:
    pass                                           # the flag stays; the Makefile will see it
```

and the shell wrapper checks for the flag and runs the training container. **The same function does the
right thing in two very different environments**, and an `ImportError` is used as a deliberate
control-flow signal rather than an error. (If the retraining *fails*, the flag is deliberately left in
place so the next run tries again.)

### Evaluate honestly

`refund-lab-autom` has the sharpest line on this. Its `evaluate.py` does not report the training
holdout score:

> *"Not the training holdout — that is a self-report. This scores every order using the number that was
> actually written when the order was ingested, by whichever model was live at that moment, and
> compares it against the outcome that arrived weeks later."*

That is the number that reflects what your users actually experienced. It is almost always worse than
the holdout number, and it is the honest one.

`energy_predML` makes a parallel point with a script that compares one-hour-ahead prediction against a
**24-hour recursive forecast** — where each prediction is fed back in as an input for the next step,
because tomorrow's inputs do not exist yet. Those are very different problems, and an RMSE from the
first does not transfer to the second. **Know what horizon you actually serve.**

---

## 2.9 Giving it to people

### The problem: readers and writers collide

`DE_pipeline_stream` stores everything in a DuckDB file, which permits exactly one writer. Every five
minutes a loading task holds that lock for a few seconds. If your dashboard opens the same file, then
either the dashboard waits, or the loading waits. Neither is acceptable.

The solution is the **serving copy**, and it is a pattern worth knowing well:

```
warehouse.duckdb              raw · staging · intermediate · marts · audit     (written by the pipeline)
     │  after every successful build:
     │  1. create warehouse_serving.duckdb.tmp
     │  2. copy just the marts into it, plus a row saying when
     │  3. os.replace(tmp, warehouse_serving.duckdb)     ← atomic
     ▼
warehouse_serving.duckdb      marts only                                       (read by everyone else)
```

Three properties fall out:

- Readers **never** take the writer's lock, so they never queue and never make the pipeline wait.
- The swap is atomic, so nobody sees a half-built file.
- The copy contains **only what readers should use** — so nobody builds a report on a staging view that
  you were about to change.

The reader's half of the handshake is equally good. The dashboard caches every query **keyed on the
file's modification time**:

```python
@st.cache_data
def load(path: str, mtime: float) -> dict[str, pd.DataFrame]: ...

mtime = SERVING.stat().st_mtime
data = load(str(SERVING), mtime)
```

Cache **by data version, not by clock**. A reload costs nothing until the file actually changes. And a
small timer compares the modification time every 30 seconds and refreshes the page when a new copy
lands — so the dashboard follows the pipeline without polling the data itself.

> The project's framing is worth adopting: **a dashboard is just another reader of the serving layer.**
> Nothing about it is privileged. An analyst's notebook sits in exactly the same position, with exactly
> the same guarantees.

### Serving over HTTP

`mediaElection27` and `energy_predML` both use FastAPI. Patterns worth taking:

**Startup work belongs in a lifespan hook.** Loading a model, starting a background task — done once,
at startup, not on the first request.

**Error mapping is part of your contract.** `energy_predML`'s prediction endpoint returns `503` when no
model is loaded, `400` when there isn't enough history to build features, `500` for anything else. A
client can act on each of those; a generic `500` for all three tells them nothing.

**Auxiliary work must not fail the request.** Logging the prediction is wrapped in its own try/except:
if the log write fails, the caller still gets their prediction and a warning goes to the log. Decide
deliberately which parts of a request are essential.

**Ship the lookup tables with the data.** `mediaElection27`'s time-series endpoint returns the data
points **plus the full list of candidates and sources**. The frontend can then render names and colours
from a single request, even when the requested window is empty. One round trip instead of three, and no
"unknown candidate 7" flickering on screen.

**Put the timezone in the query** (§1.2), parameterised per request.

### What makes a chart honest

`DE_pipeline_stream`'s dashboard has the most deliberate visual design of the four, and the rules
generalise:

- **One entity, one colour, everywhere.** The three detectors keep the same colour in every chart on
  every tab. A reader learns the mapping once.
- **Colours chosen for colour-vision deficiency, in both light and dark themes**, and checked with a
  validator rather than by eye.
- **A table view under every chart.** Anyone who doubts the picture can read the numbers.
- **Explicit UTC on time axes**, so the chart does not silently re-render in each viewer's local
  timezone and produce different-looking pictures for different people.
- **A series that is almost all zeros becomes a short table, not a chart.** A line at zero with three
  spikes is a bad chart; "the three days that differ" is a good table. *Choosing not to draw a chart
  is a design decision.*
- **Ordering that carries meaning**: detectors in pipeline order; `mediaElection27` sorts candidates by
  political lean so the chart legend reads left-to-right across the spectrum.

---

# Part 3 — The craft around the data

## 3.1 Configuration

### Four things that are often confused

| | What it is | Where it lives | Changes |
|---|---|---|---|
| **code** | logic | version control | on deploy |
| **configuration** | values that differ per environment or per run | files + environment variables | per environment |
| **secrets** | credentials | a secret store, or injected at deploy | rarely, carefully |
| **state** | where the process got to | a database, a checkpoint file | constantly |

The commonest mistakes are putting secrets in code and putting state in configuration.

### Layering

The usual, and good, order is: **defaults in code → a file → environment variables → command-line
arguments**, each overriding the last. `DE_pipeline_stream`'s simulator:

```python
raw = tomllib.loads(config_path.read_text())     # 1. the file
if "SIM_SEED" in env:  raw["world"]["seed"] = int(env["SIM_SEED"])   # 2. env overrides
return Settings.model_validate(raw)              # 3. validate the result
```

That third line is the one people skip. **Validate configuration at load, not at use.** A typo in a
number should fail at startup with a clear message, not three hours later inside a loop.

### Configuration as data

`mediaElection27` takes this furthest. Which newspapers to read, which candidates to track, and which
name variants to search for — all of it lives in two YAML files:

```yaml
- id: 3
  slug: le-pen-marine
  display_name: Marine Le Pen
  aliases:
    - { alias: "Marine Le Pen", match_kind: exact }
    - { alias: "Le Pen", match_kind: wholeword, requires_context: "Marine" }
```

That last line is a *domain rule* — "only count 'Le Pen' as Marine if 'Marine' also appears" — expressed
as data rather than as an `if` statement. Adding a candidate, fixing a feed URL, or refining a
disambiguation rule is now an edit to a file, not a code change and a deployment.

The loader has one more good property: **aliases are deleted and re-inserted per candidate**, so
removing a line from the file removes it from the database. With an upsert-only loader, deleted config
lines live forever and nobody can work out why.

### The duplicated-constant problem, and a rule of thumb

`DE_pipeline_stream` has a value — how often stations report, five minutes — that must be known by the
simulator, by the stream processor, and by the transformation layer. Three different runtimes; they
cannot import each other.

The project's answer: duplicate it, and **comment every copy with where its twins are**:

```toml
# config/source.toml
status_interval_min = 5   # the Spark analyzer needs this too: compose → STATUS_INTERVAL_MIN
```
```yaml
# dbt_project.yml
status_interval_min: 5    # = config/source.toml [emission].status_interval_min
```

And the documentation has a table headed *"I want to change X → edit Y → also keep Z in sync"*.

Contrast `energy_predML`, where everything runs in one Python process and the feature list is defined
**once** and imported by training, serving, monitoring and the dashboard.

> **Rule of thumb: if two runtimes can import the same constant, define it once. If they cannot,
> duplicate it and comment both ends.** What you must never do is duplicate it silently.

---

## 3.2 Environments and packaging

### Why "it works on my machine" happens

Because your machine has a particular Python, particular library versions, particular system libraries,
and particular environment variables — and none of that is written down anywhere.

The fixes, in increasing order of strength:

1. **A virtual environment** — a per-project directory of installed libraries, so projects don't
   collide.
2. **A lockfile** — a file recording the *exact* version of every library, including the libraries your
   libraries need. `requirements.txt` with `>=` is not a lockfile; `uv.lock` is.
3. **Pinning across boundaries.** `DE_pipeline_stream` pins the DuckDB version to be identical in the
   scheduler's container and in the developer's environment — because *the database file format must
   match*. A comment says so in both places. This is the sort of coupling that produces a baffling
   error a month later.
4. **A container** — the whole environment, including the operating system layer, described in a file.

### uv workspaces

`DE_pipeline_stream` has several Python components that share dependencies and are deployed separately.
A **workspace** is one project containing several packages with **one shared lockfile**. Each service's
container then installs only its own package:

```dockerfile
COPY pyproject.toml uv.lock ./           # 1. dependency descriptions only
COPY source/pyproject.toml source/
RUN uv sync --frozen --package bikeshare-sim --no-install-workspace   # ← cached layer
COPY source/ source/                     # 2. now the code
RUN uv sync --frozen --package bikeshare-sim --no-editable
```

Two things at once. The **layer split** means changing your code doesn't re-download the internet
(Docker caches each step, and only steps after a changed file re-run). And `--frozen` with the shared
lockfile means **the container runs exactly the versions your tests ran against.**

### Containers, briefly

- An **image** is a frozen filesystem plus a default command. It is built from a `Dockerfile` and does
  not change.
- A **container** is a running instance of an image. It is disposable.
- A **volume** or **bind mount** is storage that survives the container. A bind mount maps a directory
  on your machine into the container (good for data you want to look at); a named volume is managed by
  Docker (good for things like database internals).
- **Port mapping** (`8000:8000`) exposes a port inside the container on your machine.
- **Docker Compose** describes several containers, their configuration and their relationships in one
  YAML file.

Compose details from `DE_pipeline_stream` worth copying:

**Profiles** mark optional parts of the stack. `COMPOSE_PROFILES=stream,batch` runs everything;
clearing it runs only the core. On a laptop with limited memory this is the difference between the
project being usable and not.

**Health checks and ordered startup.** A service can declare that it waits for another to be *healthy*
(passing its health check) or *completed successfully* (for one-shot setup jobs). Otherwise everything
starts at once and half of it crashes trying to connect to something not yet listening.

**Run as your own user.** `user: "${HOST_UID}:${HOST_GID}"` means files the container writes into a
mounted directory belong to *you*, not to root. Without it you get root-owned files you cannot delete
without `sudo` — a small thing that wastes a surprising amount of time.

**Anchors to avoid repetition.** YAML lets you define a block once and merge it into several services
(`x-airflow-common: &airflow-common`, then `<<: *airflow-common`). Airflow needs four services sharing
the same image, environment and mounts; this declares that once.

### Per-service images

`energy_predML` builds four images from four requirements files: ingestion, training, monitoring,
serving. The serving image excludes the tuning and monitoring libraries; the monitoring image excludes
*all* the machine-learning libraries.

The obvious benefit is size. The non-obvious one, already met in §2.8, is that this makes the
`except ImportError` branch a **designed code path** rather than an accident. The shape of your
deployment became part of your control flow, deliberately.

### The comment about absolute paths

`energy_predML` has a short document about one infrastructure problem that is a good model for an
internal design note. In summary: MLflow's file-based storage records the **absolute path** of where a
model was saved. Train on your laptop, try to read it from a container, and the path doesn't exist.
Switching to a database-backed store plus a flag that makes the server proxy all file transfers means
paths never appear in the metadata at all, and the same address works from anywhere.

The generalisable lesson: **anything that stores an absolute path in its metadata will break the moment
two machines are involved.** That includes notebooks, pickled objects with file references, and
configuration files with `/home/yourname/` in them.

---

## 3.3 Testing

### The four kinds, and what each actually catches

| Kind | Scope | Catches | Typical speed |
|---|---|---|---|
| **unit** | one function, no I/O | logic errors, edge cases | milliseconds |
| **integration** | several real components, fake edges | wiring errors, wrong assumptions about a library | seconds |
| **data tests** | assertions about the data itself, in production | *the data* being wrong | runs with the pipeline |
| **end-to-end / smoke** | the whole running system | "is it actually working right now?" | minutes |

They catch **different** things. A perfect unit test suite will not tell you that your two components
disagree about a date format. Data tests will not tell you your function is wrong; they will tell you
today's data is.

### Isolate the database first

The single most valuable fixture in `energy_predML`:

```python
@pytest.fixture
def tmp_duckdb(tmp_path, monkeypatch):
    """Redirect DuckDB to an isolated temp file so tests never touch data/energy.duckdb."""
    db = tmp_path / "test.duckdb"
    monkeypatch.setattr("src.data.store.DB_PATH", db)
    return db
```

Four lines, and now **no test can ever touch your real data**, however wrong it is. Every project here
has an equivalent. A companion fixture then seeds 400 hours of synthetic demand — enough history for
the 168-hour lag features to be non-null — so tests have realistic data without a network call.

### Choose the mocking boundary deliberately — and write down why

A **mock** is a stand-in for a real component. The question is always *what* to replace, and these
projects each answer it explicitly in a docstring:

| Test | What is faked | What is real | The stated reason |
|---|---|---|---|
| `energy_predML` API tests | the model load and the model | the whole feature-generation path, against a temp database | test the API's behaviour, not XGBoost's |
| `refund-lab-autom` monitoring tests | the drift report (returns a controlled drift share) | the database and data access | the test is about *flag-writing logic*, not about Evidently |
| `DE_pipeline_stream` bridge tests | the network (a fake HTTP transport replaying scripted streams) and the message queue (a fake producer that acknowledges on flush and can be told to fail specific topics) | everything else | test the bridge end-to-end with no infrastructure |

The consistent principle: **mock the network and the external service; never mock your own data layer
or your own transformations.** If you mock your transformation, you are testing your mock.

Note too that the fake producer can be *told to fail*. Testing the happy path is easy; a fake that
takes a `fail_topics` argument lets you test what happens when delivery fails — which is where the
interesting logic is.

### Control time instead of waiting for it

`DE_pipeline_stream` tests a simulation that advances with the clock. It does not sleep. It injects a
fake clock:

```python
class FakeWall:
    def __init__(self): self.t = 0.0
    def __call__(self): return self.t

def run_for(sim, **delta):
    sim.clock.advance(timedelta(**delta))
    while sim.catch_up(max_steps=10_000):
        pass
```

Six simulated hours pass in milliseconds, deterministically. **Any component whose behaviour depends on
time should take its clock as a parameter.** It costs nothing and turns a class of untestable code into
a testable one.

### Test the wiring, not only the units

A small but excellent observation from `refund-lab-autom`. Four lines of glue code — folding evidence
from one channel into the structure the monitors read — were extracted into a named method, and the
comment says why:

> *"This is a separate method rather than four lines inside `cycle` for one reason: those four lines
> are the wiring, the wiring is what failed once already, and wiring you cannot call from a test is
> wiring you are not testing."*

### One end-to-end check that follows the data

`DE_pipeline_stream`'s smoke test checks the **running** system in pipeline order, stopping at the
first broken link:

```
1 source     healthy, and not paused
2 queue      new messages in both event topics
3 bridge     a deliberately triggered malformed event reaches the dead-letter queue
4 processor  all four streaming jobs are committing; a triggered impossible trip raises an alert
5 scheduler  the pipelines run; the transformation follows
6 warehouse  the alert, the rejected message and both injected faults are in the raw tables;
             the trip is flagged in the fact table; the serving copy was republished, not empty
```

Two design decisions are as instructive as the test:

- **It never starts, stops or resets anything.** The note is memorable: *"a target that resets on its
  own would wipe your world by accident."*
- **Its two deliberately triggered faults go into the ground-truth record like any other**, so the
  quality report stays honest. A test that quietly excludes its own side effects from measurement is a
  test that corrupts the measurement.

---

## 3.4 Version control, CI/CD and infrastructure

### Continuous integration

**CI** means: on every push, a machine that is not yours checks out the code and runs the checks. The
value is that it runs on a *clean* machine, so it catches "works because of something only on your
laptop".

`mediaElection27`'s pipeline is a good minimal shape:

```
ruff check .      # style and common mistakes
mypy app          # type checking, in strict mode
pytest tests/unit # ONLY the unit tests
```

That last restriction is deliberate and correct: **unit tests must not require a database**, so the
integration tests are excluded from CI and skip themselves locally when PostgreSQL is not reachable.
A test suite that cannot run without infrastructure is a test suite people stop running.

> **Jargon:** a *linter* (ruff) reads your code and flags style problems and likely bugs without running
> it. A *type checker* (mypy) verifies that the types you declared are consistent — `--strict` means it
> insists you declare them.

### Deployment without long-lived keys

`mediaElection27` deploys on every push to the main branch, and it does so **without storing any cloud
credentials**. The mechanism is **OIDC federation**: the CI system proves its identity with a
short-lived token, and the cloud account is configured to trust tokens saying "this specific repository,
on this specific branch". It then issues temporary credentials.

Why care: a stored cloud key is a permanent secret that can leak, must be rotated, and grants access
from anywhere. A federated token is valid for minutes and only for the workflow that requested it.

The deployment is also in **two phases**, for a reason worth understanding: the frontend needs the
API's address, and the address only exists after the API is deployed. So: deploy the backend → read the
resulting URL from the cloud's own outputs → build the frontend with that URL compiled in → deploy the
frontend. That ordering constraint is visible in the workflow file, which is where it belongs.

### Infrastructure as code

Writing infrastructure as code means your cloud resources are described in files in version control
rather than created by clicking. `mediaElection27` uses AWS CDK, where you write Python and it generates
the cloud provider's native template.

The mental model, which its notes state clearly, is **three artefacts with three destinations**:

```
your Python   ──(synth)──►   a template   ──(deploy)──►   real resources
(in your repo)                (generated)                  (in the cloud)
```

- Generating the template is **free and local**. Iterate as much as you like.
- The template is the lingua franca the cloud actually reads.
- **State lives in the cloud, not on your laptop.** Deploying computes a difference against what exists
  and changes only what differs.

And the trap to know before you meet it:

> **Renaming a construct in your code changes its logical identifier, so the cloud concludes the old
> resource disappeared, and destroys and recreates it.** Described in the project's notes as "the #1
> way to accidentally drop a production database."

The other one, learned in production and recorded: **a value exported from one stack cannot be updated
while another stack is using it.** Changing a shared component's identifier caused the exporting stack
to roll back while the consumers stayed pinned to the old one — resolvable only by deleting the
consumers first, in dependency order. The suggested long-term fix is to publish such values into a
parameter store and have consumers *read* them, rather than creating a hard export/import link.

---

## 3.5 Cost and running unattended

### Cost as an architectural constraint

`mediaElection27`'s budget is under five dollars a month, and the beautiful thing is that you can follow
the consequences link by link:

```
under $5/month
  → no NAT gateway (about $32/month on its own)
      → the private network has no route to the internet at all
          → the component that fetches news feeds must live OUTSIDE that network
            (cloud functions get free internet access when not attached to a private network)
          → everything that touches the database lives INSIDE it,
            reaching file storage through a free "gateway endpoint"
              → the secrets service is also unreachable from inside
                (reaching it needs a private endpoint at about $14.60/month)
                  → database credentials are injected as plain environment variables,
                    resolved by the deployment system at deploy time
                      → an explicitly documented and accepted security trade-off
```

Every step is a reasonable local decision; together they produce an architecture nobody would design
from first principles, and the notes say so. This is what real engineering under constraint looks like,
and being able to *read* such a chain is a genuine skill.

The guardrails are as instructive as the architecture: budget alarms at 80% and 100%, automatic
lifecycle rules moving old files to cold storage after 30 days and deleting them after a year, a
one-week log retention on every function, and everything tagged so spend can be attributed.

### Running unattended

`refund-lab-autom` ran for hours with nobody watching. What that requires:

**Idempotency, so a restart resumes rather than repeats** (§1.4). The restart script is four lines and
relies entirely on this.

**Distinguish "the service is broken" from "the service is gone".** The project's helper checks whether
anything is listening on the port at all before restarting the supplier process. Its comment is the
whole idea:

> *"A multi-hour outage is a fault to be logged; a dead process is an operating problem to be fixed,
> and an unattended run has to be able to tell the difference and survive both."*

Confusing the two means either restarting things that are fine, or waiting forever for something that
will never come back.

**Report the failure that prevents reporting.** A run that dies in the HTTP client never reaches its
monitors — so if a sustained outage killed the run, nothing would ever record the outage. The project
therefore also raises that particular incident **from the exception handler**. Ask yourself: *what
failure would prevent my failure detection from running?*

**Never let a monitor break the loop.** Every detector is called inside a `try/except` that logs and
continues. A crash in the twelfth of seventeen checks must not stop the other five, nor the pipeline
they are watching.

**Sample, when you cannot observe continuously.** Compressing a simulated day into one pass means you
touch the supplier once per day, and would step straight over a multi-hour outage. Advancing in three
steps and probing at each costs two extra requests and turns a blind spot into a known sampling rate —
which the project then *states* as a limitation rather than pretending it isn't one.

---

## 3.6 Writing it down

These four repositories document themselves unusually well, and the documents fall into six distinct
genres. Together they are a set worth copying.

| Genre | What it must contain | Example |
|---|---|---|
| **Plan of record** | the original request *verbatim*, the decisions taken and by whom, the architecture, the build order | `DE_pipeline_stream/PLAN.md` |
| **Build log** | written **during** the work: what was built, what was *measured*, every deviation with its reason, every bug at full length | `DE_pipeline_stream/logging_build.md`, `refund-lab-autom/NOTES.md` |
| **Guided tour** | a wiring map, an "I want to change X → edit Y" table, a reading order, and **experiments with their measured outcomes** | `DE_pipeline_stream/TOUR.md` |
| **Mental models** | the transferable concepts, deliberately separated from this codebase | `mediaElection27/dev/MENTAL-MODELS.md` |
| **Focused design note** | one problem, the target design, why it works, a stepwise plan | `energy_predML/STRATEGY.md` |
| **Coverage table** | every case × the mechanism that handles it × **the risk of missing it** | `refund-lab-autom/NOTES.md` §13 |

Four conventions from `DE_pipeline_stream` that cost almost nothing and pay constantly:

1. **Every configuration file starts with a header saying who reads it and what it wires.**
2. **Every wiring point is commented on both ends.** `KAFKA_BOOTSTRAP=kafka:9092` in the compose file
   carries a comment, and so does the line in the Python module that reads it.
3. **Every entry-point module's docstring states** its role in the flow, what is upstream and
   downstream, and how to run it on its own.
4. **Put the failure table in the docstring of the module that handles the failures.** The bridge's
   docstring lists, in a table: source unreachable, gap notice, source was reset, delivery not
   acknowledged, shutdown signal — and what it does about each. That table is the design.

And the most valuable habit of all, present in all four: **record what you measured, including the
numbers that made you look bad, with the date.** "Thirty-minute windows gave 8% precision; two-hour
windows gave 86%" is worth more than any amount of prose about window sizing. So is *"I keyed everything
to the start of the world without checking whether the dimension predated it. Ten seconds of `curl`
would have shown it — and in fact the very first probe I ran did show it. I had the evidence and did
not use it."*

---

# Part 4 — Twenty things worth remembering

1. **Name the promise at every hop.** At-most-once, at-least-once, exactly-once, best effort. Write the
   table down.
2. **Keep the original.** Land raw bytes, type later. Schema drift should break a model, never an
   ingestion.
3. **Make re-running free.** Natural key plus a conflict rule, or a version-guarded update, or build-
   and-swap. Design it in; don't hope for it.
4. **Store your position with the data it covers**, in the same transaction. That is what
   exactly-once actually means in practice.
5. **Resetting a job means resetting its progress *and* its outputs**, together.
6. **Name your clocks.** Event, emission, ingestion, landing, processing — and, when the source
   changes its mind, valid time versus knowledge time.
7. **Incremental loads filter on landing time, not event time**, or late data is lost silently.
8. **Never delete; tombstone.** Deletion destroys the evidence of deletion.
9. **Keep history for anything you will judge past events against** — SCD2 and the as-of join.
10. **Syntax at ingestion, semantics downstream.**
11. **If you cannot reject, repair *and record*.** The repair is the evidence. Repair without recording
    is corruption you performed yourself.
12. **A check nobody trusts is worse than no check.** Prefer checks whose false-positive rate is zero
    by construction. Deduplicate alerts by cause. Retract with reasons when you were wrong.
13. **Two detectors naming one event is not extra coverage — it is a guaranteed false positive.**
14. **Shift your lag features. Split time-series temporally. Keep a baseline that is one column.**
15. **Absence of a label is not a negative label**, and a feature must be as-of the moment of decision.
16. **Log every prediction you serve**, then join it to reality when reality arrives. That is the only
    honest measure of a deployed model.
17. **Readers should never take the writer's lock.** Publish a copy; swap it atomically; cache by data
    version, not by clock.
18. **Keep business logic out of the orchestrator**, so you can test it without one.
19. **Measure before you design, and write down what you measured** — including the results that made
    you look bad.
20. **Comment both ends of every wire.** If changing X requires changing Y, say so in both places.

---

**Next:** [`08-TOOLS-AND-CONCEPTS.md`](08-TOOLS-AND-CONCEPTS.md) — a long index of every tool and term
used above: what it is, what problem it solves, what using it looks like, and where in these four
projects to see it.