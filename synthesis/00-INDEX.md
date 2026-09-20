# Synthesis set — index

Produced from a full read of the four codebases (`refund-lab-source` was consulted for context only,
as instructed, and has no document of its own).

The set has two halves. **01–06** are dense and assume you know the stacks. **07–08** explain the same
material from scratch, using the codebases as pretexts for the underlying ideas.

| # | Document | What it is | Audience |
|---|---|---|---|
| 01 | [`01-DE_pipeline_stream.md`](01-DE_pipeline_stream.md) | technical synthesis: Kafka + Spark + Airflow + DuckDB + dbt streaming reference stack | expert / LLM |
| 02 | [`02-mediaElection27.md`](02-mediaElection27.md) | technical synthesis: deployed AWS analytics product under a $5/month budget | expert / LLM |
| 03 | [`03-energy_predML.md`](03-energy_predML.md) | technical synthesis: full MLOps lifecycle with drift-triggered retraining | expert / LLM |
| 04 | [`04-refund-lab-autom.md`](04-refund-lab-autom.md) | technical synthesis: bitemporal warehouse + detector suite against a hostile vendor | expert / LLM |
| 05 | [`05-CROSS-ANALYSIS.md`](05-CROSS-ANALYSIS.md) | **pass 2** — what is common across all four, what is specific to each and why, plus six deliberate contradictions | expert |
| 06 | [`06-COMPANION.md`](06-COMPANION.md) | **pass 3** — Part I: the skills/idiom catalogue along the pipeline. Part II: per-project navigation charts (entry points, architecture, module map, stack, reading path) | practitioner |
| 07 | [`07-LEARNING-GUIDE.md`](07-LEARNING-GUIDE.md) | **the verbose pass** — the ideas explained from first principles, with the four projects as worked examples. Jargon is introduced and defined as it appears | learner |
| 08 | [`08-TOOLS-AND-CONCEPTS.md`](08-TOOLS-AND-CONCEPTS.md) | the verbose index: *what is dbt? what is a watermark?* Every tool and term, with what problem it solves and where to see it working | learner / reference |

## Suggested order

- **New to the field, or to part of it:** 07 straight through, dipping into 08 whenever a name is
  unfamiliar. Then open the projects using the charts in 06 Part II.
- **Comfortable with the stacks:** 06 Part I → 05 → then 01–04 as needed.
- **Working on one project:** its chart in 06 Part II → its document (01–04) → the project's own docs.
- **Deciding an approach on a new project:** 05 Part D (the six deliberate contradictions), 06 §12,
  and the tool-choice table at the end of 08.

## Conventions

Shorthand used in 05, 06 and 08: **DE** = `DE_pipeline_stream`, **ME** = `mediaElection27`,
**EP** = `energy_predML`, **RL** = `refund-lab-autom`.

## Structure of the learning guide (07)

```
Part 0  Four worlds                     the projects as stories, and where each puts the difficulty
Part 1  Seven ideas everything rests on promises · clocks · keep the original · idempotency ·
                                        positions · belief and restatement · what quality means
Part 2  Walking the pipeline            ingestion · contracts · storage · transformation ·
                                        orchestration · streaming · observation · ML · serving
Part 3  The craft around the data       configuration · environments · testing · CI/CD and
                                        infrastructure · cost and unattended operation · documentation
Part 4  Twenty things worth remembering
```
