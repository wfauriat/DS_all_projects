# Database schema — mediaElection27

Postgres 16. All timestamps `TIMESTAMPTZ` stored in UTC.

Source of truth: [`app/db/models.py`](app/db/models.py) (SQLAlchemy ORM) and [`app/db/migrations/versions/`](app/db/migrations/versions/) (alembic).
Inspect live: `make psql` then `\dt` / `\d <table>`.

---

## Entity-relationship overview

```
                    sources                              candidates
                       │                                     │
                       │ 1                                 1 │
                       │                                     │
                       ▼ N                                 N ▼
   ingest_runs ◀── (source_id, nullable)         candidate_aliases
                       │                                     ▲
                       │ 1                                   │
                       ▼ N                                   │
                    articles                                 │
                       │                                     │
                       │ 1                                   │
                       ▼ N                                   │
                    mentions ────────── (candidate_id) ──────┘
```

---

## `sources` — the RSS feed registry

| col | notes |
|---|---|
| `id` SMALLINT PK, hand-assigned | stable across reseeds; YAML defines them |
| `slug` UNIQUE | machine-friendly, used in URLs/logs (`lemonde-politique`) |
| `outlet`, `section` | "Le Monde" / "politique" — outlet groups multiple feeds |
| `feed_url` | RSS endpoint; can be hot-swapped without code change |
| `lean` | `left` … `right`, `public`, `sovereigntist` — for political-balance views |
| `is_active` | toggle ingestion off without deleting (e.g. Les Échos, La Croix) |

---

## `articles` — one row per fetched article

| col | notes |
|---|---|
| `id` BIGSERIAL PK | autoincrement |
| `source_id` FK → sources | which feed it came from |
| `guid` | feed-supplied unique id (RSS `<guid>`) |
| `url` | **canonicalised** (utm_*/fbclid/fragment stripped) before storage |
| `title`, `summary` | what we display + what the extractor scans |
| `published_at` TIMESTAMPTZ | always UTC; falls back to channel `lastBuildDate` (e.g. Le Parisien) |
| `fetched_at` | when *we* ingested it (defaults `now()`) |
| `lang` | `'fr'` default; future-proofs for multi-language |
| `raw` JSONB | full parsed feedparser entry — lets us reprocess without re-fetching |
| `content_hash` BYTEA | `sha256(lower(strip(title)) || lower(strip(summary)))`; **not unique** (intentional) |

**Constraints**: `UNIQUE (source_id, guid)` — same item refetched from same feed silently skipped.
**Indexes**: `(published_at DESC)`, `(source_id, published_at DESC)`, `(content_hash)` — drives the time-series query and wire-detection.

---

## `candidates` — 2027 presidential candidate registry

| col | notes |
|---|---|
| `id` SMALLINT PK, hand-assigned | YAML-driven |
| `slug`, `display_name`, `party`, `lean` | standard registry fields |
| `declared_at` DATE | when they entered the race; NULL = speculative |
| `eligible` BOOLEAN | `false` for Le Pen; dashboard shows them in a separate sub-panel |
| `notes` TEXT | free-form (citations, context) |

---

## `candidate_aliases` — name variants the matcher looks for

| col | notes |
|---|---|
| `candidate_id` FK → candidates ON DELETE CASCADE | aliases die with the candidate |
| `alias` | the surface form to match (`Mélenchon`, `JLM`, `Jean-Luc Mélenchon`) |
| `match_kind` | `exact` / `wholeword` / `regex` |
| `requires_context` | secondary token that must co-occur — handles Le Pen ambiguity (`alias=Le Pen, requires_context=Marine`) |
| `is_active` | turn an alias off without deleting (e.g. if it produces too many false positives) |

**Constraint**: `UNIQUE (candidate_id, alias)`.

---

## `mentions` — extractor output, the analytical heart

| col | notes |
|---|---|
| `article_id` FK → articles ON DELETE CASCADE | drop article ⇒ drop its mentions |
| `candidate_id` FK → candidates | who got mentioned |
| `field` | `'title'` or `'summary'` — where in the article |
| `match_text`, `start_offset`, `end_offset` | exact span — useful for highlight UI later |
| `extractor` | `'keyword-v1'`, eventually `'spacy-fr-core-news-md'`, `'ner-v1'` |
| `extractor_version` | semver-like; lets you A/B two extractors side-by-side |
| `confidence` REAL | 1.0 for keyword; <1.0 for NER models |
| `attributes` JSONB | NER-specific extras (entity type, surrounding context, model logits) — **schema doesn't change when NER lands** |

**Constraint**: `UNIQUE (article_id, candidate_id, field, start_offset, extractor, extractor_version)` — exact-position dedup, but the same article can be reprocessed by a new extractor version without conflict.
**Indexes**: `(candidate_id)`, `(article_id)`, GIN on `attributes` (Phase 2).

---

## `ingest_runs` — observability log

| col | notes |
|---|---|
| `id` BIGSERIAL PK | one row per (source × ingest pass) |
| `source_id` FK, **nullable** | nullable so cross-source orchestration runs can also be logged |
| `started_at`, `finished_at` | bracket timing |
| `status` | `running` / `ok` / `partial` / `failed` |
| `feed_http_status` | the HTTP code we got |
| `n_items_seen`, `n_articles_inserted`, `n_articles_skipped_dup`, `n_mentions_inserted` | counters |
| `error` | stack trace string when status=failed |
| `meta` JSONB | etag, last-modified, elapsed_ms, the feed_url at the time of fetch |

**Index**: `(started_at DESC)` — drives the "show me the last N runs" admin view.

---

## Dedup matrix

| Scenario | Caught? | By what |
|---|---|---|
| Same item refetched from the same feed | always | `UNIQUE (source_id, guid)` + `ON CONFLICT DO NOTHING` |
| Same article with tracking params (utm_*, fbclid, etc.) | always | URL canonicalisation in `dedup.canonical_url` before storage |
| Same article republished in **two feeds of the same outlet** (e.g. France Info politique + élections) | **kept as separate rows** | by design — each feed pickup is a coverage event |
| Same wire (AFP-style) republished by **multiple outlets** | kept as separate rows | by design — same reason |
| Same article, **same feed**, but the outlet regenerated/fixed the GUID | **NOT caught** — would duplicate | nothing; rare in practice but a real gap |
| Cosmetic differences (smart quotes, HTML entities, NBSP) | partially — content_hash uses lower+strip but no Unicode normalisation | content_hash if exact, slips through if not |

For "unique stories per outlet" analytics, always `COUNT(DISTINCT content_hash)` per outlet — never `COUNT(*)`.

---

## What this schema gives you

- **Stable analytics primitive**: `mentions × articles × sources × candidates` join answers all the dashboard questions (e.g. "how often did Le Monde mention Bardella in April?", "which candidates dominated in Mediapart vs Le Figaro?").
- **Forward-compatible NLP**: when keyword → NER, no migration. Just a new `extractor`/`extractor_version` writing into the same `mentions` table; the old keyword rows stay for comparison.
- **Reproducibility**: `articles.raw` + `ingest_runs` mean you can reconstruct any past fetch state without going back to the source feeds.
- **Operational hygiene**: every fetch is logged with timing + HTTP code, so you can spot a feed silently breaking before its mention counts collapse.
- **Config-as-data**: `sources` and `candidates` are reseeded from YAML — adding a new candidate or hot-swapping a feed URL is an edit + `make seed`, never a deploy.

---

## Migration history

| revision | description | file |
|---|---|---|
| `d66ca171650a` | initial schema (all 6 tables) | `app/db/migrations/versions/d66ca171650a_initial_schema.py` |
| `98d3055b30f7` | drop `UNIQUE (articles.content_hash)`, replace with non-unique index | `app/db/migrations/versions/98d3055b30f7_drop_articles_content_hash_unique_.py` |

Apply with `make migrate` (= `alembic upgrade head`). View chain with `.venv/bin/alembic history`.
