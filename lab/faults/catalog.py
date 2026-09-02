"""The published fault catalogue.

This file is PUBLIC.  Knowing that a fault *can* happen is not the same as
knowing that it *did*: which subset was drawn for this run, and when each one
fires in simulated time, lives only in the sealed ledger.

Each entry is ``(family, group, description)``:

* ``family`` is the fine-grained classification, and is what a well-written
  incident report should aim for.
* ``group`` is either ``"data"`` (the record is wrong) or ``"operational"``
  (the record may be fine but the delivery of it was not).  Getting the group
  right earns partial credit from the grader.
"""

from __future__ import annotations

# (family, group, description)
CATALOG: dict[str, tuple[str, str, str]] = {
    # -- data: ingestion --------------------------------------------------
    "dup_exact_payments": (
        "ingestion", "data",
        "Write-retry produced byte-identical duplicate payment rows.",
    ),
    "dup_near_orders": (
        "ingestion", "data",
        "Same basket submitted twice seconds apart under different order_ids.",
    ),
    "mojibake_city": (
        "ingestion", "data",
        "UTF-8 text decoded as latin-1 somewhere upstream.",
    ),
    "id_leading_zeros": (
        "ingestion", "data",
        "A join key was round-tripped through an integer type.",
    ),
    "field_rename_midstream": (
        "ingestion", "data",
        "A JSON field changed name partway through the feed.",
    ),
    # -- data: time -------------------------------------------------------
    "tz_mixed": (
        "time", "data",
        "Timestamps stop being naive and start carrying offsets (or vice versa).",
    ),
    "late_arriving_events": (
        "time", "data",
        "Events for day D only become knowable several days later.",
    ),
    "dst_duplicate_hour": (
        "time", "data",
        "A wall-clock hour repeats / is missing around a DST boundary.",
    ),
    # -- data: semantic ---------------------------------------------------
    "unit_switch_price": (
        "semantic", "data",
        "A money column silently changes unit at a date boundary.",
    ),
    "category_rename": (
        "semantic", "data",
        "A categorical value is renamed mid-stream, splitting one class in two.",
    ),
    "new_category_late": (
        "semantic", "data",
        "A category value that exists only in the most recent period.",
    ),
    "sentinel_values": (
        "semantic", "data",
        "Magic values standing in for NULL (-999, 1900-01-01, 'N/A', 'null').",
    ),
    # -- data: statistical ------------------------------------------------
    "covariate_shift": (
        "statistical", "data",
        "Feature distribution differs between early and late periods.",
    ),
    "mnar_missing": (
        "statistical", "data",
        "Missingness in a column depends on the outcome itself.",
    ),
    "label_censoring": (
        "statistical", "data",
        "Labels near the window edge cannot have matured; absence != negative.",
    ),
    # -- data: leakage / integrity ----------------------------------------
    "leakage_column": (
        "leakage", "data",
        "A column present at training time that is unavailable at prediction time.",
    ),
    "survivorship_customers": (
        "integrity", "data",
        "Rows hard-deleted from a dimension, orphaning historical facts.",
    ),
    # -- operational ------------------------------------------------------
    "backfill_restatement": (
        "restatement", "operational",
        "Historical rows are revised weeks after the fact, silently.",
    ),
    "late_partition": (
        "delivery", "operational",
        "A simulated day's drop file arrives one or two days late.",
    ),
    "missing_delivery": (
        "delivery", "operational",
        "A day's file never arrives. Looks like a quiet Sunday.",
    ),
    "duplicate_delivery": (
        "delivery", "operational",
        "The same payload is delivered twice under different filenames.",
    ),
    "partial_write": (
        "delivery", "operational",
        "A drop file is truncated mid-row.",
    ),
    "column_added": (
        "schema", "operational",
        "A new column appears in the drop file at a date boundary.",
    ),
    "column_reordered": (
        "schema", "operational",
        "Drop file columns are reordered at a date boundary.",
    ),
    "encoding_change": (
        "encoding", "operational",
        "A drop file switches from UTF-8 to cp1252.",
    ),
    "compression_change": (
        "encoding", "operational",
        "A drop file starts (or stops) being gzipped, extension unchanged.",
    ),
    "clock_skew": (
        "time", "operational",
        "Source timestamps drift by minutes, then correct.",
    ),
    "sustained_outage": (
        "availability", "operational",
        "The API returns 503 for a multi-hour simulated window.",
    ),
    "silent_row_deletion": (
        "integrity", "operational",
        "Rows disappear from the source with no tombstone.",
    ),
    "id_reuse": (
        "integrity", "operational",
        "A retired identifier is reassigned to a different entity.",
    ),
}

# Present in every run, because the skills they teach are the point of the lab.
ALWAYS_ON = ["label_censoring", "backfill_restatement"]

SAMPLEABLE = [k for k in CATALOG if k not in ALWAYS_ON]

FAMILIES = sorted({v[0] for v in CATALOG.values()})
GROUPS = sorted({v[1] for v in CATALOG.values()})


def family_of(fault_id: str) -> str:
    return CATALOG[fault_id][0]


def group_of(fault_id: str) -> str:
    return CATALOG[fault_id][1]


def describe(fault_id: str) -> str:
    return CATALOG[fault_id][2]
