"""The bitemporal truth store.

Every fact carries two timestamps:

* a **valid time** -- when the thing happened in the world;
* a ``knowledge_time`` -- when this source system could first have told you
  about it.

Reads are always ``WHERE knowledge_time <= :as_of``.  A restatement is a new row
with the same business key, a higher version and a later ``knowledge_time``; the
visible row for a key is the highest version that was knowable at ``as_of``.
Rows can also stop being visible without any tombstone reaching the consumer.

The store holds columns that the source could never expose.  They are prefixed
with an underscore and, more importantly, are excluded by the explicit
allowlists below.  The allowlist is the enforcement point: a column added to a
table later is invisible until someone deliberately adds it here.
"""

from __future__ import annotations

import datetime as dt
import json
import sqlite3
import threading
from dataclasses import dataclass
from pathlib import Path

from lab import config as cfgmod
from lab.generator import build_world

# ---------------------------------------------------------------------------
# table shapes
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class TableSpec:
    name: str
    key: tuple[str, ...]      # partition for version resolution
    valid: str                # the valid-time column, and the pagination order
    columns: tuple[str, ...]  # physical columns, in insert order


SPECS: dict[str, TableSpec] = {
    "customers": TableSpec(
        "customers", ("customer_id", "valid_from"), "valid_from",
        ("customer_id", "version", "valid_from", "knowledge_time", "_deleted",
         "signup_date", "country", "city", "segment", "lifetime_value_cents",
         "is_active"),
    ),
    "orders": TableSpec(
        "orders", ("order_id",), "occurred_at",
        ("order_id", "version", "occurred_at", "knowledge_time", "_deleted",
         "customer_id", "channel", "payment_method", "shipping_speed",
         "order_total_cents", "items",
         "_prior_orders", "_top_cat", "_refund_propensity", "_true_refund",
         "_refund_ts", "_support_tickets"),
    ),
    "payments": TableSpec(
        "payments", ("payment_id",), "occurred_at",
        ("payment_id", "version", "occurred_at", "knowledge_time", "_deleted",
         "order_id", "customer_id", "amount_cents", "status", "processor",
         "billing_city"),
    ),
    "events": TableSpec(
        "events", ("event_id",), "occurred_at",
        ("event_id", "version", "occurred_at", "knowledge_time", "_deleted",
         "customer_id", "session_id", "event_type", "sku"),
    ),
    "labels": TableSpec(
        "labels", ("order_id",), "resolved_at",
        ("order_id", "version", "resolved_at", "knowledge_time", "_deleted",
         "refund_at", "refunded_within_30d"),
    ),
    "products": TableSpec(
        "products", ("sku",), "active_from",
        ("sku", "version", "active_from", "knowledge_time", "_deleted",
         "category", "product_name", "list_price_cents"),
    ),
}

_COL_TYPES = {
    "version": "INTEGER NOT NULL",
    "_deleted": "INTEGER NOT NULL DEFAULT 0",
    "lifetime_value_cents": "INTEGER",
    "is_active": "INTEGER",
    "order_total_cents": "INTEGER",
    "amount_cents": "INTEGER",
    "list_price_cents": "INTEGER",
    "refunded_within_30d": "INTEGER",
    "_prior_orders": "INTEGER",
    "_true_refund": "INTEGER",
    "_support_tickets": "INTEGER",
    "_refund_propensity": "REAL",
}

# ---------------------------------------------------------------------------
# serialisation allowlists -- ADD, never subtract.  Anything not named here is
# unreachable from every accessor the consumer has.
# ---------------------------------------------------------------------------

ALLOWLIST: dict[str, tuple[str, ...]] = {
    "customers": ("customer_id", "valid_from", "valid_to", "signup_date", "country",
                  "city", "segment", "lifetime_value_cents", "is_active",
                  "version", "knowledge_time"),
    "orders": ("order_id", "customer_id", "occurred_at", "channel", "payment_method",
               "shipping_speed", "order_total_cents", "items",
               "version", "knowledge_time"),
    "payments": ("payment_id", "order_id", "customer_id", "occurred_at",
                 "amount_cents", "status", "processor", "billing_city"),
    "events": ("event_id", "customer_id", "session_id", "occurred_at",
               "event_type", "sku", "version", "knowledge_time"),
    "labels": ("order_id", "resolved_at", "refund_at", "refunded_within_30d",
               "version", "knowledge_time"),
    "products": ("sku", "category", "product_name", "list_price_cents", "active_from"),
}

# Fields the serving layer is permitted to introduce on top of the stored
# columns.  Same rule: allowlist, not blocklist.
RENDER_ALLOWLIST: dict[str, tuple[str, ...]] = {
    "orders": ("refund_processed_at", "support_ticket_count", "order_channel"),
    "payments": ("settlement_batch_id",),
    "products": ("catalog_revision",),
    "customers": (),
    "events": (),
    "labels": (),
}


class AllowlistError(RuntimeError):
    pass


def allowed_keys(table: str) -> frozenset[str]:
    return frozenset(ALLOWLIST[table]) | frozenset(RENDER_ALLOWLIST.get(table, ()))


def check_payload(table: str, payload: dict) -> dict:
    """Fail closed.  Anything not explicitly allowed is a bug, not a feature."""
    extra = set(payload) - allowed_keys(table)
    if extra:
        raise AllowlistError(f"{table}: {sorted(extra)}")
    return payload


def iso(v) -> str | None:
    if v is None:
        return None
    if isinstance(v, dt.datetime):
        return v.replace(microsecond=0).isoformat()
    return str(v)


# ---------------------------------------------------------------------------
# build
# ---------------------------------------------------------------------------


def _ddl(spec: TableSpec) -> str:
    cols = ["row_id INTEGER PRIMARY KEY"]
    for c in spec.columns:
        cols.append(f"{c} {_COL_TYPES.get(c, 'TEXT')}")
    return f"CREATE TABLE {spec.name} (\n  " + ",\n  ".join(cols) + "\n)"


def _indexes(spec: TableSpec) -> list[str]:
    t = spec.name
    keycols = ", ".join(spec.key)
    return [
        f"CREATE INDEX ix_{t}_valid ON {t} ({spec.valid}, row_id)",
        f"CREATE INDEX ix_{t}_key ON {t} ({keycols}, version)",
        f"CREATE INDEX ix_{t}_kt ON {t} (knowledge_time)",
    ]


def _row_tuple(spec: TableSpec, row: dict):
    out = []
    for c in spec.columns:
        v = row.get(c)
        if c == "items":
            v = json.dumps(v, ensure_ascii=False, separators=(",", ":")) if v is not None else None
        elif isinstance(v, dt.datetime):
            v = iso(v)
        elif isinstance(v, dt.date):
            v = v.isoformat()
        out.append(v)
    return tuple(out)


def normalize_versions(world) -> None:
    """Re-rank versions by knowledge time.

    Rows are produced in several passes and in no particular chronological
    order, but the visibility rule is "highest version wins", so version order
    has to agree with knowledge order or a later revision could lose to an
    earlier one.  Rows sharing a knowledge time inside one business key keep the
    same version -- that is how a genuine duplicate write stays a duplicate
    instead of quietly superseding itself.
    """
    lists = {
        "customers": world.customers,
        "orders": world.orders,
        "payments": world.payments,
        "events": world.events,
        "labels": world.labels,
        "products": world.products,
    }
    for name, spec in SPECS.items():
        groups: dict[tuple, list[dict]] = {}
        for r in lists[name]:
            groups.setdefault(tuple(r[c] for c in spec.key), []).append(r)
        for g in groups.values():
            if len(g) == 1:
                g[0]["version"] = 1
                continue
            rank = {kt: i + 1 for i, kt in enumerate(sorted({r["knowledge_time"] for r in g}))}
            for r in g:
                r["version"] = rank[r["knowledge_time"]]


def build(cfg, plan, rng, pyrng) -> dict[str, int]:
    """Generate the world, bake the sampled data faults into it, persist it.

    ``plan`` is mutated: each entry gains the parameters and affected keys that
    only become known once the world exists.
    """
    from lab.faults import data as data_faults
    from lab.faults import ops as ops_faults

    world = build_world(
        rng, pyrng, cfg.clock.sim_start, cfg.world.horizon_sim_days, cfg.world.scale
    )

    # products are a slow-moving dimension; give them a knowledge time
    for p in world.products:
        p.setdefault("version", 1)
        p.setdefault("knowledge_time", cfg.clock.sim_start)
        p.setdefault("_deleted", 0)

    by_id = {p.id: p for p in plan}
    data_faults.apply(world, by_id, rng, pyrng, cfg)
    ops_faults.apply_to_store(world, by_id, rng, pyrng, cfg)
    normalize_versions(world)

    path = cfgmod.WORLD_PATH
    if path.exists():
        path.unlink()
    con = sqlite3.connect(path)
    con.execute("PRAGMA journal_mode=OFF")
    con.execute("PRAGMA synchronous=OFF")
    stats: dict[str, int] = {}
    rows_of = {
        "customers": world.customers,
        "orders": world.orders,
        "payments": world.payments,
        "events": world.events,
        "labels": world.labels,
        "products": world.products,
    }
    for name, spec in SPECS.items():
        con.execute(_ddl(spec))
        rows = rows_of[name]
        placeholders = ", ".join("?" * len(spec.columns))
        con.executemany(
            f"INSERT INTO {spec.name} ({', '.join(spec.columns)}) VALUES ({placeholders})",
            (_row_tuple(spec, r) for r in rows),
        )
        for stmt in _indexes(spec):
            con.execute(stmt)
        stats[name] = len(rows)
    con.commit()
    con.execute("PRAGMA optimize")
    con.close()
    return stats


# ---------------------------------------------------------------------------
# read side
# ---------------------------------------------------------------------------


class Store:
    """Read-only accessor.  One SQLite connection per thread."""

    def __init__(self, path: Path | None = None):
        self.path = Path(path or cfgmod.WORLD_PATH)
        if not self.path.exists():
            raise FileNotFoundError(
                f"no world at {self.path}; run `python lab.py init` first"
            )
        self._local = threading.local()

    @property
    def con(self) -> sqlite3.Connection:
        con = getattr(self._local, "con", None)
        if con is None:
            con = sqlite3.connect(
                f"file:{self.path}?mode=ro", uri=True, check_same_thread=False
            )
            con.row_factory = sqlite3.Row
            self._local.con = con
        return con

    # -- queries ---------------------------------------------------------

    def _visible_sql(self, spec: TableSpec, extra_where: str = "") -> str:
        key_match = " AND ".join(f"b.{k} = a.{k}" for k in spec.key)
        return f"""
            SELECT a.* FROM {spec.name} AS a
            WHERE a.knowledge_time <= :as_of
              AND a._deleted = 0
              {extra_where}
              AND NOT EXISTS (
                SELECT 1 FROM {spec.name} AS b
                WHERE {key_match}
                  AND b.knowledge_time <= :as_of
                  AND b.version > a.version)
        """

    def page(
        self,
        table: str,
        as_of: str,
        since: str | None = None,
        until: str | None = None,
        after: tuple[str, int] | None = None,
        limit: int = 200,
    ) -> list[sqlite3.Row]:
        spec = SPECS[table]
        params: dict = {"as_of": as_of, "limit": limit}
        extra = ""
        if since is not None:
            extra += f" AND a.{spec.valid} >= :since"
            params["since"] = since
        if until is not None:
            extra += f" AND a.{spec.valid} < :until"
            params["until"] = until
        if after is not None:
            extra += f" AND (a.{spec.valid}, a.row_id) > (:c_v, :c_r)"
            params["c_v"], params["c_r"] = after
        sql = self._visible_sql(spec, extra) + f" ORDER BY a.{spec.valid}, a.row_id LIMIT :limit"
        return list(self.con.execute(sql, params))

    def count(self, table: str, as_of: str, since: str | None, until: str | None) -> int:
        spec = SPECS[table]
        params: dict = {"as_of": as_of}
        extra = ""
        if since is not None:
            extra += f" AND a.{spec.valid} >= :since"
            params["since"] = since
        if until is not None:
            extra += f" AND a.{spec.valid} < :until"
            params["until"] = until
        sql = f"SELECT COUNT(*) FROM ({self._visible_sql(spec, extra)})"
        return int(self.con.execute(sql, params).fetchone()[0])

    def one(self, table: str, key: str, as_of: str) -> sqlite3.Row | None:
        spec = SPECS[table]
        extra = f" AND a.{spec.key[0]} = :key"
        sql = self._visible_sql(spec, extra) + f" ORDER BY a.{spec.valid} DESC, a.row_id DESC LIMIT 1"
        return self.con.execute(sql, {"as_of": as_of, "key": key}).fetchone()

    def by_knowledge_day(self, table: str, day: dt.date, as_of: str) -> list[sqlite3.Row]:
        """Everything this source learned on a given simulated day.

        This is what a nightly extract job would scrape out of the operational
        database: not "what happened today" but "what we found out today".
        """
        spec = SPECS[table]
        lo = dt.datetime.combine(day, dt.time()).isoformat()
        hi = dt.datetime.combine(day + dt.timedelta(days=1), dt.time()).isoformat()
        key_match = " AND ".join(f"b.{k} = a.{k}" for k in spec.key)
        sql = f"""
            SELECT a.* FROM {spec.name} AS a
            WHERE a.knowledge_time >= :lo AND a.knowledge_time < :hi
              AND a.knowledge_time <= :as_of
              AND a._deleted = 0
              AND NOT EXISTS (
                SELECT 1 FROM {spec.name} AS b
                WHERE {key_match}
                  AND b.knowledge_time <= :as_of
                  AND b.version > a.version)
            ORDER BY a.{spec.valid}, a.row_id
        """
        return list(self.con.execute(sql, {"lo": lo, "hi": hi, "as_of": as_of}))

    def snapshot(self, table: str, as_of: str) -> list[sqlite3.Row]:
        spec = SPECS[table]
        sql = self._visible_sql(spec) + f" ORDER BY a.{spec.valid}, a.row_id"
        return list(self.con.execute(sql, {"as_of": as_of}))

    def customer_valid_to(self, customer_id: str, valid_from: str, as_of: str) -> str | None:
        row = self.con.execute(
            """SELECT MIN(valid_from) FROM customers
               WHERE customer_id = ? AND valid_from > ? AND knowledge_time <= ?""",
            (customer_id, valid_from, as_of),
        ).fetchone()
        return row[0] if row else None

    # -- serialisation ---------------------------------------------------

    def serialize(self, table: str, row: sqlite3.Row, as_of: str) -> dict:
        allow = ALLOWLIST[table]
        out: dict = {}
        keys = row.keys()
        for col in allow:
            if col == "valid_to":
                continue
            if col in keys:
                v = row[col]
                if col == "items" and v is not None:
                    v = json.loads(v)
                out[col] = v
        if table == "customers":
            out["valid_to"] = self.customer_valid_to(
                row["customer_id"], row["valid_from"], as_of
            )
        return check_payload(table, out)
