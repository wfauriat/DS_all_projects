"""The runtime surface the serving layer consumes.

OPERATOR: do not read this file.

Everything below is deliberately neutral: it hands the API and the drop writer
plain directives ("emit this key under that name after this date", "this file
is due today, gzipped, cp1252, cut short") and nothing about why.  The mapping
from a drawn fault to a directive lives here and nowhere downstream.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from pathlib import Path

from lab import config as cfgmod

from . import schedule as sched


def _d(s: str | None) -> dt.date | None:
    return None if s is None else dt.date.fromisoformat(s[:10])


def _t(s: str | None) -> dt.datetime | None:
    return None if s is None else dt.datetime.fromisoformat(s)


@dataclass(frozen=True)
class RenderPolicy:
    """How the serving layer must dress the rows it has been given."""

    rename_after: dt.datetime | None = None
    rename_from: str = "channel"
    rename_to: str = "order_channel"
    price_as_major_units_after: dt.datetime | None = None
    tz_suffix_after: dt.datetime | None = None
    tz_suffix: str = "+00:00"
    extra_order_fields_after: dt.datetime | None = None
    numeric_customer_id_from: dt.date | None = None
    skew_windows: tuple[tuple[dt.datetime, dt.datetime, int], ...] = ()

    def skew(self, sim_now: dt.datetime) -> dt.timedelta:
        for lo, hi, secs in self.skew_windows:
            if lo <= sim_now < hi:
                return dt.timedelta(seconds=secs)
        return dt.timedelta(0)


@dataclass(frozen=True)
class FileSpec:
    feed: str
    content_day: dt.date
    name_day: dt.date
    gzip: bool
    encoding: str = "utf-8"
    suffix: str = ""
    extra_column: tuple[str, int] | None = None
    permute_seed: int | None = None
    truncate_fraction: float | None = None
    atomic: bool = True


@dataclass
class _Delivery:
    late: dict[tuple[str, dt.date], int] = field(default_factory=dict)
    missing: set[tuple[str, dt.date]] = field(default_factory=set)
    duplicate: dict[tuple[str, dt.date], str] = field(default_factory=dict)
    truncate: dict[tuple[str, dt.date], float] = field(default_factory=dict)
    add_column: dict[str, tuple[dt.date, str, int]] = field(default_factory=dict)
    reorder: dict[str, tuple[dt.date, int]] = field(default_factory=dict)
    encoding: dict[str, tuple[dt.date, dt.date, str]] = field(default_factory=dict)
    compression: dict[str, tuple[dt.date, dt.date, bool]] = field(default_factory=dict)


class Effects:
    """Immutable view of what this run's serving layer must do."""

    def __init__(self, plan: list):
        self._plan = list(plan)
        self._by_id = {p.id: p for p in self._plan}
        self._outages: list[tuple[dt.datetime, dt.datetime]] = []
        self._delivery = _Delivery()
        self._policy = self._compile_policy()
        self._compile_delivery()

    # -- construction ----------------------------------------------------

    @classmethod
    def from_ledger(cls, path: Path | None = None) -> "Effects":
        led = sched.read_ledger(path or cfgmod.LEDGER_PATH)
        return cls([sched.PlannedFault.from_dict(d) for d in led["faults"]])

    @classmethod
    def empty(cls) -> "Effects":
        return cls([])

    # -- compilation -----------------------------------------------------

    def _p(self, fid: str) -> dict | None:
        pf = self._by_id.get(fid)
        return None if pf is None else pf.params

    def _compile_policy(self) -> RenderPolicy:
        kw: dict = {}
        if (p := self._p("field_rename_midstream")):
            kw["rename_after"] = _t(p["after"])
            kw["rename_from"] = p["from_name"]
            kw["rename_to"] = p["to_name"]
        if (p := self._p("unit_switch_price")):
            kw["price_as_major_units_after"] = _t(p["after"])
        if (p := self._p("tz_mixed")):
            kw["tz_suffix_after"] = _t(p["after"])
            kw["tz_suffix"] = p["offset"]
        if (p := self._p("leakage_column")):
            kw["extra_order_fields_after"] = _t(p["after"])
        if (p := self._p("id_leading_zeros")):
            kw["numeric_customer_id_from"] = _d(p["from"])
        if (p := self._p("clock_skew")):
            kw["skew_windows"] = ((_t(p["from"]), _t(p["to"]), int(p["seconds"])),)
        return RenderPolicy(**kw)

    def _compile_delivery(self) -> None:
        d = self._delivery
        if (p := self._p("sustained_outage")):
            self._outages.append((_t(p["from"]), _t(p["to"])))
        if (p := self._p("late_partition")):
            d.late[(p["feed"], _d(p["day"]))] = int(p["delay_days"])
        if (p := self._p("missing_delivery")):
            d.missing.add((p["feed"], _d(p["day"])))
        if (p := self._p("duplicate_delivery")):
            d.duplicate[(p["feed"], _d(p["day"]))] = p["suffix"]
        if (p := self._p("partial_write")):
            d.truncate[(p["feed"], _d(p["day"]))] = float(p["keep_fraction"])
        if (p := self._p("column_added")):
            d.add_column[p["feed"]] = (_d(p["after"]), p["column"], int(p["position"]))
        if (p := self._p("column_reordered")):
            d.reorder[p["feed"]] = (_d(p["after"]), int(p["permutation_seed"]))
        if (p := self._p("encoding_change")):
            d.encoding[p["feed"]] = (_d(p["from"]), _d(p["to"]), p["encoding"])
        if (p := self._p("compression_change")):
            d.compression[p["feed"]] = (_d(p["from"]), _d(p["to"]), bool(p["gzip"]))

    # -- API -------------------------------------------------------------

    @property
    def policy(self) -> RenderPolicy:
        return self._policy

    def unavailable(self, sim_now: dt.datetime) -> bool:
        return any(lo <= sim_now < hi for lo, hi in self._outages)

    # -- drop ------------------------------------------------------------

    def deliveries_for(
        self, boundary_day: dt.date, feeds: tuple[str, ...], default_gzip: dict[str, bool]
    ) -> list[FileSpec]:
        """What lands in the directory at the start of simulated day ``boundary_day``."""
        d = self._delivery
        out: list[FileSpec] = []
        for feed in feeds:
            due: list[dt.date] = []
            natural = boundary_day - dt.timedelta(days=1)
            if d.late.get((feed, natural)) is None:
                due.append(natural)
            for (f, day), delay in d.late.items():
                if f == feed and day + dt.timedelta(days=1 + delay) == boundary_day:
                    due.append(day)
            for day in due:
                if (feed, day) in d.missing:
                    continue
                out.append(self._spec(feed, day, default_gzip[feed]))
                sfx = d.duplicate.get((feed, day))
                if sfx is not None:
                    out.append(self._spec(feed, day, default_gzip[feed], suffix=sfx))
        return out

    def _spec(self, feed: str, day: dt.date, default_gzip: bool, suffix: str = "") -> FileSpec:
        d = self._delivery
        gz = default_gzip
        comp = d.compression.get(feed)
        if comp and comp[0] <= day < comp[1]:
            gz = comp[2]
        enc = "utf-8"
        e = d.encoding.get(feed)
        if e and e[0] <= day < e[1]:
            enc = e[2]
        extra = None
        a = d.add_column.get(feed)
        if a and day >= a[0]:
            extra = (a[1], a[2])
        perm = None
        r = d.reorder.get(feed)
        if r and day >= r[0]:
            perm = r[1]
        trunc = d.truncate.get((feed, day)) if not suffix else None
        return FileSpec(
            feed=feed, content_day=day, name_day=day, gzip=gz, encoding=enc,
            suffix=suffix, extra_column=extra, permute_seed=perm,
            truncate_fraction=trunc, atomic=trunc is None,
        )
