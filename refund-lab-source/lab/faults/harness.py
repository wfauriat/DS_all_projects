"""Force-firing harness.

TEST-ONLY.  Nothing in the service imports this, no CLI exposes it, and it
never writes to the sealed ledger.  It exists so the lab's own test suite can
build a world containing exactly one known fault and assert that the documented
signature appears.

OPERATOR: using this against your live run would tell you what you are supposed
to be finding out for yourself.  It also cannot: it builds a *different* world
in a *different* state directory and leaves the sealed ledger untouched.
"""

from __future__ import annotations

import datetime as dt

from lab import world as worldmod
from lab.faults import schedule as sched

_HARNESS_SEED = b"refund-lab-forced"


def forced_plan(
    cfg,
    fault_ids: list[str],
    times: dict[str, tuple[dt.datetime, dt.datetime]] | None = None,
    seed: bytes = _HARNESS_SEED,
) -> list:
    rng, pyrng = sched.rngs(seed)
    plan = sched.schedule(
        list(fault_ids), rng, pyrng, cfg.clock.sim_start, cfg.world.horizon_sim_days
    )
    for pf in plan:
        override = (times or {}).get(pf.id)
        if override is not None:
            pf.sim_start, pf.sim_end = override
    plan.sort(key=lambda p: p.sim_start)
    return plan


def build(
    cfg,
    fault_ids: list[str],
    times: dict[str, tuple[dt.datetime, dt.datetime]] | None = None,
    seed: bytes = _HARNESS_SEED,
):
    """Build a world containing exactly ``fault_ids``.  Returns (plan, stats)."""
    plan = forced_plan(cfg, fault_ids, times, seed)
    rng, pyrng = sched.rngs(seed)
    stats = worldmod.build(cfg, plan, rng, pyrng)
    return plan, stats
