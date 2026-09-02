#!/usr/bin/env python3
"""refund-lab-source control CLI.

    python lab.py up                 init if needed, then run the vendor
    python lab.py init [--force]     draw a run, build the world, seal the ledger
    python lab.py serve              run the vendor (HTTP API + file drop)

Nothing here prints the seed or the fault plan.  The run-id is a hash prefix
and is safe to quote anywhere.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import shutil
import sys
from pathlib import Path

from lab import config as cfgmod


def _rel(path: Path) -> str:
    try:
        return str(path.relative_to(cfgmod.REPO_ROOT))
    except ValueError:
        return str(path)


def _init(args: argparse.Namespace) -> int:
    from lab import world
    from lab.faults import schedule as sched

    cfg = cfgmod.load()

    if cfgmod.LEDGER_PATH.exists() and not args.force:
        print(
            "refusing to re-initialise: a sealed ledger already exists at\n"
            f"  {cfgmod.LEDGER_PATH}\n"
            "Re-initialising draws a new run-id and discards the current world,\n"
            "its history and any detection record you have built against it.\n"
            "Pass --force if that is genuinely what you want.",
            file=sys.stderr,
        )
        return 2

    cfgmod.STATE_DIR.mkdir(parents=True, exist_ok=True)
    for stale in (cfgmod.LEDGER_PATH, cfgmod.CLOCK_PATH, cfgmod.WORLD_PATH, cfgmod.RUN_PATH):
        if stale.exists():
            stale.unlink()
    if cfg.drop.path.exists():
        shutil.rmtree(cfg.drop.path)

    seed = sched.draw_seed()
    rid = sched.run_id(seed)
    rng, pyrng = sched.rngs(seed)

    plan = sched.schedule(
        sched.select(pyrng, cfg.faults.n_sampled),
        rng, pyrng,
        cfg.clock.sim_start,
        cfg.world.horizon_sim_days,
    )

    stats = world.build(cfg, plan, rng, pyrng)

    ledger = {
        "run_id": rid,
        "seed": seed.hex(),
        "created_real": dt.datetime.now().replace(microsecond=0).isoformat(),
        "config_fingerprint": cfg.fingerprint,
        "sim_start": cfg.clock.sim_start.isoformat(),
        "sim_end": cfg.sim_end.isoformat(),
        "horizon_sim_days": cfg.world.horizon_sim_days,
        "scale": cfg.world.scale,
        "faults": [p.to_dict() for p in plan],
    }
    digest = sched.write_ledger(cfgmod.LEDGER_PATH, ledger)

    cfgmod.RUN_PATH.write_text(
        json.dumps(
            {
                "run_id": rid,
                "created_real": ledger["created_real"],
                "config_fingerprint": cfg.fingerprint,
                "sealed_sha256": digest,
                "sim_start": ledger["sim_start"],
                "sim_end": ledger["sim_end"],
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    del seed, rng, pyrng, plan, ledger

    print(f"run-id            {rid}")
    print(f"sealed ledger     {_rel(cfgmod.LEDGER_PATH)}")
    print(f"  sha256          {digest}")
    print(f"world             {_rel(cfgmod.WORLD_PATH)}")
    for name, n in stats.items():
        print(f"  {name:<15} {n:>9,} rows")
    print(f"simulated horizon {ledger_span(cfg)}")
    print()
    print("Record the sha256 above. If it changes, the ledger was regenerated or")
    print("tampered with and any score computed against it is meaningless.")
    return 0


def ledger_span(cfg) -> str:
    return (
        f"{cfg.clock.sim_start.date().isoformat()} .. "
        f"{cfg.sim_end.date().isoformat()}  ({cfg.world.horizon_sim_days} simulated days)"
    )


def _serve(args: argparse.Namespace) -> int:
    import uvicorn

    from lab.api import RunMismatch, verify_run

    cfg = cfgmod.load()
    if not cfgmod.LEDGER_PATH.exists():
        print("no run initialised; run `python lab.py init` first", file=sys.stderr)
        return 2
    try:
        verify_run(cfg)
    except RunMismatch as exc:
        print(str(exc), file=sys.stderr)
        return 2

    uvicorn.run(
        "lab.api:app",
        host=args.host or cfg.api.host,
        port=args.port or cfg.api.port,
        log_level="info",
        access_log=True,
    )
    return 0


def _up(args: argparse.Namespace) -> int:
    """Start from a fresh clone in one command.

    Draws a run only if there is not one already; it will never replace one.
    """
    if not cfgmod.LEDGER_PATH.exists():
        rc = _init(argparse.Namespace(force=False))
        if rc != 0:
            return rc
        print()
    return _serve(args)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="lab.py", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    p_init = sub.add_parser("init", help="draw a run, build the world, seal the ledger")
    p_init.add_argument("--force", action="store_true",
                        help="discard an existing run and draw a new one")
    p_init.set_defaults(fn=_init)

    p_serve = sub.add_parser("serve", help="run the vendor")
    p_serve.add_argument("--host", default=None)
    p_serve.add_argument("--port", type=int, default=None)
    p_serve.set_defaults(fn=_serve)

    p_up = sub.add_parser("up", help="init if needed, then run the vendor")
    p_up.add_argument("--host", default=None)
    p_up.add_argument("--port", type=int, default=None)
    p_up.set_defaults(fn=_up)

    args = ap.parse_args(argv)
    return args.fn(args)


if __name__ == "__main__":
    raise SystemExit(main())
