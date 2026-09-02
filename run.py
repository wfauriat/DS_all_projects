#!/usr/bin/env python
"""Entry point.

    python run.py bootstrap        full extraction of every endpoint, once
    python run.py cycle [N]        run N pipeline increments
    python run.py run              run until the horizon ends
    python run.py status           where the pipeline has got to
    python run.py tests            run the full data-test suite now
"""
from __future__ import annotations

import sys

from pipeline import config, orchestrator, util

log = util.get_logger("run")


def status():
    from pipeline import storage
    st = storage.Store()
    cl = orchestrator.VendorClient()
    try:
        clk = cl.clock()
    except Exception:
        clk = {}
    print(f"cycle             {st.get_state('cycle_id', 0)}")
    print(f"sim now           {clk.get('sim_now')}  (end {clk.get('sim_end')})")
    print(f"elapsed sim days  {clk.get('elapsed_sim_days')}")
    print(f"covered through   {st.get_state('covered_through')}")
    for t in ("src_orders", "cur_orders", "src_events", "cur_events", "src_customers",
              "cur_customers", "src_labels", "cur_labels", "src_payments", "cur_payments",
              "cur_products", "fct_orders", "fct_payments", "dim_customer", "agg_daily",
              "drop_files", "predictions"):
        print(f"{t:<16} {st.q1(f'SELECT COUNT(*) FROM {t}'):>9}")
    print(f"{'incidents':<16} {st.q1('SELECT COUNT(*) FROM incident_log'):>9}")
    print(f"{'alerts':<16} {st.q1('SELECT COUNT(*) FROM alerts'):>9}")
    fails = st.q1("SELECT COUNT(*) FROM test_results WHERE status='fail'")
    total = st.q1("SELECT COUNT(*) FROM test_results")
    print(f"{'tests':<16} {total - fails:>9} pass / {fails} fail")
    for r in st.q("SELECT model_version, n_train, auc_holdout, base_rate FROM model_runs "
                  "ORDER BY model_version DESC LIMIT 3"):
        print(f"model {r['model_version']} n={r['n_train']} auc={r['auc_holdout']} "
              f"base={r['base_rate']}")
    for r in st.q("SELECT detected_at_sim, suspected_fault, suspected_family FROM incident_log "
                  "ORDER BY detected_at_sim"):
        print(f"  {r['detected_at_sim']}  {r['suspected_fault'] or '-':<26} "
              f"{r['suspected_family']}")


def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else "run"
    if cmd == "status":
        return status()
    p = orchestrator.Pipeline()
    if cmd == "bootstrap":
        p.bootstrap()
    elif cmd == "cycle":
        n = int(sys.argv[2]) if len(sys.argv) > 2 else 1
        p.run(max_cycles=n, until_horizon=True)
    elif cmd == "tests":
        sim_now, _ = p.clock()
        days = [r[0] for r in p.st.q("SELECT DISTINCT order_day FROM fct_orders "
                                     "ORDER BY order_day DESC LIMIT 6")]
        for name, stat, obs, detail in p.tf.test(p.cycle_id, sim_now, days, full=True):
            print(f"{stat:<5} {name:<38} {obs if obs is not None else ''} {detail}")
        p.st.commit()
    else:
        p.run()


if __name__ == "__main__":
    main()
