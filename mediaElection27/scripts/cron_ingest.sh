#!/usr/bin/env bash
# Wrapper invoked by cron to run one ingest pass.
#
# Why a wrapper instead of putting `python -m app.ingest.run` directly in the
# crontab: cron's environment is minimal (empty PATH-ish, $HOME = home, no
# project context). This script makes the env explicit, fixes cwd, captures the
# exit code, and prints scannable start/end markers around each run.
#
# Smoke-test manually: ./scripts/cron_ingest.sh
# Cron schedule lives in the user crontab (see README / `crontab -l`).

set -uo pipefail   # no -e: we want to log failures, not exit before printing them

PROJECT_DIR="/home/ai-user/Documents/Sandbox/mediaElection27"
PYTHON="$PROJECT_DIR/.venv/bin/python"
LOG_DIR="${XDG_STATE_HOME:-$HOME/.local/state}/media27"
PG_HOST="localhost"
PG_PORT="5432"
mkdir -p "$LOG_DIR"

# Wait up to 60s for Postgres to accept TCP connections. Matters mostly for
# @reboot runs, where cron can fire before the Docker daemon has finished
# bringing the postgres container up. For normal 4-hourly runs the first
# attempt succeeds and this is a no-op.
wait_for_postgres() {
    for _ in $(seq 1 30); do
        if (echo > "/dev/tcp/$PG_HOST/$PG_PORT") 2>/dev/null; then
            return 0
        fi
        sleep 2
    done
    return 1
}

start_ts=$(date --iso-8601=seconds)
echo "===== $start_ts ingest start ====="

if ! wait_for_postgres; then
    echo "Postgres not reachable on $PG_HOST:$PG_PORT after 60s — aborting"
    end_ts=$(date --iso-8601=seconds)
    echo "===== $end_ts ingest end (exit 2) ====="
    exit 2
fi

cd "$PROJECT_DIR"
"$PYTHON" -m app.ingest.run --once
rc=$?

end_ts=$(date --iso-8601=seconds)
echo "===== $end_ts ingest end (exit $rc) ====="
exit "$rc"
