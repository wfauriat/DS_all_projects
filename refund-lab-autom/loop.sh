#!/bin/bash
# Unattended driver: keeps the pipeline running to the horizon, restarting it if
# it ever exits non-zero.  State is checkpointed every cycle, so a restart
# resumes rather than repeats.
cd "$(dirname "$0")"
for attempt in $(seq 1 200); do
  /opt/venvs/pyDS/bin/python run.py run >> logs/run.out 2>&1
  rc=$?
  echo "$(date -Iseconds) run.py exited rc=$rc (attempt $attempt)" >> logs/run.out
  if [ "$rc" -eq 0 ]; then
    echo "$(date -Iseconds) horizon reached" >> logs/run.out
    exit 0
  fi
  sleep 5
done
