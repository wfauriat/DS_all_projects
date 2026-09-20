#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# streaming/entrypoint.sh: gives the container's UID a name, then runs the command.
#
# docker-compose.yml runs Spark as YOUR UID, so the Parquet files and
# checkpoints it writes under ./data belong to you. That UID has no entry in
# this image's /etc/passwd, and the JVM's Unix login (Hadoop's
# UserGroupInformation) needs a user NAME for it. Without one, every query
# fails at start. nss_wrapper fakes the entry for this process tree only.
# The Spark image's own entrypoint does the same for Kubernetes.
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

if ! getent passwd "$(id -u)" > /dev/null; then
  for wrapper in /usr/lib/*/libnss_wrapper.so /usr/lib/libnss_wrapper.so; do
    if [ -s "$wrapper" ]; then
      NSS_WRAPPER_PASSWD="$(mktemp)"
      NSS_WRAPPER_GROUP="$(mktemp)"
      echo "spark:x:$(id -u):$(id -g):spark:${HOME:-/tmp}:/bin/false" > "$NSS_WRAPPER_PASSWD"
      echo "spark:x:$(id -g):" > "$NSS_WRAPPER_GROUP"
      export LD_PRELOAD="$wrapper" NSS_WRAPPER_PASSWD NSS_WRAPPER_GROUP
      break
    fi
  done
fi

exec "$@"
