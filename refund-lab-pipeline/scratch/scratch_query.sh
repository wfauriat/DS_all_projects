#!/usr/bin/env bash
set -euo pipefail

DB=landing.db

section() { echo "=================="; echo "$1"; echo "=================="; }

section "Number of records in orders_raw"
sqlite3 "$DB" -header -column \
"SELECT COUNT(*) FROM orders_raw;"

section "Last records in orders_raw"
sqlite3 "$DB" -header -column \
"SELECT id, order_id, customer_id, knowledge_time
FROM orders_raw ORDER BY id DESC
LIMIT 10;"

section "Handful of records"
sqlite3 landing.db -header -column "SELECT id, order_id,
 customer_id, occurred_at, knowledge_time, version  FROM orders_raw;"