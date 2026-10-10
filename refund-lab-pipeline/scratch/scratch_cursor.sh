#!/usr/bin/bash
set -euo pipefail

python << 'EOF'
from refund_lab.client import decode_cursor
import sqlite3
conn = sqlite3.connect("../landing.db")
cursor = conn.cursor()
cursor.execute("SELECT cursor FROM page_ledger WHERE id=2;")
row = cursor.fetchone()
conn.close()
print(decode_cursor(row[0]))
EOF

sqlite3 ../landing.db "SELECT cursor FROM page_ledger WHERE id=2;" | \
    base64 -d