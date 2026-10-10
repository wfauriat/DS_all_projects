#!/usr/bin/bash
set -euo pipefail


uv run python << 'EOF'
from refund_lab.db import init_db
from refund_lab.query import (query_daily_volume, query_orders_as_of, 
    query_weekday_weekend_split, query_segment_country)

from pprint import pprint

conn = init_db()

# result1 = query_daily_volume(conn)
# pprint(result1)

result2 = query_segment_country(conn)
pprint(result2)

EOF


