#!/usr/bin/env bash
# Quick exploration of ./data. Run: bash explore.sh   (or copy/paste blocks one by one)
cd "$(dirname "$0")/data" || exit 1

echo "== 1. Sizes: bytes / lines / words per file =="
ls -lh
wc -l *.csv *.jsonl *.log

echo; echo "== 2. Peek: header + first 3 rows of each CSV =="
for f in *.csv; do echo "--- $f"; head -n 4 "$f"; done

echo; echo "== 3. Aligned view of a CSV =="
head -n 6 dim_customer.csv | column -s, -t

echo; echo "== 4. Column names, numbered =="
head -n 1 fact_order_items.csv | tr ',' '\n' | nl

echo; echo "== 5. Frequency of a column (status = col 4 of fact_orders) =="
tail -n +2 fact_orders.csv | cut -d, -f4 | sort | uniq -c | sort -rn

echo; echo "== 6. Missing values: empty fields per column (customer email = col 3, seller rating = col 5) =="
awk -F, 'NR>1 && $3==""' dim_customer.csv | wc -l
awk -F, 'NR>1 && $5==""' dim_seller.csv | wc -l

echo; echo "== 7. Quick sum/avg with awk: total qty and avg unit_price of order items =="
awk -F, 'NR>1 {q+=$4; p+=$5; n++} END {print "qty:", q, " avg price:", p/n}' fact_order_items.csv

echo; echo "== 8. Date range of orders =="
tail -n +2 fact_orders.csv | cut -d, -f3 | sort | sed -n '1p;$p'

echo; echo "== 9. Duplicate key check (order_id should be unique) =="
tail -n +2 fact_orders.csv | cut -d, -f1 | sort | uniq -d | head

echo; echo "== 10. Log: line types, junk lines, event counts =="
head -n 3 web_events.log
echo "lines NOT matching the expected pattern:"
grep -vcE '^[0-9T:-]+Z INFO user=[0-9]+ event=[a-z_]+ product=[0-9]+ session=s[0-9]+$' web_events.log
grep -vE '^[0-9T:-]+Z INFO user=[0-9]+ event=[a-z_]+ product=[0-9]+ session=s[0-9]+$' web_events.log | sort | uniq -c
grep -oE 'event=[a-z_]+' web_events.log | sort | uniq -c | sort -rn

echo; echo "== 11. JSONL with jq (if installed) =="
if command -v jq >/dev/null; then
  head -n 2 payments.jsonl | jq .
  jq -r '.method' payments.jsonl | sort | uniq -c
  jq -s 'map(select(.card == null)) | length' payments.jsonl   # payments without card info
  jq -s 'map(.amount) | add' payments.jsonl                    # total paid
else
  echo "jq not installed; fallback:"; head -n 2 payments.jsonl
fi

echo; echo "== 12. Cross-file check: max ids used in facts vs dimension sizes =="
tail -n +2 fact_orders.csv | cut -d, -f2 | sort -n | tail -n 1     # max customer_id in orders
tail -n +2 dim_customer.csv | wc -l                               # customers in dim
tail -n +2 fact_order_items.csv | cut -d, -f3 | sort -n | tail -n 1 # max product_id in items
tail -n +2 dim_product.csv | wc -l                                # products in dim
