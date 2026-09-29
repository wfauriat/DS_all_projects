# Bash cheatsheet for exploring data files

Examples use the marketplace dataset in `data/`.

## Look

```bash
head -n 5 f.csv            # first 5 lines   (tail -n 5: last 5)
tail -n +2 f.csv           # everything from line 2 (skip header)
wc -l f.csv                # line count
less -S f.csv              # scroll; -S = no line wrap; q to quit
column -s, -t f.csv | less -S   # CSV as aligned table
file f.csv                 # encoding / type guess
```

## Columns and fields: `cut`, `awk`

```bash
cut -d, -f1,4 f.csv        # fields 1 and 4, delimiter ","   (-f2- = 2 to end)
head -n1 f.csv | tr ',' '\n' | nl       # numbered column names
```

`cut` cannot handle quoted commas (`"a,b"`). For messy CSV use `csvkit` / `duckdb` / `python`.

`awk` syntax: `awk -F<sep> 'CONDITION { ACTION }' file`. `$1` is field 1, `$0` the whole line, `NR` the line number, `NF` the number of fields, `END{}` runs once at the end.

```bash
awk -F, 'NR>1 && $4=="cancelled"' fact_orders.csv          # filter rows
awk -F, 'NR>1 {s+=$5} END {print s/(NR-1)}' items.csv       # average of col 5
awk -F, 'NR>1 {n[$4]++} END {for (k in n) print k, n[k]}' fact_orders.csv  # group-by count
awk -F, 'NR>1 {s[$2]+=$4} END {for (k in s) print k, s[k]}' items.csv       # group-by sum
awk -F, 'NF != 6' items.csv                                  # rows with wrong field count
```

## Filter lines: `grep`

```bash
grep -i  'error' f.log     # case-insensitive
grep -v  '^$' f.log        # invert (drop blank lines)
grep -c  'WARN' f.log      # count matching lines
grep -n  'WARN' f.log      # show line numbers
grep -E  'user=(1|2)\b'    # extended regex (|, +, ?, ())
grep -o  'event=[a-z_]*'   # print only the match
grep -B2 -A2 'ERROR' f.log # 2 lines of context before/after
```

## Sort, count, dedupe: `sort`, `uniq`

`uniq` only collapses *adjacent* duplicates, so always `sort` first.

```bash
sort f | uniq -c | sort -rn | head    # top-N frequency table (the classic)
sort -u f                              # distinct values
sort -t, -k3,3 f.csv                   # sort by column 3 (delimiter ,)
sort -t, -k5,5nr f.csv                 # numeric, reversed
uniq -d                                # show only duplicates (sorted input)
```

## Rewrite: `sed`, `tr`

```bash
sed -n '10,20p' f          # print lines 10-20 only
sed -n '1p;$p' f           # first and last line
sed 's/old/new/g' f        # substitute (g = every occurrence per line)
sed -E 's/user=([0-9]+)/\1/'  # capture group with -E
sed '/^--/d' f             # delete lines matching a pattern
tr -d '\r' < f             # strip Windows line endings
tr 'A-Z' 'a-z' < f         # lowercase
```

## JSON: `jq`

Filters chain with `|`. `.` is the current value. `-r` gives raw strings (no quotes). `-c` gives one compact line per record. `-s` slurps all lines into one array.

```bash
jq . f.json                              # pretty-print
jq -c 'keys' payments.jsonl | sort -u    # which keys exist?
jq -r '.method' payments.jsonl | sort | uniq -c
jq 'select(.amount > 1000)' payments.jsonl               # filter records
jq '{id: .order_id, brand: .card.brand}' payments.jsonl  # reshape; nested access
jq -r '[.order_id, .method, .amount] | @csv' payments.jsonl   # JSONL -> CSV
jq -s 'map(.amount) | add / length' payments.jsonl             # aggregate
jq -s 'group_by(.method) | map({m: .[0].method, n: length})' payments.jsonl
```

## Glue: pipes, redirects, loops

```bash
cmd1 | cmd2                # stdout of 1 -> stdin of 2
cmd > out.txt              # overwrite      (>> append)
cmd 2> err.txt             # redirect stderr;  cmd > out 2>&1 = both
diff <(sort a) <(sort b)   # process substitution: compare two commands' output
for f in *.csv; do echo "$f: $(wc -l < "$f")"; done
comm -23 <(sort a.txt) <(sort b.txt)   # lines in a but not in b (e.g. orphan keys)
```

Referential-integrity check (customers in orders that are missing from the dimension):

```bash
comm -23 <(cut -d, -f2 fact_orders.csv | tail -n +2 | sort -u) \
         <(cut -d, -f1 dim_customer.csv | tail -n +2 | sort -u)
```

## Beyond this: when bash stops being the right tool

- **`sqlite3 db.sqlite`** with `.mode csv`, `.import f.csv table`, `.schema`, `.headers on`, `.mode column`: quick SQL on files.
- **`duckdb`**: `duckdb -c "SELECT status, count(*) FROM 'fact_orders.csv' GROUP BY 1"` queries CSV/JSON/Parquet in place, no import. Probably the best exploration tool after bash.
- **`csvkit`** (`csvstat`, `csvcut`, `csvlook`) or **`xsv`/`qsv`**: CSV-aware (quotes handled), fast stats.
- **Python/pandas** once you need joins, types, or anything you'll keep.

Rule of thumb: bash for a first look (< 1 min), SQL/duckdb for questions, Python for pipelines.
