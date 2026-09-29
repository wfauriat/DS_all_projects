"""Create marketplace.db from schema.sql and load the CSV files from ./data.

Run:  python load_csv.py     (rebuilds marketplace.db from scratch every time)
Bad rows are written to rejects_csv.tsv (table, csv line number, reason, raw row) instead of aborting the load.
"""
import csv
import logging
import sqlite3
from pathlib import Path

DATA = Path("data")
DB = Path("marketplace.db")
SCHEMA = Path("schema.sql")
REJECTS = Path("rejects_csv.tsv")

# parents before children: foreign keys are enforced
TABLES = ["dim_country", "dim_city", "dim_category", "dim_customer",
          "dim_seller", "dim_product", "fact_orders", "fact_order_items"]
CASTS = {"INTEGER": int, "REAL": float, "TEXT": str}

log = logging.getLogger("load_csv")


def column_casts(con, table):
    """{column: python cast} taken from 
    the declared column types in the schema."""
    return {name: CASTS[ctype] 
            for _, name, ctype, *_ in con.execute(
                f"PRAGMA table_info({table})")}


def parse(row, casts):
    """CSV strings -> typed values; empty string -> None (NULL)."""
    return {col: (casts[col](val) if val != "" else None) 
            for col, val in row.items()}


def load_table(con, table, rejects):
    casts = column_casts(con, table)
    cols = list(casts)
    sql = (f"INSERT INTO {table} ({', '.join(cols)}) "
          f"VALUES ({', '.join('?' * len(cols))})")
    ok = bad = 0
    with (DATA / f"{table}.csv").open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if set(reader.fieldnames) != set(cols):
            raise SystemExit(
                f"{table}: CSV header {reader.fieldnames} "
                f"does not match schema {cols}")
        for lineno, row in enumerate(reader, start=2):  # line 1 is the header
            try:
                rec = parse(row, casts)
                con.execute(sql, [rec[c] for c in cols])
                ok += 1
            except (ValueError, sqlite3.IntegrityError) as e:
                bad += 1
                rejects.write(f"{table}\t{lineno}\t{e}\t{dict(row)}\n")
    log.info("%-18s loaded %5d  rejected %d", table, ok, bad)


def main():
    logging.basicConfig(level=logging.INFO,
            format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    DB.unlink(missing_ok=True)
    con = sqlite3.connect(DB)
    con.execute("PRAGMA foreign_keys = ON")  # per connection, off by default
    con.executescript(SCHEMA.read_text(encoding="utf-8"))
    with REJECTS.open("w", encoding="utf-8") as rejects, con: 
        # `with con` = one transaction, commit at the end
        for table in TABLES:
            load_table(con, table, rejects)
    log.info("foreign key violations: %d",
              len(con.execute("PRAGMA foreign_key_check").fetchall()))
    con.close()


if __name__ == "__main__":
    main()
