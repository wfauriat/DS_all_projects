"""Load data/payments.jsonl into the `payments` table of an existing marketplace.db (run load_csv.py first).

Run:  python load_payments.py     (safe to rerun: the payments table is emptied first)
Flattens the nested `card` object into card_brand / card_last4 (NULL when card is null).
Bad lines go to rejects_payments.tsv; then payments are reconciled against the order items.
"""
import json
import logging
import sqlite3
from pathlib import Path

DATA = Path("data/payments.jsonl")
DB = Path("marketplace.db")
REJECTS = Path("rejects_payments.tsv")
TOLERANCE = 0.01  # one cent: payments are rounded to 2 decimals, item totals are not

log = logging.getLogger("load_payments")

INSERT = """INSERT INTO payments (order_id, paid_at, 
    method, amount, card_brand, card_last4)
            VALUES (?, ?, ?, ?, ?, ?)"""

# payments minus item totals (items: quantity * unit_price * (1 - discount), NULL discount counts as 0)
RECONCILE = """
WITH items_total AS (
    SELECT order_id,
    sum(quantity * unit_price * \
        (1 - COALESCE(discount_pct, 0) / 100.0)) AS total
    FROM fact_order_items GROUP BY order_id
)
SELECT p.order_id, p.amount, t.total
FROM payments AS p
LEFT JOIN items_total AS t ON t.order_id = p.order_id
WHERE t.total IS NULL OR abs(p.amount - t.total) > ?
"""


def to_row(line):
    """One JSONL line -> tuple for INSERT.
    Raises ValueError/KeyError/TypeError on a malformed record."""
    rec = json.loads(line)  # JSONDecodeError is a ValueError
    card = rec["card"] or {}  # null card -> no brand / last4
    return (int(rec["order_id"]), rec["paid_at"],
            rec["method"], float(rec["amount"]),
            card.get("brand"), card.get("last4"))


def main():
    logging.basicConfig(level=logging.INFO,
             format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    con = sqlite3.connect(DB)
    con.execute("PRAGMA foreign_keys = ON")
    ok = bad = 0
    with REJECTS.open("w", encoding="utf-8") as rejects, con:  # one transaction
        con.execute("DELETE FROM payments")
        for lineno, line in enumerate(
            DATA.read_text(encoding="utf-8").splitlines(), start=1):
            try:
                con.execute(INSERT, to_row(line))
                ok += 1
            except (ValueError, KeyError,
                    TypeError, sqlite3.IntegrityError) as e:
                bad += 1
                rejects.write(f"{lineno}\t{type(e).__name__}: {e}\t{line}\n")
    log.info("payments loaded %d  rejected %d", ok, bad)

    mismatches = con.execute(RECONCILE, (TOLERANCE,)).fetchall()
    for order_id, amount, total in mismatches:
        log.warning(
            "order %s: paid %s, items total %s", order_id, amount, total)
    log.info(
        "reconciliation: %d payments differ from item totals "
        "by more than %.2f", len(mismatches), TOLERANCE)
    no_payment = con.execute(
        """SELECT count(*) FROM fact_orders AS o
        LEFT JOIN payments AS p ON p.order_id = o.order_id
        WHERE p.order_id IS NULL""").fetchone()[0]
    log.info("orders without a payment: %d", no_payment)
    con.close()


if __name__ == "__main__":
    main()
