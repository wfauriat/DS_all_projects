"""Parse data/web_events.log into the `web_events` table of an existing marketplace.db (run load_csv.py first).

Run:  python load_events.py     (safe to rerun: the web_events table is emptied first)
Expected line:  2025-01-01T01:02:14Z INFO user=73 event=view product=24 session=s1626
Anything else (blank lines, restarts, errors, WARN lines with user=?) goes to rejects_events.tsv with a reason.
"""
import logging
import re
import sqlite3
from collections import Counter
from datetime import datetime
from pathlib import Path

LOG = Path("data/web_events.log")
DB = Path("marketplace.db")
REJECTS = Path("rejects_events.tsv")

LINE = re.compile(r"^(?P<ts>\S+) INFO user=(?P<user>\d+) event=(?P<event>\w+) product=(?P<product>\d+) session=(?P<session>\w+)$")
INSERT = "INSERT INTO web_events (event_ts, user_id, event, product_id, session_id) VALUES (?, ?, ?, ?, ?)"

log = logging.getLogger("load_events")


def parse(line):
    """One log line -> tuple for INSERT. Raises ValueError with the reason if the line is not a valid event."""
    if not line.strip():
        raise ValueError("blank line")
    m = LINE.match(line)
    if not m:
        raise ValueError("does not match the INFO event pattern")
    datetime.strptime(m["ts"], "%Y-%m-%dT%H:%M:%SZ")  # ValueError if not a real timestamp
    return m["ts"], int(m["user"]), m["event"], int(m["product"]), m["session"]


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    con = sqlite3.connect(DB)
    con.execute("PRAGMA foreign_keys = ON")
    ok, reasons = 0, Counter()
    with REJECTS.open("w", encoding="utf-8") as rejects, con:  # one transaction
        con.execute("DELETE FROM web_events")
        with LOG.open(encoding="utf-8") as f:
            for lineno, line in enumerate(f, start=1):
                line = line.rstrip("\n")
                try:
                    con.execute(INSERT, parse(line))
                    ok += 1
                except (ValueError, sqlite3.IntegrityError) as e:
                    reasons[str(e)] += 1
                    rejects.write(f"{lineno}\t{e}\t{line}\n")
    log.info("web_events loaded %d  rejected %d", ok, sum(reasons.values()))
    for reason, n in reasons.most_common():
        log.info("  rejected %4d x %s", n, reason)
    con.close()


if __name__ == "__main__":
    main()
