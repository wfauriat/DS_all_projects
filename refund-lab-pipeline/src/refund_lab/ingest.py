import httpx
import sqlite3
import datetime
import json
import logging

from .config import API_URL, LANDING_DB
from .client import auth_to_API, fetch_page_with_retry, decode_cursor
from .db import init_db, complete_page, write_entry


logger = logging.getLogger(__name__)

CHAIN_RETRY_LIMIT = 3


class UncompletePull(Exception):
    """ Pull could not complete all the way through
    """
    pass

def run_pull(entity: str, since: str, until: str, as_of: str,
             conn: sqlite3.Connection, client: httpx.Client) -> dict:
    page = 1
    table = []
    total_rows = 0
    cursor = None
    this_pull = {
        "entity": entity,
        "since": since,
        "until": until,
        "as_of": as_of,
        "table": [],
        "total_rows": 0
    }
    while True:
        for _ in range(CHAIN_RETRY_LIMIT):
            payload = fetch_page_with_retry(client, entity, cursor,
                                    as_of, since, until, limit=10)
            if cursor is None or payload["next_cursor"] is None:
                break
            this_position = decode_cursor(payload["cursor"])
            assert this_position is not None
            claimed_prev = decode_cursor(payload["next_cursor"])
            assert claimed_prev is not None
            if (claimed_prev["prev_rows_served"] == \
                this_position["rows_served"]) & \
                (claimed_prev["prev_last_seen_value"] == \
                this_position["last_seen_value"]):
                break
        else:
            raise UncompletePull(f"Could not complete pull at page {page} "
                        f"from cursor {json.dumps(decode_cursor(cursor))}")
        data = payload["data"]
        timestamp = datetime.datetime.now().isoformat()

        for row in data: 
            added = write_entry[entity](row, conn, timestamp, cursor, page)
            total_rows += added
        complete_page(conn, payload, entity,
                        cursor, page, as_of, since, until)
        table.extend(payload["data"])            
        cursor = payload["next_cursor"]
        if page == 1:
            as_of = payload["as_of"]
            this_pull["as_of"] = as_of
        page += 1
        if cursor is None:
            break
    this_pull["table"] = table
    this_pull["total_rows"] = total_rows
    logger.info(f"{total_rows} new {entity} added to {entity}_raw"
                f" at {LANDING_DB}")
    return this_pull


if __name__ == "__main__":

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    conn = init_db()

    client = httpx.Client(base_url=API_URL, timeout=10,
                        mounts={"all://localhost": None,
                                "all://127.0.0.1": None})
    auth_to_API(client)

    entity = "orders"
    # entity = "customers"
    as_of = "2026-05-01T00:00:00"
    since = "2020-01-01T00:00:00"
    until = "2026-05-01T00:00:00"
    # since = "2026-01-15T00:00:00"
    # until = "2026-01-25T00:00:00"
 
    pulled = run_pull(entity, since, until, as_of,
             conn, client)
    logger.info(f"Completed pull : "
                f"{[pulled[key] for key in 
                    ["entity", "as_of", "since", "until"]]}")
    # print(pulled["orders"][0])


