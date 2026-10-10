import sqlite3
import json 
from refund_lab.schema import SCHEMA_LEDGER, SCHEMA_ORDERS_RAW
from refund_lab.db import write_order_entry, complete_page
from refund_lab.utils import content_hash

def test_write_entry_dedupes_identical_rows():
    conn = sqlite3.connect(":memory:")
    conn.execute(SCHEMA_LEDGER)
    conn.execute(SCHEMA_ORDERS_RAW)
    mock_dict = {
        "order_id": 1, "customer_id": 1,
        "occurred_at": "2023-01-01 00:00:00",
        "channel": "web", "payment_method": "credit_card",
        "shipping_speed": "standard",
        "order_total_cents": 1000,
        "items": json.dumps({"item1": 1, "item2": 2}),
        "version": 1, "knowledge_time": "2023-01-01 00:00:00",
    }
    write_order_entry(mock_dict, conn, "2023-01-01 00:00:00",
                 "some_cursor", "some_page")
    write_order_entry(mock_dict, conn, "2023-01-01 00:00:00",
                 "some_cursor", "some_page")
    cursor = conn.execute("SELECT * FROM orders_raw")
    rows = cursor.fetchall()
    assert len(rows) == 1

def test_write_entry_keeps_restated_version():
    conn = sqlite3.connect(":memory:")
    conn.execute(SCHEMA_LEDGER)
    conn.execute(SCHEMA_ORDERS_RAW)
    mock_dict_v1 = {
        "order_id": 1, "customer_id": 1,
        "occurred_at": "2023-01-01 00:00:00",
        "channel": "web", "payment_method": "credit_card",
        "shipping_speed": "standard",
        "order_total_cents": 1000,
        "items": json.dumps({"item1": 1, "item2": 2}),
        "version": 1, "knowledge_time": "2023-01-01 00:00:00",
    }
    mock_dict_v2 = mock_dict_v1.copy()
    mock_dict_v2["version"] = 2
    mock_dict_v2["knowledge_time"] = "2023-01-02 00:00:00"
    write_order_entry(mock_dict_v1, conn, "2023-01-01 00:00:00",
                    "some_cursor", "some_page")
    write_order_entry(mock_dict_v2, conn, "2023-01-01 00:00:00",
                    "some_cursor", "some_page")
    cursor = conn.execute("SELECT * FROM orders_raw")
    rows = cursor.fetchall()
    assert len(rows) == 2
    cursor = conn.execute("SELECT content_hash FROM orders_raw ORDER BY id")
    hash1 = cursor.fetchone()[0]
    hash2 = cursor.fetchone()[0]
    assert hash1 != hash2

def test_complete_page_replaces_on_rerun():
    conn = sqlite3.connect(":memory:")
    conn.execute(SCHEMA_LEDGER)
    conn.execute(SCHEMA_ORDERS_RAW)
    mock_payload1 = {
        "as_of": "2023-01-01 00:00:00",
        "since": "2023-01-01 00:00:00",
        "until": "2023-01-01 00:00:00",
        "cursor": "some_cursor",
        "next_cursor": "some_next_cursor"
    }
    mock_payload2 = mock_payload1.copy()
    mock_payload2["next_cursor"] = "some_other_next_cursor"
    complete_page(conn, mock_payload1, "orders", "some_cursor", 1,
                  "2023-01-01 00:00:00", "2023-01-01 00:00:00",
                  "2023-01-01 00:00:00")
    complete_page(conn, mock_payload2, "orders", "some_cursor", 1,
                  "2023-01-01 00:00:00", "2023-01-01 00:00:00",
                  "2023-01-01 00:00:00")
    cursor = conn.execute("SELECT payload_hash FROM page_ledger")
    rows = cursor.fetchall()
    assert len(rows) == 1
    assert content_hash(mock_payload2) == rows[0][0]

