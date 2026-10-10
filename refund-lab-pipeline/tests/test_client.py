from unittest.mock import Mock
import pytest
import time
import base64
import json

from refund_lab.client import (fetch_page_with_retry, decode_cursor,
                                classify_response)
from refund_lab.client import RetryableAuth, RetryableBackoff
from refund_lab.ingest import run_pull, UncompletePull, CHAIN_RETRY_LIMIT

def test_fetch_page_with_retry_recovers_from_429(monkeypatch):
    monkeypatch.setattr(time, "sleep", lambda seconds: None)
    resp_429 = Mock(status_code=429, headers={}, text={})
    resp_200 = Mock(status_code=200)
    mock_payload = {"cursor":None, "next_cursor":None,
                    "as_of":None, "total_count":5}
    resp_200.json.return_value = mock_payload
    fake_client = Mock()
    fake_client.get = Mock(side_effect=[resp_429, resp_200])
    payload = fetch_page_with_retry(fake_client, "orders", None)
    assert payload == resp_200.json.return_value
    assert fake_client.get.call_count == 2


def test_decode_cursor_maps_to_correct_dict():
    # Arrange
    raw = {"a": "2026-01-15T00:00:00", "n": 5, "r": 10,
         "v": "2026-01-15T05:00:00", "t": "orders",
         "s": "2026-01-01T00:00:00", "u": "2026-01-31T00:00:00"}
    cursor_str = base64.b64encode(json.dumps(raw).encode()).decode()
    # Act
    cursor_dict = decode_cursor(cursor_str)
    # Assert
    assert cursor_dict is not None
    assert cursor_dict["as_of"] == "2026-01-15T00:00:00"
    assert cursor_dict["total_count"] == 5
    assert cursor_dict["rows_served"] == 10
    assert cursor_dict["last_seen_value"] == "2026-01-15T05:00:00"
    assert cursor_dict["since"] == "2026-01-01T00:00:00"
    assert cursor_dict["until"] == "2026-01-31T00:00:00"


def test_classify_response_raises_on_401():
    resp = Mock(status_code=401)
    with pytest.raises(RetryableAuth):
        classify_response(resp)

def test_classify_response_raises_on_429():
    resp = Mock(status_code=429, headers={"retry-after":0.1})
    with pytest.raises(RetryableBackoff) as exc_info: 
        classify_response(resp)
    assert exc_info.value.retry_after == 0.1

def test_run_pull_recovers_from_one_bad_chain_link(monkeypatch):
    data = [{"order_id":"", "customer_id":"",
            "occurred_at":"", "channel":"",
            "payment_method":"", "shipping_speed":"",
            "order_total_cents":1, "items":[],
            "version":1, "knowledge_time":"2026-01-02T00:00:00"}]
    cu1 = {"v": "2026-01-02T00:00:00", "r":20 ,
           "pr": 10, "pv": "2026-01-01T00:00:00",
            "t": "orders", "n":10}
    cu1_str= base64.b64encode(json.dumps(cu1).encode()).decode()
    payload1 = {"cursor": None, "next_cursor":cu1_str, "data":data,
                "as_of":""}
    cu2 = {"v": "2026-01-02T00:00:00", "r":20 ,
           "pr": 30, "pv": "2026-01-03T00:00:00",
            "t": "orders", "n":10}
    cu2_str= base64.b64encode(json.dumps(cu2).encode()).decode()
    payload2 = {"cursor": cu1_str, "next_cursor":cu2_str, "data":data,
                "as_of":""}
    cu3 = {"v": "2026-01-02T00:00:00", "r":30 ,
           "pr": 20, "pv": "2026-01-02T00:00:00",
            "t": "orders", "n":10}
    cu3_str= base64.b64encode(json.dumps(cu3).encode()).decode()
    payload3 = {"cursor": cu3_str, "next_cursor":None, "data":data,
                "as_of":""}
    mock_fetch = Mock(side_effect=[payload1, payload2, payload3])
    monkeypatch.setattr(
        "refund_lab.ingest.fetch_page_with_retry", mock_fetch)
    conn = Mock()
    client = Mock()
    this_pull = run_pull("orders","", "", "",
                          conn, client)
    assert mock_fetch.call_count == 3
    assert len(this_pull["table"]) == 2



def test_run_pull_raises_on_retry_exhaustion(monkeypatch):
    data = [{"order_id":"", "customer_id":"",
            "occurred_at":"", "channel":"",
            "payment_method":"", "shipping_speed":"",
            "order_total_cents":1, "items":[],
            "version":1, "knowledge_time":"2026-01-02T00:00:00"}]
    cu1 = {"v": "2026-01-02T00:00:00", "r":20 ,
           "pr": 10, "pv": "2026-01-01T00:00:00",
            "t": "orders", "n":10}
    cu1_str= base64.b64encode(json.dumps(cu1).encode()).decode()
    payload1 = {"cursor": None, "next_cursor":cu1_str, "data":data,
                "as_of":""}
    cu2 = {"v": "2026-01-02T00:00:00", "r":20 ,
           "pr": 30, "pv": "2026-01-03T00:00:00",
            "t": "orders", "n":10}
    cu2_str= base64.b64encode(json.dumps(cu2).encode()).decode()
    payload2 = {"cursor": cu1_str, "next_cursor":cu2_str, "data":data,
                "as_of":""}
    mock_fetch = Mock(side_effect=[payload1] + \
                                   [payload2]*CHAIN_RETRY_LIMIT)
    monkeypatch.setattr(
        "refund_lab.ingest.fetch_page_with_retry", mock_fetch)
    conn = Mock()
    client = Mock()
    with pytest.raises(UncompletePull):
        run_pull("orders","", "", "", conn, client)
    assert mock_fetch.call_count == CHAIN_RETRY_LIMIT + 1
