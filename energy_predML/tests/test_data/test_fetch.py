"""Tests for EIA API client (mocked)."""

from unittest.mock import MagicMock, patch

import pytest

from src.data.fetch import fetch_demand


@patch("src.data.fetch.requests.get")
def test_fetch_single_page(mock_get):
    """Test fetching when all data fits in one page."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "response": {
            "total": "2",
            "data": [
                {"period": "2026-01-01T00", "respondent": "CISO", "type": "D", "value": 25000},
                {"period": "2026-01-01T01", "respondent": "CISO", "type": "D", "value": 24000},
            ],
        }
    }
    mock_get.return_value = mock_response

    records = fetch_demand(region="CISO", start="2026-01-01T00", end="2026-01-01T01")

    assert len(records) == 2
    assert records[0]["value"] == 25000
    mock_get.assert_called_once()


@patch("src.data.fetch.requests.get")
def test_fetch_pagination(mock_get):
    """Test that pagination fetches multiple pages."""
    page1 = MagicMock()
    page1.json.return_value = {
        "response": {
            "total": "3",
            "data": [
                {"period": "2026-01-01T00", "value": 25000},
                {"period": "2026-01-01T01", "value": 24000},
            ],
        }
    }

    page2 = MagicMock()
    page2.json.return_value = {
        "response": {
            "total": "3",
            "data": [
                {"period": "2026-01-01T02", "value": 23000},
            ],
        }
    }

    mock_get.side_effect = [page1, page2]

    records = fetch_demand(region="CISO", start="2026-01-01T00", end="2026-01-01T02")

    assert len(records) == 3
    assert mock_get.call_count == 2


@patch("src.data.fetch.requests.get")
def test_fetch_empty_response(mock_get):
    """Test handling of empty API response."""
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "response": {"total": "0", "data": []}
    }
    mock_get.return_value = mock_response

    records = fetch_demand(region="CISO", start="2026-01-01T00", end="2026-01-01T01")

    assert len(records) == 0
