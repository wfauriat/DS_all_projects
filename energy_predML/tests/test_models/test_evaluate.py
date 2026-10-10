"""Tests for model evaluation metrics."""

import numpy as np
import pytest

from src.models.evaluate import compute_metrics


def test_perfect_prediction():
    y = np.array([100.0, 200.0, 300.0])
    metrics = compute_metrics(y, y)
    assert metrics["rmse"] == pytest.approx(0.0)
    assert metrics["mae"] == pytest.approx(0.0)
    assert metrics["r2"] == pytest.approx(1.0)
    assert metrics["mape"] == pytest.approx(0.0)


def test_known_error():
    y_true = np.array([100.0, 200.0, 300.0])
    y_pred = np.array([110.0, 190.0, 310.0])
    metrics = compute_metrics(y_true, y_pred)
    assert metrics["rmse"] == pytest.approx(10.0)
    assert metrics["mae"] == pytest.approx(10.0)


def test_r2_range():
    y_true = np.array([100.0, 200.0, 300.0, 400.0])
    y_pred = np.array([110.0, 190.0, 310.0, 390.0])
    metrics = compute_metrics(y_true, y_pred)
    assert 0 < metrics["r2"] < 1


def test_mape_percentage():
    y_true = np.array([100.0, 200.0])
    y_pred = np.array([90.0, 220.0])
    metrics = compute_metrics(y_true, y_pred)
    # MAPE = mean(|10/100|, |20/200|) * 100 = mean(0.1, 0.1) * 100 = 10%
    assert metrics["mape"] == pytest.approx(10.0)
