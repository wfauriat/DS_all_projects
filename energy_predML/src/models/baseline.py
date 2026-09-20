"""Baseline model: predict demand as same hour yesterday."""

import logging

import mlflow
import numpy as np
import pandas as pd

from src.models.evaluate import compute_metrics

logger = logging.getLogger(__name__)


def train_baseline(df: pd.DataFrame, target: str = "demand_mwh") -> dict[str, float]:
    """Train and evaluate a naive baseline: demand = demand_lag_24h.

    This provides a lower bound for model performance. Any useful model
    should beat this baseline.

    Args:
        df: Feature DataFrame with temporal split already applied (validation set).
        target: Target column name.

    Returns:
        Dictionary of evaluation metrics.
    """
    # Baseline prediction: same hour yesterday
    y_true = df[target].values
    y_pred = df["demand_lag_24h"].values

    metrics = compute_metrics(y_true, y_pred)

    # Log to MLflow
    with mlflow.start_run(run_name="baseline_lag24h"):
        mlflow.set_tag("model_type", "baseline")
        mlflow.log_params({"method": "lag_24h"})
        mlflow.log_metrics(metrics)

    logger.info("Baseline (lag_24h) — RMSE: %.1f, MAE: %.1f, R2: %.4f",
                metrics["rmse"], metrics["mae"], metrics["r2"])
    return metrics
