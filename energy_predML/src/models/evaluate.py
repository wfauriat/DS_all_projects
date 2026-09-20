"""Model evaluation metrics."""

import logging

import numpy as np
import pandas as pd
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

logger = logging.getLogger(__name__)


def compute_metrics(y_true: np.ndarray, y_pred: np.ndarray) -> dict[str, float]:
    """Compute regression metrics for model evaluation.

    Args:
        y_true: Actual demand values.
        y_pred: Predicted demand values.

    Returns:
        Dictionary with RMSE, MAE, R2, and MAPE.
    """
    rmse = float(np.sqrt(mean_squared_error(y_true, y_pred)))
    mae = float(mean_absolute_error(y_true, y_pred))
    r2 = float(r2_score(y_true, y_pred))

    # MAPE (avoid division by zero)
    nonzero = y_true != 0
    mape = float(np.mean(np.abs((y_true[nonzero] - y_pred[nonzero]) / y_true[nonzero])) * 100)

    metrics = {"rmse": rmse, "mae": mae, "r2": r2, "mape": mape}
    logger.info("Metrics: RMSE=%.1f, MAE=%.1f, R2=%.4f, MAPE=%.2f%%", rmse, mae, r2, mape)
    return metrics
