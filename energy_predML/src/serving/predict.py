"""Prediction logic: generate features for a timestamp and call the model."""

import logging
from datetime import datetime

import numpy as np
import pandas as pd

from src.data.store import load_demand
from src.features.lag import add_lag_features
from src.features.temporal import add_temporal_features
from src.features.pipeline import NON_FEATURE_COLS

logger = logging.getLogger(__name__)

# Need enough history for lag_168h + rolling_7d warm-up
HISTORY_HOURS = 336  # 14 days


def generate_features_for_timestamp(target_ts: datetime, region: str) -> pd.DataFrame:
    """Build a feature row for a single timestamp by loading history from DuckDB.

    Args:
        target_ts: The timestamp to generate a prediction for.
        region: Grid region code.

    Returns:
        Single-row DataFrame with all features ready for model input.

    Raises:
        ValueError: If insufficient historical data is available.
    """
    # Load enough history to compute lags and rolling features
    end_ts = target_ts
    start_ts = target_ts - pd.Timedelta(hours=HISTORY_HOURS)

    history = load_demand(
        region=region,
        start=start_ts.isoformat(),
        end=end_ts.isoformat(),
    )

    if len(history) < 169:  # minimum for 168h lag + 1 target row
        raise ValueError(
            f"Insufficient historical data: got {len(history)} rows, need at least 169. "
            f"Range: {start_ts} to {end_ts}"
        )

    # Check if target timestamp exists in data; if not, append a placeholder row
    target_ts_utc = pd.Timestamp(target_ts, tz="UTC")
    if not (history["timestamp"] == target_ts_utc).any():
        placeholder = pd.DataFrame({
            "timestamp": [target_ts_utc],
            "region": [region],
            "demand_mwh": [np.nan],
        })
        history = pd.concat([history, placeholder], ignore_index=True)

    history = history.sort_values("timestamp").reset_index(drop=True)
    history["timestamp"] = pd.to_datetime(history["timestamp"], utc=True)

    # Apply feature engineering
    history = add_temporal_features(history)
    history = add_lag_features(history)

    # Get the row for the target timestamp
    target_row = history[history["timestamp"] == target_ts_utc]

    if target_row.empty:
        raise ValueError(f"Could not find target timestamp {target_ts_utc} in feature DataFrame")

    # Drop columns that aren't model features
    feature_cols = [c for c in target_row.columns if c not in NON_FEATURE_COLS]
    result = target_row[feature_cols]

    # Check for NaN in features (lag warm-up issues)
    null_cols = result.columns[result.isna().any()].tolist()
    if null_cols:
        logger.warning("NaN in features: %s — filling with 0", null_cols)
        result = result.fillna(0)

    return result


def predict_demand(
    model,
    target_ts: datetime,
    region: str,
    model_rmse: float | None = None,
) -> dict:
    """Generate a demand prediction for a specific timestamp.

    Args:
        model: Trained model with a .predict() method.
        target_ts: Timestamp to predict for.
        region: Grid region code.
        model_rmse: Validation RMSE from the training run. When provided,
            the 95% confidence interval is computed as prediction ± 1.96 × RMSE
            (Gaussian error assumption). Falls back to ±4% of the prediction
            when not available.

    Returns:
        Dictionary with prediction results.
    """
    features = generate_features_for_timestamp(target_ts, region)
    prediction = float(model.predict(features.values)[0])

    # Confidence interval: ±1.96 × val RMSE gives ~95% coverage under Gaussian errors.
    # Falls back to a naive ±4% of the prediction when the RMSE is unavailable.
    if model_rmse is not None:
        margin = 1.96 * model_rmse
    else:
        margin = prediction * 0.04
    lower = prediction - margin
    upper = prediction + margin

    result = {
        "timestamp": target_ts.isoformat(),
        "region": region,
        "demand_mwh": round(prediction, 1),
        "confidence_interval": [round(lower, 1), round(upper, 1)],
    }

    logger.info("Prediction for %s [%s]: %.1f MWh", target_ts, region, prediction)
    return result
