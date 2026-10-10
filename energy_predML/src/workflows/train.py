"""Prefect flow for model training: load features, train, tune, register."""

import logging

import mlflow
import xgboost as xgb

from src.config import TRAIN_CUTOFF_PATH, settings
from src.data.store import load_demand
from src.features.pipeline import build_features, get_latest_features, save_features
from src.models.baseline import train_baseline
from src.models.evaluate import compute_metrics
from src.models.xgboost_model import (
    get_feature_columns,
    train_xgboost,
    tune_xgboost,
)

logger = logging.getLogger(__name__)

EXPERIMENT_NAME = "energy_grid_forecast"


def run_training(
    use_tuning: bool = True,
    n_trials: int = 50,
    rebuild_features: bool = False,
) -> None:
    """Run the full model training pipeline.

    Args:
        use_tuning: Whether to run Optuna hyperparameter tuning.
        n_trials: Number of Optuna trials if tuning.
        rebuild_features: If True, rebuild features from DuckDB instead of loading Parquet.
    """
    # Set up MLflow — requires the server to be running
    tracking_uri = settings.MLFLOW_TRACKING_URI
    if tracking_uri.startswith("http"):
        try:
            import requests as _req
            _req.get(tracking_uri, timeout=3)
        except Exception as exc:
            raise RuntimeError(
                f"MLflow server not reachable at {tracking_uri}. "
                "Start it first with:\n"
                "  local mode:  make local-mlflow  (in a separate terminal)\n"
                "  docker mode: make docker-train   (handles MLflow automatically)"
            ) from exc
    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment(EXPERIMENT_NAME)

    # Load features
    if rebuild_features:
        df = build_features(region=settings.DEFAULT_REGION)
        save_features(df)
    else:
        try:
            df = get_latest_features()
        except FileNotFoundError:
            logger.info("No saved features found, building from DuckDB")
            df = build_features(region=settings.DEFAULT_REGION)
            save_features(df)

    # Temporal train/val split (80/20)
    split_idx = int(len(df) * 0.8)
    train_df = df.iloc[:split_idx].copy()
    val_df = df.iloc[split_idx:].copy()

    logger.info(
        "Train: %d rows (%s to %s), Val: %d rows (%s to %s)",
        len(train_df), train_df["timestamp"].min(), train_df["timestamp"].max(),
        len(val_df), val_df["timestamp"].min(), val_df["timestamp"].max(),
    )

    # 1. Baseline model
    logger.info("--- Training baseline model ---")
    baseline_metrics = train_baseline(val_df)

    # 2. XGBoost with default params
    logger.info("--- Training XGBoost (default params) ---")
    with mlflow.start_run(run_name="xgboost_default"):
        mlflow.set_tag("model_type", "xgboost")
        model, default_metrics = train_xgboost(train_df, val_df)
        mlflow.log_params(model.get_params())
        mlflow.log_metrics(default_metrics)

    # 3. Optuna tuning (optional)
    best_params = None
    if use_tuning:
        logger.info("--- Tuning XGBoost with Optuna (%d trials) ---", n_trials)
        best_params = tune_xgboost(df.iloc[:split_idx], n_trials=n_trials)

        # Retrain with best params on full train set, evaluate on val
        logger.info("--- Retraining with best params ---")
        model, tuned_metrics = train_xgboost(train_df, val_df, params=best_params)

        with mlflow.start_run(run_name="xgboost_tuned"):
            mlflow.set_tag("model_type", "xgboost_tuned")
            mlflow.log_params(best_params)
            mlflow.log_metrics(tuned_metrics)

            # Log model to MLflow
            feature_cols = get_feature_columns(train_df)
            mlflow.xgboost.log_model(
                model,
                artifact_path="model",
                input_example=train_df[feature_cols].iloc[:1].values,
                registered_model_name="energy_demand_xgboost",
            )
            logger.info("Tuned model registered to MLflow")
    else:
        # Register the default model
        with mlflow.start_run(run_name="xgboost_registered"):
            mlflow.set_tag("model_type", "xgboost")
            feature_cols = get_feature_columns(train_df)
            mlflow.xgboost.log_model(
                model,
                artifact_path="model",
                input_example=train_df[feature_cols].iloc[:1].values,
                registered_model_name="energy_demand_xgboost",
            )
            mlflow.log_metrics(default_metrics)
            logger.info("Default model registered to MLflow")

    # Persist training cutoff so the monitor knows what "new data" means.
    # Use the DuckDB max (not the parquet snapshot) so the cutoff always
    # reflects everything available at training time, even when the parquet
    # file is slightly stale.
    cutoff_ts = load_demand(region=settings.DEFAULT_REGION)["timestamp"].max()
    TRAIN_CUTOFF_PATH.write_text(str(cutoff_ts))
    logger.info("Training cutoff written: %s", cutoff_ts)

    # Summary
    logger.info("=== Training Summary ===")
    logger.info("Baseline RMSE: %.1f", baseline_metrics["rmse"])
    logger.info("XGBoost default RMSE: %.1f", default_metrics["rmse"])
    if use_tuning:
        logger.info("XGBoost tuned RMSE: %.1f", tuned_metrics["rmse"])


if __name__ == "__main__":
    run_training(use_tuning=True, n_trials=30)
