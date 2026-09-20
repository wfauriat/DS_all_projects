"""XGBoost model training with Optuna hyperparameter tuning."""

import logging
from typing import Any

import numpy as np
import pandas as pd
import xgboost as xgb

from src.features.pipeline import NON_FEATURE_COLS
from src.models.evaluate import compute_metrics

logger = logging.getLogger(__name__)


def get_feature_columns(df: pd.DataFrame) -> list[str]:
    """Return the list of feature columns (everything except target/metadata)."""
    return [c for c in df.columns if c not in NON_FEATURE_COLS]


def train_xgboost(
    train_df: pd.DataFrame,
    val_df: pd.DataFrame,
    params: dict[str, Any] | None = None,
    target: str = "demand_mwh",
) -> tuple[xgb.XGBRegressor, dict[str, float]]:
    """Train an XGBoost model and evaluate on the validation set.

    Args:
        train_df: Training DataFrame.
        val_df: Validation DataFrame.
        params: XGBoost hyperparameters. Uses defaults if None.
        target: Target column name.

    Returns:
        Tuple of (trained model, validation metrics).
    """
    feature_cols = get_feature_columns(train_df)

    X_train = train_df[feature_cols].values
    y_train = train_df[target].values
    X_val = val_df[feature_cols].values
    y_val = val_df[target].values

    if params is None:
        params = {
            "n_estimators": 500,
            "max_depth": 6,
            "learning_rate": 0.05,
            "subsample": 0.8,
            "colsample_bytree": 0.8,
            "reg_alpha": 0.1,
            "reg_lambda": 1.0,
        }

    model = xgb.XGBRegressor(
        **params,
        objective="reg:squarederror",
        early_stopping_rounds=50,
        random_state=42,
    )

    model.fit(
        X_train, y_train,
        eval_set=[(X_val, y_val)],
        verbose=False,
    )

    y_pred = model.predict(X_val)
    metrics = compute_metrics(y_val, y_pred)

    logger.info("XGBoost — RMSE: %.1f, MAE: %.1f, R2: %.4f",
                metrics["rmse"], metrics["mae"], metrics["r2"])
    return model, metrics


def tune_xgboost(
    df: pd.DataFrame,
    n_trials: int = 50,
    n_splits: int = 3,
    target: str = "demand_mwh",
) -> dict[str, Any]:
    """Run Optuna hyperparameter tuning with time-series cross-validation.

    Args:
        df: Full feature DataFrame (will be split by TimeSeriesSplit).
        n_trials: Number of Optuna trials.
        n_splits: Number of TimeSeriesSplit folds.
        target: Target column name.

    Returns:
        Best hyperparameters found.
    """
    import optuna
    from sklearn.model_selection import TimeSeriesSplit

    feature_cols = get_feature_columns(df)
    X = df[feature_cols].values
    y = df[target].values

    tscv = TimeSeriesSplit(n_splits=n_splits)

    def objective(trial: optuna.Trial) -> float:
        params = {
            "n_estimators": trial.suggest_int("n_estimators", 100, 1000),
            "max_depth": trial.suggest_int("max_depth", 3, 10),
            "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.3, log=True),
            "subsample": trial.suggest_float("subsample", 0.6, 1.0),
            "colsample_bytree": trial.suggest_float("colsample_bytree", 0.6, 1.0),
            "reg_alpha": trial.suggest_float("reg_alpha", 1e-3, 10.0, log=True),
            "reg_lambda": trial.suggest_float("reg_lambda", 1e-3, 10.0, log=True),
            "min_child_weight": trial.suggest_int("min_child_weight", 1, 10),
        }

        rmses = []
        for train_idx, val_idx in tscv.split(X):
            model = xgb.XGBRegressor(
                **params,
                objective="reg:squarederror",
                early_stopping_rounds=50,
                random_state=42,
            )
            model.fit(
                X[train_idx], y[train_idx],
                eval_set=[(X[val_idx], y[val_idx])],
                verbose=False,
            )
            y_pred = model.predict(X[val_idx])
            rmse = float(np.sqrt(np.mean((y[val_idx] - y_pred) ** 2)))
            rmses.append(rmse)

        return float(np.mean(rmses))

    study = optuna.create_study(direction="minimize", study_name="xgboost_tuning")
    study.optimize(objective, n_trials=n_trials, show_progress_bar=True)

    logger.info("Best trial RMSE: %.1f", study.best_value)
    logger.info("Best params: %s", study.best_params)
    return study.best_params
