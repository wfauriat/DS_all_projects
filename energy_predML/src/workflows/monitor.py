"""Drift monitoring: compare reference vs current data, trigger retraining on drift."""

import logging

import pandas as pd

from src.config import RETRAIN_FLAG_PATH, TRAIN_CUTOFF_PATH, settings
from src.data.store import load_demand, load_predictions
from src.features.pipeline import NON_FEATURE_COLS, build_features
from src.monitoring.drift import (
    generate_drift_report,
    generate_regression_report,
    save_report,
)

logger = logging.getLogger(__name__)


def _load_matched_predictions(region: str, min_rows: int = 48) -> pd.DataFrame | None:
    """Inner-join logged predictions with actual demand on (timestamp, region).

    Args:
        region: Grid region code.
        min_rows: Minimum matched rows required; returns None if below threshold.

    Returns:
        DataFrame with columns [prediction, target], or None if insufficient data.
    """
    preds = load_predictions(region=region)
    if preds.empty:
        return None

    actuals = load_demand(region=region)
    merged = preds.merge(
        actuals[["timestamp", "region", "demand_mwh"]],
        on=["timestamp", "region"],
        how="inner",
        suffixes=("_pred", "_actual"),
    )

    if len(merged) < min_rows:
        return None

    return pd.DataFrame({
        "prediction": merged["demand_mwh_pred"].values,
        "target":     merged["demand_mwh_actual"].values,
    })


def run_monitoring(
    train_ratio: float = 0.8,
    region: str | None = None,
) -> dict:
    """Run drift monitoring by comparing training data (reference) vs recent data (current).

    Args:
        train_ratio: Fraction of data used for training (reference).
        region: Grid region code.

    Returns:
        Dictionary with drift detection results, report paths, and retrain status.
    """
    region = region or settings.DEFAULT_REGION

    # Build features from all available data
    df = build_features(region=region)

    # Prefer a cutoff-based split: reference = data the model was trained on,
    # current = data that arrived after the last training run.
    # Falls back to a fixed 80/20 split when no cutoff exists or when there
    # is too little new data (< 24 rows) to produce a meaningful report.
    MIN_CURRENT_ROWS = 24
    if TRAIN_CUTOFF_PATH.exists():
        cutoff_ts = pd.Timestamp(TRAIN_CUTOFF_PATH.read_text().strip())
        reference_df = df[df["timestamp"] <= cutoff_ts].copy()
        current_df   = df[df["timestamp"] >  cutoff_ts].copy()
        if len(current_df) < MIN_CURRENT_ROWS:
            logger.info(
                "Only %d rows after training cutoff (%s) — not enough new data "
                "for a meaningful drift report. Run local-fetch / docker-fetch to "
                "ingest fresh data, then re-run monitoring.",
                len(current_df),
                cutoff_ts,
            )
            return {
                "drift_metrics": {"drift_share": 0.0},
                "drift_report_path": None,
                "regression_report_path": None,
                "needs_retraining": False,
                "retrain_triggered": False,
            }
        logger.info(
            "Using cutoff split: reference %d rows (up to %s), current %d rows (after %s)",
            len(reference_df), cutoff_ts, len(current_df), cutoff_ts,
        )
    else:
        split_idx = int(len(df) * train_ratio)
        reference_df = df.iloc[:split_idx].copy()
        current_df   = df.iloc[split_idx:].copy()
        logger.info(
            "No training cutoff found — using %.0f/%0.f split: "
            "reference %d rows, current %d rows",
            train_ratio * 100, (1 - train_ratio) * 100,
            len(reference_df), len(current_df),
        )

    feature_cols = [c for c in df.columns if c not in NON_FEATURE_COLS]

    # 1. Data drift report
    logger.info("--- Generating data drift report ---")
    drift_report, drift_metrics = generate_drift_report(
        reference_df, current_df, feature_cols
    )
    drift_html = save_report(drift_report, "data_drift")

    # 2. Regression performance report using real logged predictions
    logger.info("--- Generating regression report ---")
    result = {
        "drift_metrics": drift_metrics,
        "drift_report_path": str(drift_html),
        "regression_report_path": None,
        "needs_retraining": False,
        "retrain_triggered": False,
    }

    matched = _load_matched_predictions(region)
    if matched is not None:
        split_idx_pred = int(len(matched) * train_ratio)
        regression_snapshot = generate_regression_report(
            matched.iloc[:split_idx_pred], matched.iloc[split_idx_pred:]
        )
        regression_html = save_report(regression_snapshot, "regression_perf")
        result["regression_report_path"] = str(regression_html)
    else:
        logger.info(
            "Skipping regression report: fewer than 48 matched predictions. "
            "Run the API to accumulate predictions first."
        )

    # 3. Check if retraining is needed
    drift_share = drift_metrics.get("drift_share", 0)
    needs_retraining = drift_share > settings.DRIFT_THRESHOLD
    result["needs_retraining"] = needs_retraining

    if not needs_retraining:
        logger.info(
            "No significant drift: %.0f%% of features drifted (threshold: %.0f%%)",
            drift_share * 100,
            settings.DRIFT_THRESHOLD * 100,
        )
        return result

    # 4. Drift detected — write flag file and attempt auto-retrain
    logger.warning(
        "DRIFT DETECTED: %.0f%% of features drifted (threshold: %.0f%%).",
        drift_share * 100,
        settings.DRIFT_THRESHOLD * 100,
    )
    RETRAIN_FLAG_PATH.touch()
    logger.info("Retrain flag written to %s", RETRAIN_FLAG_PATH)

    try:
        from src.workflows.train import run_training  # noqa: PLC0415 — lazy: not available in monitor container
        logger.info("Training deps available — running auto-retrain (no tuning)...")
        run_training(use_tuning=False)
        RETRAIN_FLAG_PATH.unlink(missing_ok=True)
        result["retrain_triggered"] = True
        logger.info("Auto-retrain completed successfully.")
    except ImportError:
        logger.info(
            "Docker monitor mode: training deps unavailable. "
            "Flag written — run `make docker-train` to retrain."
        )
    except Exception as train_err:
        logger.error("Auto-retrain failed: %s", train_err, exc_info=True)
        # Flag intentionally left in place so the next monitoring run retries.

    return result


if __name__ == "__main__":
    result = run_monitoring()
    print(f"\nDrift share: {result['drift_metrics'].get('drift_share', 0):.1%}")
    print(f"Needs retraining: {result['needs_retraining']}")
    print(f"Retrain triggered: {result['retrain_triggered']}")
    print(f"Drift report: {result['drift_report_path']}")
    print(f"Regression report: {result['regression_report_path']}")
