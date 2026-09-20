"""Drift detection using Evidently AI."""

import logging
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
from evidently import DataDefinition, Dataset, Regression, Report
from evidently.presets import DataDriftPreset, RegressionPreset

from src.config import DATA_DIR

logger = logging.getLogger(__name__)

REPORTS_DIR = DATA_DIR / "drift_reports"


def generate_drift_report(
    reference_df: pd.DataFrame,
    current_df: pd.DataFrame,
    feature_columns: list[str],
) -> tuple[object, dict]:
    """Generate a data drift report comparing reference and current data.

    Args:
        reference_df: Training/reference data.
        current_df: Recent production data.
        feature_columns: List of feature columns to monitor.

    Returns:
        Tuple of (Evidently Snapshot, drift metrics dict).
    """
    ref = reference_df[feature_columns].copy()
    cur = current_df[feature_columns].copy()

    report = Report(metrics=[DataDriftPreset()])
    snapshot = report.run(reference_data=ref, current_data=cur)

    # Extract drift metrics from snapshot
    metric_results = snapshot.dump_dict().get("metric_results", {})
    drift_summary = {"drifted_columns": [], "total_columns": len(feature_columns)}

    for val in metric_results.values():
        name = val.get("display_name", "")
        if name == "Count of Drifted Columns":
            count_info = val.get("counts", {})
            drift_summary["number_of_drifted_columns"] = count_info.get("drifted", 0)
        elif name.startswith("Value drift for "):
            col_name = name.replace("Value drift for ", "")
            p_value = val.get("value", 1.0)
            if p_value < 0.05:  # standard significance threshold
                drift_summary["drifted_columns"].append(col_name)

    n_drifted = len(drift_summary["drifted_columns"])
    drift_summary["number_of_drifted_columns"] = n_drifted
    drift_summary["drift_share"] = n_drifted / max(len(feature_columns), 1)
    drift_summary["dataset_drift"] = drift_summary["drift_share"] > 0.3

    logger.info(
        "Drift report: %d/%d features drifted (share=%.2f, dataset_drift=%s)",
        n_drifted,
        len(feature_columns),
        drift_summary["drift_share"],
        drift_summary["dataset_drift"],
    )
    return snapshot, drift_summary


def generate_regression_report(
    reference_df: pd.DataFrame,
    current_df: pd.DataFrame,
) -> object:
    """Generate a regression performance report.

    Both DataFrames must have 'target' and 'prediction' columns.

    Args:
        reference_df: Historical predictions with actuals.
        current_df: Recent predictions with actuals.

    Returns:
        Evidently Snapshot.
    """
    dd = DataDefinition(regression=[Regression(target="target", prediction="prediction")])

    report = Report(metrics=[RegressionPreset()])
    snapshot = report.run(
        reference_data=Dataset.from_pandas(reference_df, data_definition=dd),
        current_data=Dataset.from_pandas(current_df, data_definition=dd),
    )

    logger.info("Regression report generated")
    return snapshot


def save_report(snapshot: object, name: str) -> Path:
    """Save an Evidently snapshot as HTML and JSON.

    Args:
        snapshot: Evidently Snapshot object.
        name: Base name for the report files.

    Returns:
        Path to the HTML report.
    """
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")

    html_path = REPORTS_DIR / f"{name}_{timestamp}.html"
    json_path = REPORTS_DIR / f"{name}_{timestamp}.json"

    snapshot.save_html(str(html_path))
    snapshot.save_json(str(json_path))

    logger.info("Report saved: %s", html_path)
    return html_path
