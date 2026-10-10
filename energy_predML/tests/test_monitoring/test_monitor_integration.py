"""Integration tests: monitoring workflow → drift detection → retrain flag.

generate_drift_report and save_report are mocked to return controlled values
so tests can focus on the flag-writing and early-return logic without
needing carefully crafted Evidently-detectable distributions.
The DuckDB data layer (build_features, load_demand, load_predictions) runs
against the real temp DB.
"""

from unittest.mock import MagicMock, patch

import pytest

# A reusable snapshot stub — Evidently snapshot is never called in these tests
_MOCK_SNAPSHOT = MagicMock()

_NO_DRIFT = {"drift_share": 0.05, "drifted_columns": [], "dataset_drift": False}
_HIGH_DRIFT = {
    "drift_share": 0.80,
    "drifted_columns": ["demand_lag_24h", "demand_lag_168h", "hour_of_day"],
    "dataset_drift": True,
}


@pytest.fixture
def monitoring_paths(tmp_path, monkeypatch):
    """Redirect flag, cutoff, and report paths to isolated temp locations."""
    retrain_flag = tmp_path / "retrain_needed"
    cutoff_file = tmp_path / "train_cutoff.txt"
    reports_dir = tmp_path / "drift_reports"
    reports_dir.mkdir()

    monkeypatch.setattr("src.workflows.monitor.RETRAIN_FLAG_PATH", retrain_flag)
    monkeypatch.setattr("src.workflows.monitor.TRAIN_CUTOFF_PATH", cutoff_file)
    monkeypatch.setattr("src.monitoring.drift.REPORTS_DIR", reports_dir)

    return {
        "retrain_flag": retrain_flag,
        "cutoff_file": cutoff_file,
        "reports_dir": reports_dir,
    }


def test_no_drift_does_not_write_retrain_flag(seed_demand, monitoring_paths):
    """When drift share is below 30%, the retrain flag must not be created."""
    from src.workflows.monitor import run_monitoring

    fake_report = monitoring_paths["reports_dir"] / "report.html"

    with (
        patch(
            "src.workflows.monitor.generate_drift_report",
            return_value=(_MOCK_SNAPSHOT, _NO_DRIFT),
        ),
        patch("src.workflows.monitor.save_report", return_value=fake_report),
    ):
        result = run_monitoring(region="CISO")

    assert result["needs_retraining"] is False
    assert not monitoring_paths["retrain_flag"].exists()


def test_drift_writes_retrain_flag(seed_demand, monitoring_paths):
    """When drift share exceeds 30%, the retrain flag must be written to disk."""
    from src.workflows.monitor import run_monitoring

    fake_report = monitoring_paths["reports_dir"] / "report.html"

    with (
        patch(
            "src.workflows.monitor.generate_drift_report",
            return_value=(_MOCK_SNAPSHOT, _HIGH_DRIFT),
        ),
        patch("src.workflows.monitor.save_report", return_value=fake_report),
        # Prevent auto-retrain from running (and deleting the flag)
        patch(
            "src.workflows.train.run_training",
            side_effect=Exception("blocked in test"),
        ),
    ):
        result = run_monitoring(region="CISO")

    assert result["needs_retraining"] is True
    assert monitoring_paths["retrain_flag"].exists()

    # Cleanup so other tests aren't affected
    monitoring_paths["retrain_flag"].unlink(missing_ok=True)


def test_skips_regression_report_when_fewer_than_48_predictions(
    seed_demand, monitoring_paths
):
    """With no logged predictions, the regression report path must be None."""
    from src.workflows.monitor import run_monitoring

    fake_report = monitoring_paths["reports_dir"] / "report.html"

    with (
        patch(
            "src.workflows.monitor.generate_drift_report",
            return_value=(_MOCK_SNAPSHOT, _NO_DRIFT),
        ),
        patch("src.workflows.monitor.save_report", return_value=fake_report),
    ):
        result = run_monitoring(region="CISO")

    assert result["regression_report_path"] is None


def test_insufficient_new_data_after_cutoff_returns_early(seed_demand, monitoring_paths):
    """When a cutoff file exists but fewer than 24 rows follow it, return early
    without calling generate_drift_report at all."""
    from src.data.store import load_demand
    from src.workflows.monitor import run_monitoring

    # Place the cutoff just before the last row → only 1 row of "current" data
    demand = load_demand(region="CISO")
    cutoff_ts = demand["timestamp"].iloc[-2]
    monitoring_paths["cutoff_file"].write_text(str(cutoff_ts))

    with patch(
        "src.workflows.monitor.generate_drift_report"
    ) as mock_drift:
        result = run_monitoring(region="CISO")

    # Early return path: no drift check, no report, no flag
    mock_drift.assert_not_called()
    assert result["needs_retraining"] is False
    assert result["drift_report_path"] is None
    assert not monitoring_paths["retrain_flag"].exists()
