"""Streamlit dashboard for Energy Grid Load Forecasting."""

import sys
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import streamlit as st

# Ensure src is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.config import DATA_DIR, settings
from src.data.store import load_demand
from src.features.pipeline import NON_FEATURE_COLS, build_features, get_latest_features
from src.models.xgboost_model import get_feature_columns

st.set_page_config(page_title="Energy Grid Forecast", layout="wide")
st.title("Energy Grid Load Forecasting")

# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------
region = st.sidebar.selectbox("Region", ["CISO", "ERCO", "PJM", "MISO"], index=0)
st.sidebar.markdown("---")
st.sidebar.markdown("**Data source:** EIA Open Data API")
st.sidebar.markdown(f"**Region:** {region}")


# ---------------------------------------------------------------------------
# Helper: load model
# ---------------------------------------------------------------------------
@st.cache_resource
def load_model():
    """Load the trained XGBoost model from MLflow."""
    import mlflow

    mlflow.set_tracking_uri(settings.MLFLOW_TRACKING_URI)

    from mlflow.tracking import MlflowClient

    # 1. Try latest version from the model registry
    try:
        client = MlflowClient()
        versions = client.search_model_versions("name='energy_demand_xgboost'")
        if versions:
            latest = max(versions, key=lambda v: int(v.version))
            return mlflow.xgboost.load_model(
                f"models:/energy_demand_xgboost/{latest.version}"
            )
    except Exception:
        pass

    # 2. Fallback: most recent run tagged xgboost_tuned
    try:
        experiment = mlflow.get_experiment_by_name("energy_grid_forecast")
        if experiment:
            runs = mlflow.search_runs(
                experiment_ids=[experiment.experiment_id],
                filter_string="tags.model_type = 'xgboost_tuned'",
                order_by=["start_time DESC"],
                max_results=1,
            )
            if not runs.empty:
                run_id = runs.iloc[0]["run_id"]
                return mlflow.xgboost.load_model(f"runs:/{run_id}/model")
    except Exception:
        pass

    return None


# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------
tab1, tab2, tab3, tab4 = st.tabs([
    "Forecast",
    "Historical Accuracy",
    "Feature Importance",
    "Drift Reports",
])

# ===== Tab 1: Forecast =====
with tab1:
    st.header("24-Hour Demand Forecast")

    model = load_model()
    if model is None:
        st.error("No trained model found. Run `python -m src.workflows.train` first.")
    else:
        try:
            df = build_features(region=region)
            feature_cols = get_feature_columns(df)

            # Use last 24 rows as forecast display
            forecast_df = df.tail(24).copy()
            timestamps = pd.to_datetime(forecast_df["timestamp"]).dt.tz_localize(None)
            actuals = forecast_df["demand_mwh"].values
            predictions = model.predict(forecast_df[feature_cols].values)

            # Confidence bands (~2% MAPE)
            error_margin = predictions * 0.02
            upper = predictions + 2 * error_margin
            lower = predictions - 2 * error_margin

            fig, ax = plt.subplots(figsize=(12, 5))
            ax.plot(timestamps, actuals, "k-o", markersize=4, label="Actual", linewidth=1.5)
            ax.plot(timestamps, predictions, "b-o", markersize=4, label="Predicted", linewidth=1.5)
            ax.fill_between(timestamps, lower, upper, alpha=0.2, color="blue", label="95% CI")
            ax.set_xlabel("Time")
            ax.set_ylabel("Demand (MWh)")
            ax.set_title(f"Last 24 Hours — {region}")
            ax.legend()
            ax.grid(True, alpha=0.3)
            plt.xticks(rotation=45)
            plt.tight_layout()
            st.pyplot(fig)

            # Summary metrics
            rmse = float(np.sqrt(np.mean((actuals - predictions) ** 2)))
            mae = float(np.mean(np.abs(actuals - predictions)))
            col1, col2, col3 = st.columns(3)
            col1.metric("RMSE", f"{rmse:.0f} MWh")
            col2.metric("MAE", f"{mae:.0f} MWh")
            col3.metric("Avg Demand", f"{np.mean(actuals):.0f} MWh")

        except Exception as e:
            st.error(f"Error generating forecast: {e}")


# ===== Tab 2: Historical Accuracy =====
with tab2:
    st.header("Historical Accuracy")

    model = load_model()
    if model is None:
        st.error("No trained model found.")
    else:
        try:
            df = build_features(region=region)
            feature_cols = get_feature_columns(df)

            # Use last 30% as validation
            split_idx = int(len(df) * 0.7)
            val_df = df.iloc[split_idx:].copy()
            timestamps = pd.to_datetime(val_df["timestamp"]).dt.tz_localize(None)
            actuals = val_df["demand_mwh"].values
            predictions = model.predict(val_df[feature_cols].values)

            # Plot predicted vs actual
            fig, axes = plt.subplots(2, 1, figsize=(12, 8))

            ax = axes[0]
            ax.plot(timestamps, actuals, "k-", linewidth=1, label="Actual", alpha=0.9)
            ax.plot(timestamps, predictions, "b-", linewidth=1, label="Predicted", alpha=0.7)
            ax.set_ylabel("Demand (MWh)")
            ax.set_title(f"Predicted vs Actual — {region}")
            ax.legend()
            ax.grid(True, alpha=0.3)

            ax = axes[1]
            residuals = actuals - predictions
            ax.bar(timestamps, residuals, width=0.03, color=np.where(residuals > 0, "#4CAF50", "#F44336"), alpha=0.7)
            ax.axhline(0, color="k", linewidth=0.5)
            ax.set_ylabel("Residual (MWh)")
            ax.set_title("Prediction Residuals")
            ax.grid(True, alpha=0.3)

            plt.tight_layout()
            st.pyplot(fig)

            # Metrics
            rmse = float(np.sqrt(np.mean(residuals ** 2)))
            mae = float(np.mean(np.abs(residuals)))
            mape = float(np.mean(np.abs(residuals / actuals)) * 100)
            r2 = float(1 - np.sum(residuals ** 2) / np.sum((actuals - np.mean(actuals)) ** 2))

            col1, col2, col3, col4 = st.columns(4)
            col1.metric("RMSE", f"{rmse:.0f} MWh")
            col2.metric("MAE", f"{mae:.0f} MWh")
            col3.metric("MAPE", f"{mape:.2f}%")
            col4.metric("R²", f"{r2:.4f}")

        except Exception as e:
            st.error(f"Error: {e}")


# ===== Tab 3: Feature Importance =====
with tab3:
    st.header("Feature Importance")

    model = load_model()
    if model is None:
        st.error("No trained model found.")
    else:
        try:
            df = build_features(region=region)
            feature_cols = get_feature_columns(df)

            importances = model.feature_importances_
            feat_imp = pd.DataFrame({
                "feature": feature_cols,
                "importance": importances,
            }).sort_values("importance", ascending=True)

            fig, ax = plt.subplots(figsize=(10, 8))
            ax.barh(feat_imp["feature"], feat_imp["importance"], color="#2196F3")
            ax.set_xlabel("Importance (gain)")
            ax.set_title("XGBoost Feature Importance")
            ax.grid(True, alpha=0.3, axis="x")
            plt.tight_layout()
            st.pyplot(fig)

            # Show table
            st.dataframe(
                feat_imp.sort_values("importance", ascending=False).reset_index(drop=True),
                use_container_width=True,
            )

        except Exception as e:
            st.error(f"Error: {e}")


# ===== Tab 4: Drift Reports =====
with tab4:
    st.header("Drift Reports")

    reports_dir = DATA_DIR / "drift_reports"

    if not reports_dir.exists():
        st.info("No drift reports found. Run `python -m src.workflows.monitor` to generate reports.")
    else:
        html_files = sorted(reports_dir.glob("*.html"), reverse=True)

        if not html_files:
            st.info("No drift reports found. Run `python -m src.workflows.monitor` to generate reports.")
        else:
            selected = st.selectbox(
                "Select report",
                html_files,
                format_func=lambda p: p.name,
            )

            if selected:
                html_content = selected.read_text()
                st.components.v1.html(html_content, height=800, scrolling=True)
