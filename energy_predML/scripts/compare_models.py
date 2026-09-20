"""Compare 1-step vs 24h-ahead recursive forecasting."""

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from src.features.pipeline import build_features, save_features
from src.models.evaluate import compute_metrics
from src.models.xgboost_model import get_feature_columns, train_xgboost, tune_xgboost

HORIZON = 24  # hours ahead


def recursive_forecast_24h(model, df_full, feature_cols, start_idx, horizon=HORIZON):
    """Generate a 24h-ahead recursive forecast starting from start_idx.

    At each step, predict demand for the next hour, then feed that prediction
    back into the lag/rolling/diff features for subsequent steps.
    Only uses predicted (or past real) values — never peeks at future actuals.

    Args:
        model: Trained XGBoost model.
        df_full: Full feature DataFrame (need history before start_idx for lags).
        feature_cols: List of feature column names.
        start_idx: Integer index in df_full where the forecast window starts.
        horizon: Number of steps to forecast.

    Returns:
        Array of predicted demand values (length = horizon).
    """
    # Work on a copy so we can mutate values
    work = df_full.copy()
    preds = []

    for step in range(horizon):
        idx = start_idx + step
        if idx >= len(work):
            break

        # Predict this step
        X = work.iloc[idx][feature_cols].values.reshape(1, -1)
        pred = float(model.predict(X)[0])
        preds.append(pred)

        # Write prediction into demand_mwh so future lags use it
        work.iloc[idx, work.columns.get_loc("demand_mwh")] = pred

        # Update features for future rows that will reference this prediction
        # Lag features: row at idx+lag_hours will have demand_lag_Xh = pred
        for lag_hours, lag_col in [
            (1, "demand_lag_1h"),
            (24, "demand_lag_24h"),
            (48, "demand_lag_48h"),
            (168, "demand_lag_168h"),
        ]:
            target_idx = idx + lag_hours
            if target_idx < len(work):
                work.iloc[target_idx, work.columns.get_loc(lag_col)] = pred

        # Update diff features for next row
        # demand_diff_1h[t] = demand[t-1] - demand[t-2] (no leakage)
        if idx + 1 < len(work) and idx - 1 >= 0:
            prev_demand = work.iloc[idx - 1]["demand_mwh"]
            work.iloc[idx + 1, work.columns.get_loc("demand_diff_1h")] = pred - prev_demand

        # demand_diff_24h[t] = demand[t-1] - demand[t-25]
        if idx + 1 < len(work) and idx - 24 >= 0:
            demand_25_ago = work.iloc[idx - 24]["demand_mwh"]
            work.iloc[idx + 1, work.columns.get_loc("demand_diff_24h")] = pred - demand_25_ago

        # Update rolling features for next row
        if idx + 1 < len(work):
            # Rolling 24h: mean/std of demand[t-24..t-1] — now demand[t-1]=pred
            start_roll = max(0, idx - 23)
            recent_24 = work.iloc[start_roll:idx + 1]["demand_mwh"].values
            if len(recent_24) == 24:
                work.iloc[idx + 1, work.columns.get_loc("demand_roll_24h_mean")] = np.mean(recent_24)
                work.iloc[idx + 1, work.columns.get_loc("demand_roll_24h_std")] = np.std(recent_24, ddof=1)

            # Rolling 7d
            start_roll_7d = max(0, idx - 167)
            recent_168 = work.iloc[start_roll_7d:idx + 1]["demand_mwh"].values
            if len(recent_168) == 168:
                work.iloc[idx + 1, work.columns.get_loc("demand_roll_7d_mean")] = np.mean(recent_168)
                work.iloc[idx + 1, work.columns.get_loc("demand_roll_7d_std")] = np.std(recent_168, ddof=1)

    return np.array(preds)


def run_recursive_over_validation(model, df_full, feature_cols, val_start_idx, horizon=HORIZON):
    """Run multiple non-overlapping 24h forecast windows across the validation set.

    Returns arrays of (timestamps, actuals, 1-step preds, recursive preds).
    """
    all_timestamps = []
    all_actuals = []
    all_1step = []
    all_recursive = []

    idx = val_start_idx
    while idx + horizon <= len(df_full):
        window = df_full.iloc[idx:idx + horizon]
        ts = pd.to_datetime(window["timestamp"]).dt.tz_localize(None).values
        actuals = window["demand_mwh"].values

        # 1-step predictions (uses real lags — best case)
        preds_1step = model.predict(window[feature_cols].values)

        # 24h recursive forecast (uses own predictions as inputs)
        preds_recursive = recursive_forecast_24h(model, df_full, feature_cols, idx, horizon)

        all_timestamps.extend(ts)
        all_actuals.extend(actuals)
        all_1step.extend(preds_1step)
        all_recursive.extend(preds_recursive)

        idx += horizon  # non-overlapping windows

    return (
        np.array(all_timestamps),
        np.array(all_actuals),
        np.array(all_1step),
        np.array(all_recursive),
    )


# ============================================================
# Main
# ============================================================

# Rebuild features (diff features were fixed)
print("Rebuilding features with fixed diff (no leakage)...")
df = build_features()
save_features(df)

split_idx = int(len(df) * 0.8)
train_df = df.iloc[:split_idx].copy()
val_df = df.iloc[split_idx:].copy()
feature_cols = get_feature_columns(df)

# Train tuned model
print("Tuning XGBoost (10 trials)...")
best_params = tune_xgboost(train_df, n_trials=10, n_splits=3)
model, metrics_1step = train_xgboost(train_df, val_df, params=best_params)

# Run recursive forecasting
print("Running recursive 24h-ahead forecasts...")
timestamps, actuals, preds_1step, preds_recursive = run_recursive_over_validation(
    model, df, feature_cols, split_idx, HORIZON
)

metrics_1step_full = compute_metrics(actuals, preds_1step)
metrics_recursive = compute_metrics(actuals, preds_recursive)

print(f"\n{'='*50}")
print(f"1-step-ahead  — RMSE: {metrics_1step_full['rmse']:.0f}, MAE: {metrics_1step_full['mae']:.0f}, MAPE: {metrics_1step_full['mape']:.2f}%")
print(f"24h recursive — RMSE: {metrics_recursive['rmse']:.0f}, MAE: {metrics_recursive['mae']:.0f}, MAPE: {metrics_recursive['mape']:.2f}%")
print(f"{'='*50}\n")

# --- Plot ---
fig, axes = plt.subplots(4, 1, figsize=(14, 16))

# Panel 1: 1-step predictions vs actual
ax = axes[0]
ax.plot(timestamps, actuals, "k-", linewidth=1.5, label="Actual", alpha=0.9)
ax.plot(timestamps, preds_1step, "-", linewidth=1, color="#2196F3",
        label=f"1-step-ahead (RMSE={metrics_1step_full['rmse']:.0f})", alpha=0.7)
ax.set_ylabel("Demand (MWh)")
ax.set_title("1-Step-Ahead Predictions (uses real past demand)")
ax.legend(loc="upper right")
ax.grid(True, alpha=0.3)
ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
ax.xaxis.set_major_locator(mdates.DayLocator())
plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha="right")

# Panel 2: 24h recursive predictions vs actual
ax = axes[1]
ax.plot(timestamps, actuals, "k-", linewidth=1.5, label="Actual", alpha=0.9)
ax.plot(timestamps, preds_recursive, "-", linewidth=1, color="#FF5722",
        label=f"24h recursive (RMSE={metrics_recursive['rmse']:.0f})", alpha=0.7)
# Add vertical lines at forecast window boundaries
for i in range(0, len(timestamps), HORIZON):
    ax.axvline(timestamps[i], color="gray", linewidth=0.5, alpha=0.3, linestyle="--")
ax.set_ylabel("Demand (MWh)")
ax.set_title("24h-Ahead Recursive Forecast (uses own predictions as lag inputs)")
ax.legend(loc="upper right")
ax.grid(True, alpha=0.3)
ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
ax.xaxis.set_major_locator(mdates.DayLocator())
plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha="right")

# Panel 3: Both overlaid
ax = axes[2]
ax.plot(timestamps, actuals, "k-", linewidth=1.5, label="Actual", alpha=0.9)
ax.plot(timestamps, preds_1step, "-", linewidth=1, color="#2196F3", label="1-step-ahead", alpha=0.6)
ax.plot(timestamps, preds_recursive, "-", linewidth=1, color="#FF5722", label="24h recursive", alpha=0.6)
ax.set_ylabel("Demand (MWh)")
ax.set_title("Comparison: 1-Step vs 24h Recursive")
ax.legend(loc="upper right")
ax.grid(True, alpha=0.3)
ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
ax.xaxis.set_major_locator(mdates.DayLocator())
plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha="right")

# Panel 4: Error by forecast hour (how does error grow with horizon?)
ax = axes[3]
n_windows = len(actuals) // HORIZON
errors_by_hour = np.zeros((n_windows, HORIZON))
for w in range(n_windows):
    s = w * HORIZON
    e = s + HORIZON
    errors_by_hour[w] = np.abs(actuals[s:e] - preds_recursive[s:e])

mean_error_by_hour = errors_by_hour.mean(axis=0)
std_error_by_hour = errors_by_hour.std(axis=0)

hours = np.arange(1, HORIZON + 1)
ax.bar(hours, mean_error_by_hour, color="#FF9800", alpha=0.7, label="Mean |error|")
ax.errorbar(hours, mean_error_by_hour, yerr=std_error_by_hour,
            fmt="none", color="k", capsize=3, alpha=0.5)
ax.set_xlabel("Forecast Hour (hours ahead)")
ax.set_ylabel("Absolute Error (MWh)")
ax.set_title("Error Growth with Forecast Horizon (24h recursive)")
ax.set_xticks(hours)
ax.legend()
ax.grid(True, alpha=0.3, axis="y")

plt.tight_layout()
plt.savefig("model_comparison.png", dpi=150, bbox_inches="tight")
print("Saved plot to model_comparison.png")
plt.show()
