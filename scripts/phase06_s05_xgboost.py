"""
Phase 06 - Step 05: XGBoost.

Purpose:
    Fit an XGBoost regressor on panel_ml.csv with the pre-registered
    Decision 4 hyperparameter grid using RandomizedSearchCV (n_iter=50)
    over TimeSeriesSplit folds, then evaluate on the temporal-holdout
    test set and report native gain-based feature importance plus
    permutation importance.

    Per Decision 5 XGBoost policy:
    - 12 numeric features -> NaN preserved (XGBoost native handling;
      XGBoost learns optimal default direction at each split)
    - cluster_kmeans_k3 -> NaN preserved, value passed as float
      (XGBoost treats it as numerical with optimal-default-direction
      learning for missing values)
    - No SimpleImputer / no scaling.

    Per Convention 6.7: random KFold metrics reported alongside
    temporal-split metrics.

Inputs:
    data/processed/panel_ml.csv

Outputs:
    outputs/tables/phase06_s05_xgb_results.csv
    outputs/tables/phase06_s05_predictions.csv
    outputs/models/phase06_s05_xgb.joblib
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

from sklearn.inspection import permutation_importance
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import KFold, RandomizedSearchCV, TimeSeriesSplit

import xgboost as xgb

# Make src importable.
PROJECT_ROOT_HINT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT_HINT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT_HINT))

from src.paths import find_project_root
from src.io_utils import read_csv_with_encoding_fallback

SEED = 42
np.random.seed(SEED)

NUMERIC_FEATURES = [
    "mean_years_schooling",
    "enrol_secondary",
    "log_gdp_per_capita_ppp",
    "log_population",
    "urban_population_pct",
    "agri_value_added_gdp",
    "manu_value_added_gdp",
    "services_value_added_gdp",
    "trade_openness",
    "gov_expenditure_gdp",
    "inflation_cpi",
    "unemployment_rate",
]
CLUSTER_FEATURE = "cluster_kmeans_k3"
TARGET = "gini"
ALL_FEATURES = NUMERIC_FEATURES + [CLUSTER_FEATURE]

# Decision 4 pre-registered XGBoost grid.
XGB_PARAM_GRID = {
    "n_estimators": [100, 300, 500, 1000],
    "max_depth": [3, 5, 7, 10],
    "learning_rate": [0.01, 0.05, 0.1],
    "subsample": [0.6, 0.8, 1.0],
    "colsample_bytree": [0.6, 0.8, 1.0],
    "reg_lambda": [0, 1, 5, 10],
    "reg_alpha": [0, 0.1, 1.0],
}
N_ITER = 50
N_SPLITS = 5
PERM_N_REPEATS = 10


def report_metrics(y_true: np.ndarray, y_pred: np.ndarray, label: str) -> dict:
    rmse = float(np.sqrt(mean_squared_error(y_true, y_pred)))
    mae = float(mean_absolute_error(y_true, y_pred))
    r2 = float(r2_score(y_true, y_pred))
    print(f"  {label:<8} RMSE={rmse:.3f}  MAE={mae:.3f}  R2={r2:.3f}  N={len(y_true):,}")
    return {"split": label, "rmse": rmse, "mae": mae, "r2": r2, "n": len(y_true)}


def _is_at_boundary(value, grid: list) -> bool:
    """Return True if value is at the min or max of a numeric grid."""
    numeric_grid = [v for v in grid if isinstance(v, (int, float))
                    and not isinstance(v, bool)]
    if not numeric_grid or value not in numeric_grid:
        return False
    return value in (min(numeric_grid), max(numeric_grid))


def main() -> None:
    project_root = find_project_root()
    in_path = project_root / "data" / "processed" / "panel_ml.csv"
    tables_dir = project_root / "outputs" / "tables"
    models_dir = project_root / "outputs" / "models"
    tables_dir.mkdir(parents=True, exist_ok=True)
    models_dir.mkdir(parents=True, exist_ok=True)
    out_metrics = tables_dir / "phase06_s05_xgb_results.csv"
    out_preds = tables_dir / "phase06_s05_predictions.csv"
    out_model = models_dir / "phase06_s05_xgb.joblib"

    print(f"[phase06_s05] project root: {project_root}")
    print(f"[phase06_s05] input:        {in_path}")
    print(f"[phase06_s05] xgboost ver:  {xgb.__version__}")
    print(f"[phase06_s05] grid size:    "
          f"{int(np.prod([len(v) for v in XGB_PARAM_GRID.values()]))} combinations")
    print(f"[phase06_s05] n_iter:       {N_ITER}")
    print(f"[phase06_s05] CV folds:     {N_SPLITS} (TimeSeriesSplit)")

    # ---- Load ----
    df, enc = read_csv_with_encoding_fallback(in_path)
    print(f"[phase06_s05] loaded:       {df.shape[0]:,} rows x "
          f"{df.shape[1]} cols (encoding={enc})")

    # ---- Build feature matrix (NaN preserved) ----
    X_full = df[ALL_FEATURES].copy().astype(float)
    y_full = df[TARGET].astype(float)
    print(f"[phase06_s05] feature matrix: {X_full.shape}")
    nan_total = int(X_full.isna().sum().sum())
    print(f"[phase06_s05] NaN cells in X (preserved): {nan_total}")

    # ---- Train / test split ----
    train_mask = df["split"].values == "train"
    test_mask = df["split"].values == "test"

    X_train = X_full.loc[train_mask].reset_index(drop=True)
    y_train = y_full.loc[train_mask].reset_index(drop=True)
    X_test = X_full.loc[test_mask].reset_index(drop=True)
    y_test = y_full.loc[test_mask].reset_index(drop=True)
    print(f"[phase06_s05] train: {len(X_train):,}, test: {len(X_test):,}")

    # ---- Sort train rows by year for TimeSeriesSplit ----
    train_meta = df.loc[train_mask, ["iso3", "year"]].reset_index(drop=True)
    sort_idx = train_meta["year"].argsort(kind="mergesort").values
    X_train_sorted = X_train.iloc[sort_idx].reset_index(drop=True)
    y_train_sorted = y_train.iloc[sort_idx].reset_index(drop=True)

    # ---- RandomizedSearchCV ----
    base_xgb = xgb.XGBRegressor(
        random_state=SEED,
        objective="reg:squarederror",
        tree_method="hist",  # fast; supports missing values natively
        n_jobs=1,            # leave parallelism to RandomizedSearchCV
        verbosity=0,
    )
    tss = TimeSeriesSplit(n_splits=N_SPLITS)

    print(f"\n[phase06_s05] starting RandomizedSearchCV "
          f"(this can take a few minutes)...")
    t0 = time.time()
    rs = RandomizedSearchCV(
        estimator=base_xgb,
        param_distributions=XGB_PARAM_GRID,
        n_iter=N_ITER,
        scoring="neg_root_mean_squared_error",
        cv=tss,
        n_jobs=-1,
        refit=True,
        random_state=SEED,
        verbose=0,
    )
    rs.fit(X_train_sorted, y_train_sorted)
    elapsed = time.time() - t0
    print(f"[phase06_s05] search complete in {elapsed:.1f}s")

    best_params = rs.best_params_
    best_cv_rmse = -rs.best_score_
    print(f"[phase06_s05] best params:")
    for k, v in best_params.items():
        print(f"  {k}: {v}")
    print(f"[phase06_s05] best CV RMSE: {best_cv_rmse:.3f}")

    # ---- Boundary check ----
    boundary_warnings = []
    for hp_name, hp_value in best_params.items():
        grid = XGB_PARAM_GRID[hp_name]
        if _is_at_boundary(hp_value, grid):
            boundary_warnings.append((hp_name, hp_value, grid))
    if boundary_warnings:
        print(f"\n[phase06_s05] WARNING: best params at grid boundary:")
        for name, val, grid in boundary_warnings:
            print(f"  {name} = {val}  (grid: {grid})")
        print(f"  consider Step 05b override if portfolio narrative requires.")
    else:
        print(f"[phase06_s05] grid-boundary check: all best params in interior.")

    # ---- Performance on temporal split ----
    best_model = rs.best_estimator_
    print(f"\n[phase06_s05] performance:")
    train_pred = best_model.predict(X_train)
    test_pred = best_model.predict(X_test)
    metrics_train = report_metrics(y_train.values, train_pred, "train")
    metrics_test = report_metrics(y_test.values, test_pred, "test")

    # ---- Random KFold diagnostic ----
    print(f"\n[phase06_s05] random KFold diagnostic (n_splits={N_SPLITS}):")
    rkf = KFold(n_splits=N_SPLITS, shuffle=True, random_state=SEED)
    rkf_rmses = []
    for fold_idx, (tr, va) in enumerate(rkf.split(X_train)):
        rkf_model = xgb.XGBRegressor(
            random_state=SEED,
            objective="reg:squarederror",
            tree_method="hist",
            n_jobs=-1,
            verbosity=0,
            **best_params,
        )
        rkf_model.fit(X_train.iloc[tr], y_train.iloc[tr])
        pred_va = rkf_model.predict(X_train.iloc[va])
        rmse_va = float(np.sqrt(mean_squared_error(y_train.iloc[va], pred_va)))
        rkf_rmses.append(rmse_va)
        print(f"  fold {fold_idx + 1}: RMSE={rmse_va:.3f}")
    rkf_mean = float(np.mean(rkf_rmses))
    rkf_std = float(np.std(rkf_rmses, ddof=1))
    leakage_gap = best_cv_rmse - rkf_mean
    print(f"  mean: {rkf_mean:.3f}  (sd {rkf_std:.3f})")
    print(f"  vs TimeSeriesSplit best CV RMSE: {best_cv_rmse:.3f}")
    print(f"  gap (TSS - RKF): {leakage_gap:+.3f}")

    # ---- Importance: native gain-based ----
    fitted_xgb = best_model
    try:
        gain_importance = fitted_xgb.feature_importances_
    except Exception as e:
        raise RuntimeError(f"Failed to read feature_importances_: {e}")

    if len(gain_importance) != len(ALL_FEATURES):
        raise AssertionError(
            f"Feature length mismatch: gain importance has "
            f"{len(gain_importance)} entries, expected {len(ALL_FEATURES)}"
        )

    # ---- Importance: permutation on test set ----
    print(f"\n[phase06_s05] computing permutation importance on test "
          f"(n_repeats={PERM_N_REPEATS})...")
    t0 = time.time()
    perm = permutation_importance(
        best_model,
        X_test,
        y_test,
        n_repeats=PERM_N_REPEATS,
        random_state=SEED,
        n_jobs=-1,
        scoring="neg_root_mean_squared_error",
    )
    elapsed = time.time() - t0
    print(f"  done in {elapsed:.1f}s")

    importance_df = pd.DataFrame({
        "feature": ALL_FEATURES,
        "gain_importance": gain_importance,
        "perm_importance_mean": perm.importances_mean,
        "perm_importance_std": perm.importances_std,
    })
    importance_df = importance_df.sort_values(
        "gain_importance", ascending=False
    ).reset_index(drop=True)
    importance_df.insert(0, "rank_by_gain",
                         range(1, len(importance_df) + 1))

    print(f"\n[phase06_s05] feature importance (sorted by gain):")
    print(f"  {'rank':>4}  {'feature':<28}  {'gain':>10}  "
          f"{'perm_mean':>10}  {'perm_sd':>9}")
    for _, row in importance_df.iterrows():
        print(f"  {row['rank_by_gain']:>4}  {row['feature']:<28}  "
              f"{row['gain_importance']:>10.4f}  "
              f"{row['perm_importance_mean']:>10.4f}  "
              f"{row['perm_importance_std']:>9.4f}")

    # ---- Build long-format metrics CSV ----
    metrics_rows = []
    for k, v in best_params.items():
        metrics_rows.append({
            "model": "xgb", "metric": f"best_{k}",
            "value": str(v), "split": None, "n": None,
        })
    metrics_rows.append({
        "model": "xgb", "metric": "tss_cv_rmse_best",
        "value": best_cv_rmse, "split": "tss_cv", "n": len(X_train),
    })
    metrics_rows.append({
        "model": "xgb", "metric": "rkf_cv_rmse_mean",
        "value": rkf_mean, "split": "rkf_cv", "n": len(X_train),
    })
    metrics_rows.append({
        "model": "xgb", "metric": "rkf_cv_rmse_std",
        "value": rkf_std, "split": "rkf_cv", "n": len(X_train),
    })
    for m in (metrics_train, metrics_test):
        for key in ("rmse", "mae", "r2"):
            metrics_rows.append({
                "model": "xgb", "metric": key,
                "value": m[key], "split": m["split"], "n": m["n"],
            })

    metrics_df = pd.DataFrame(metrics_rows)
    imp_long = importance_df.melt(
        id_vars=["feature", "rank_by_gain"],
        value_vars=["gain_importance", "perm_importance_mean",
                    "perm_importance_std"],
        var_name="importance_type", value_name="value",
    )
    imp_long["model"] = "xgb"
    imp_long["metric"] = imp_long["importance_type"] + "_" + imp_long["feature"]
    imp_long["split"] = None
    imp_long["n"] = None
    imp_long = imp_long[[
        "model", "metric", "value", "split", "n", "rank_by_gain",
    ]]
    metrics_df = pd.concat([metrics_df, imp_long], ignore_index=True)
    metrics_df.to_csv(out_metrics, index=False, encoding="utf-8")
    print(f"\n[phase06_s05] wrote: {out_metrics}")
    print(f"  rows: {len(metrics_df):,}")

    # ---- Predictions CSV ----
    preds_rows = []
    for split_label, mask, y_arr, pred_arr in [
        ("train", train_mask, y_train.values, train_pred),
        ("test", test_mask, y_test.values, test_pred),
    ]:
        sub = df.loc[mask, ["iso3", "year", CLUSTER_FEATURE]].reset_index(drop=True)
        sub["split"] = split_label
        sub["y_true"] = y_arr
        sub["y_pred"] = pred_arr
        sub["residual"] = sub["y_true"] - sub["y_pred"]
        sub["model"] = "xgb"
        preds_rows.append(sub)
    preds_df = pd.concat(preds_rows, ignore_index=True)
    preds_df = preds_df[[
        "model", "iso3", "year", "split", "y_true", "y_pred",
        "residual", CLUSTER_FEATURE,
    ]]
    preds_df.to_csv(out_preds, index=False, encoding="utf-8")
    print(f"[phase06_s05] wrote: {out_preds}")
    print(f"  rows: {len(preds_df):,}")

    # ---- Save model ----
    joblib.dump(best_model, out_model)
    print(f"[phase06_s05] wrote: {out_model}")

    print("\n[phase06_s05] Step 05 complete.")


if __name__ == "__main__":
    main()
