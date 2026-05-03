"""
Phase 06 - Step 03: Linear baseline (Ridge regression).

Purpose:
    Fit a Ridge regression baseline on panel_ml.csv. This serves as
    the linear comparison anchor for Phase 06's tree models and
    contributes to the Phase 05 vs Phase 06 ranking comparison in
    Step 07.

    Per Decision 5 (Ridge policy):
    - 12 numeric features -> SimpleImputer(median) + StandardScaler
    - cluster_kmeans_k3 -> 4 one-hot dummies (0/1/2/unclustered)

    Per Decision 4: alphas = np.logspace(-3, 3, 13), CV =
    TimeSeriesSplit(n_splits=5).

    Coefficients are reported on the original (un-standardised)
    feature scale to enable direct magnitude comparison with Phase
    05 RE Spec A coefficients in Step 07.

    Per Convention 6.7: random KFold metrics are reported alongside
    temporal-split metrics as a leakage diagnostic.

Inputs:
    data/processed/panel_ml.csv

Outputs:
    outputs/tables/phase06_s03_linear_baseline.csv
    outputs/tables/phase06_s03_predictions.csv
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import KFold, TimeSeriesSplit, GridSearchCV
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

# Make src importable.
PROJECT_ROOT_HINT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT_HINT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT_HINT))

from src.paths import find_project_root
from src.io_utils import read_csv_with_encoding_fallback

SEED = 42
np.random.seed(SEED)

# Feature definitions (must match Step 02 layout).
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

# Per Decision 4: pre-registered alpha grid.
ALPHA_GRID = np.logspace(-3, 3, 13)
N_SPLITS = 5


def encode_cluster_dummies(series: pd.Series) -> pd.DataFrame:
    """Encode cluster_kmeans_k3 (0/1/2 or NaN) as 4 one-hot columns.

    Returns a DataFrame with columns:
      cluster_0, cluster_1, cluster_2, cluster_unclustered

    NaN values get a 1 in cluster_unclustered (everyone else is 0
    there). This explicitly separates Phase-04-excluded countries
    from any of the three clusters rather than letting them act as
    an implicit reference category.
    """
    out = pd.DataFrame(index=series.index)
    out["cluster_0"] = (series == 0).astype(int)
    out["cluster_1"] = (series == 1).astype(int)
    out["cluster_2"] = (series == 2).astype(int)
    out["cluster_unclustered"] = series.isna().astype(int)
    # Sanity: each row sums to exactly 1.
    row_sums = out.sum(axis=1)
    if not (row_sums == 1).all():
        bad = (row_sums != 1).sum()
        raise AssertionError(
            f"Cluster one-hot encoding error: {bad} rows do not sum to 1"
        )
    return out


def build_preprocessor() -> ColumnTransformer:
    """Numeric pipeline: median impute + standardise. Cluster dummies pass through."""
    numeric_pipeline = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler()),
        ]
    )
    cluster_cols = ["cluster_0", "cluster_1", "cluster_2", "cluster_unclustered"]
    preprocessor = ColumnTransformer(
        transformers=[
            ("num", numeric_pipeline, NUMERIC_FEATURES),
            ("cluster", "passthrough", cluster_cols),
        ],
        remainder="drop",
    )
    return preprocessor


def report_metrics(y_true: np.ndarray, y_pred: np.ndarray, label: str) -> dict:
    """Compute RMSE, MAE, R^2 for one (y_true, y_pred) pair."""
    rmse = float(np.sqrt(mean_squared_error(y_true, y_pred)))
    mae = float(mean_absolute_error(y_true, y_pred))
    r2 = float(r2_score(y_true, y_pred))
    print(f"  {label:<8} RMSE={rmse:.3f}  MAE={mae:.3f}  R2={r2:.3f}  N={len(y_true):,}")
    return {"split": label, "rmse": rmse, "mae": mae, "r2": r2, "n": len(y_true)}


def main() -> None:
    project_root = find_project_root()
    in_path = project_root / "data" / "processed" / "panel_ml.csv"
    tables_dir = project_root / "outputs" / "tables"
    tables_dir.mkdir(parents=True, exist_ok=True)
    out_metrics = tables_dir / "phase06_s03_linear_baseline.csv"
    out_preds = tables_dir / "phase06_s03_predictions.csv"

    print(f"[phase06_s03] project root: {project_root}")
    print(f"[phase06_s03] input:        {in_path}")
    print(f"[phase06_s03] alpha grid:   {len(ALPHA_GRID)} values "
          f"in [{ALPHA_GRID.min():.0e}, {ALPHA_GRID.max():.0e}]")

    # ---- Load ----
    df, enc = read_csv_with_encoding_fallback(in_path)
    print(f"[phase06_s03] loaded:       {df.shape[0]:,} rows x "
          f"{df.shape[1]} cols (encoding={enc})")

    # ---- Build feature matrix ----
    cluster_dummies = encode_cluster_dummies(df[CLUSTER_FEATURE])
    X_full = pd.concat([df[NUMERIC_FEATURES], cluster_dummies], axis=1)
    y_full = df[TARGET].astype(float)
    print(f"[phase06_s03] feature matrix: {X_full.shape}")
    print(f"[phase06_s03] cluster dummies sum check (each row = 1): OK")

    # ---- Train / test split ----
    train_mask = df["split"].values == "train"
    test_mask = df["split"].values == "test"

    X_train = X_full.loc[train_mask].reset_index(drop=True)
    y_train = y_full.loc[train_mask].reset_index(drop=True)
    X_test = X_full.loc[test_mask].reset_index(drop=True)
    y_test = y_full.loc[test_mask].reset_index(drop=True)
    print(f"[phase06_s03] train: {len(X_train):,}, test: {len(X_test):,}")

    # ---- Hyperparameter search: TimeSeriesSplit + GridSearchCV ----
    # Sort training rows by year so TimeSeriesSplit creates temporally-
    # contiguous folds rather than splitting on country-year row order.
    train_meta = df.loc[train_mask, ["iso3", "year"]].reset_index(drop=True)
    sort_idx = train_meta["year"].argsort(kind="mergesort").values
    X_train_sorted = X_train.iloc[sort_idx].reset_index(drop=True)
    y_train_sorted = y_train.iloc[sort_idx].reset_index(drop=True)

    pipeline = Pipeline(
        steps=[
            ("preprocessor", build_preprocessor()),
            ("ridge", Ridge(random_state=SEED)),
        ]
    )

    param_grid = {"ridge__alpha": ALPHA_GRID}
    tss = TimeSeriesSplit(n_splits=N_SPLITS)

    print(f"\n[phase06_s03] tuning Ridge alpha via TimeSeriesSplit "
          f"({N_SPLITS} folds), grid size {len(ALPHA_GRID)}")
    gs = GridSearchCV(
        estimator=pipeline,
        param_grid=param_grid,
        scoring="neg_root_mean_squared_error",
        cv=tss,
        n_jobs=1,  # deterministic and CMD-friendly; problem size is tiny
        refit=True,
    )
    gs.fit(X_train_sorted, y_train_sorted)

    best_alpha = gs.best_params_["ridge__alpha"]
    best_cv_rmse = -gs.best_score_
    print(f"[phase06_s03] best alpha:   {best_alpha:.4g}")
    print(f"[phase06_s03] best CV RMSE: {best_cv_rmse:.3f}")

    # Boundary check (Convention 6.3 trigger): warn if best alpha is at
    # the grid edge.
    if best_alpha == ALPHA_GRID.min() or best_alpha == ALPHA_GRID.max():
        print(f"[phase06_s03] WARNING: best alpha at grid boundary; "
              f"consider expanding grid in a Step 03b override.")

    # ---- Refit best model on full (unsorted) train, evaluate ----
    best_model = gs.best_estimator_
    # GridSearchCV.refit refits on X_train_sorted, which is the same
    # data as X_train just permuted; the fitted Ridge is identical.

    print(f"\n[phase06_s03] performance:")
    train_pred = best_model.predict(X_train)
    test_pred = best_model.predict(X_test)
    metrics_train = report_metrics(y_train.values, train_pred, "train")
    metrics_test = report_metrics(y_test.values, test_pred, "test")

    # ---- Random KFold diagnostic (Convention 6.7) ----
    print(f"\n[phase06_s03] random KFold diagnostic (n_splits={N_SPLITS}):")
    rkf = KFold(n_splits=N_SPLITS, shuffle=True, random_state=SEED)
    rkf_pipeline = Pipeline(
        steps=[
            ("preprocessor", build_preprocessor()),
            ("ridge", Ridge(alpha=best_alpha, random_state=SEED)),
        ]
    )
    rkf_rmses = []
    for fold_idx, (tr, va) in enumerate(rkf.split(X_train)):
        rkf_pipeline.fit(X_train.iloc[tr], y_train.iloc[tr])
        pred_va = rkf_pipeline.predict(X_train.iloc[va])
        rmse_va = float(np.sqrt(mean_squared_error(y_train.iloc[va], pred_va)))
        rkf_rmses.append(rmse_va)
        print(f"  fold {fold_idx + 1}: RMSE={rmse_va:.3f}")
    rkf_mean = float(np.mean(rkf_rmses))
    rkf_std = float(np.std(rkf_rmses, ddof=1))
    print(f"  mean: {rkf_mean:.3f}  (sd {rkf_std:.3f})")
    print(f"  vs TimeSeriesSplit best CV RMSE: {best_cv_rmse:.3f}")
    leakage_gap = best_cv_rmse - rkf_mean
    print(f"  gap (TSS - RKF): {leakage_gap:+.3f}  "
          f"(positive gap = TSS harder = expected on panel data)")

    # ---- Coefficients on original (un-standardised) scale ----
    # The fitted pipeline applies StandardScaler with mean/scale from
    # the training data. To recover coefficients in original units, we
    # divide each numeric coefficient by its scale_. Cluster dummies
    # are pass-through, so their coefficients stay as-is.
    fitted_pre = best_model.named_steps["preprocessor"]
    fitted_ridge = best_model.named_steps["ridge"]

    # Order of features in the pipeline output:
    cluster_cols = ["cluster_0", "cluster_1", "cluster_2", "cluster_unclustered"]
    pipeline_feature_order = NUMERIC_FEATURES + cluster_cols
    raw_coefs_std = fitted_ridge.coef_  # standardised-scale for numeric

    scaler = fitted_pre.named_transformers_["num"].named_steps["scaler"]
    scales = scaler.scale_  # length = len(NUMERIC_FEATURES)
    means = scaler.mean_

    n_numeric = len(NUMERIC_FEATURES)
    coefs_orig = np.empty_like(raw_coefs_std)
    coefs_orig[:n_numeric] = raw_coefs_std[:n_numeric] / scales
    coefs_orig[n_numeric:] = raw_coefs_std[n_numeric:]  # cluster dummies pass through

    # Intercept on original scale: original intercept absorbs the
    # standardisation shift.
    intercept_std = fitted_ridge.intercept_
    intercept_orig = float(
        intercept_std - np.sum(raw_coefs_std[:n_numeric] * means / scales)
    )

    coef_df = pd.DataFrame({
        "feature": pipeline_feature_order,
        "coef_original_scale": coefs_orig,
        "coef_standardised_scale": raw_coefs_std,
    })
    coef_df["abs_coef_original"] = coef_df["coef_original_scale"].abs()
    coef_df["abs_coef_standardised"] = coef_df["coef_standardised_scale"].abs()
    coef_df = coef_df.sort_values(
        "abs_coef_standardised", ascending=False
    ).reset_index(drop=True)
    coef_df.insert(0, "rank_by_abs_standardised",
                   range(1, len(coef_df) + 1))

    print(f"\n[phase06_s03] Ridge coefficients (sorted by |coef| on "
          f"standardised scale):")
    for _, row in coef_df.iterrows():
        print(f"  {row['rank_by_abs_standardised']:>2}. "
              f"{row['feature']:<28} "
              f"orig={row['coef_original_scale']:>+8.4f}  "
              f"std={row['coef_standardised_scale']:>+8.4f}")

    # Direction sanity: mys coefficient should be negative.
    mys_coef = float(coef_df.loc[
        coef_df["feature"] == "mean_years_schooling",
        "coef_original_scale"
    ].values[0])
    print(f"\n[phase06_s03] mean_years_schooling raw-scale coefficient: "
          f"{mys_coef:+.4f}")
    if mys_coef < 0:
        print(f"  direction: negative (consistent with Phase 05 RE Spec A "
              f"-0.69)")
    else:
        print(f"  WARNING: positive direction conflicts with Phase 05 "
              f"RE Spec A -0.69")

    # ---- Build long-format metrics CSV ----
    metrics_rows = []
    metrics_rows.append({
        "model": "ridge", "metric": "best_alpha", "value": best_alpha,
        "split": None, "n": None,
    })
    metrics_rows.append({
        "model": "ridge", "metric": "intercept_original_scale",
        "value": intercept_orig, "split": None, "n": None,
    })
    metrics_rows.append({
        "model": "ridge", "metric": "tss_cv_rmse_best",
        "value": best_cv_rmse, "split": "tss_cv", "n": len(X_train),
    })
    metrics_rows.append({
        "model": "ridge", "metric": "rkf_cv_rmse_mean",
        "value": rkf_mean, "split": "rkf_cv", "n": len(X_train),
    })
    metrics_rows.append({
        "model": "ridge", "metric": "rkf_cv_rmse_std",
        "value": rkf_std, "split": "rkf_cv", "n": len(X_train),
    })
    for m in (metrics_train, metrics_test):
        for key in ("rmse", "mae", "r2"):
            metrics_rows.append({
                "model": "ridge", "metric": key,
                "value": m[key], "split": m["split"], "n": m["n"],
            })

    metrics_df = pd.DataFrame(metrics_rows)
    # Append per-feature coefficients in the same long-format file for
    # convenience (one source of truth for Step 07 ingestion).
    coef_long = coef_df[[
        "feature", "coef_original_scale", "coef_standardised_scale",
        "rank_by_abs_standardised",
    ]].rename(columns={"feature": "metric"}).copy()
    coef_long["model"] = "ridge"
    coef_long["split"] = None
    coef_long["n"] = None
    coef_long = coef_long.rename(columns={
        "coef_original_scale": "value",
    })
    coef_long["metric"] = "coef_" + coef_long["metric"].astype(str)
    coef_long["coef_standardised_scale"] = coef_long[
        "coef_standardised_scale"
    ].astype(float)
    metrics_df = pd.concat([
        metrics_df,
        coef_long[[
            "model", "metric", "value", "split", "n",
            "coef_standardised_scale", "rank_by_abs_standardised",
        ]],
    ], ignore_index=True)

    metrics_df.to_csv(out_metrics, index=False, encoding="utf-8")
    print(f"\n[phase06_s03] wrote: {out_metrics}")
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
        sub["model"] = "ridge"
        preds_rows.append(sub)

    preds_df = pd.concat(preds_rows, ignore_index=True)
    preds_df = preds_df[[
        "model", "iso3", "year", "split", "y_true", "y_pred",
        "residual", CLUSTER_FEATURE,
    ]]
    preds_df.to_csv(out_preds, index=False, encoding="utf-8")
    print(f"[phase06_s03] wrote: {out_preds}")
    print(f"  rows: {len(preds_df):,}")

    print("\n[phase06_s03] Step 03 complete.")


if __name__ == "__main__":
    main()
