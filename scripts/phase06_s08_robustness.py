"""
Phase 06 - Step 08: Robustness checks.

Purpose:
    Two pre-registered robustness checks per kickoff Step 08:

    (a) Feature-set sensitivity: re-fit RF and XGBoost on the
        Spec-A-only 5-feature set (mys, enrol_secondary,
        log_gdp_per_capita_ppp, log_population,
        urban_population_pct), keeping the same temporal split.
        Compare test metrics and mys mean |SHAP| ranking against the
        13-feature baseline (Steps 04/05/06). If headline findings
        survive, the 13-feature expansion was decorative; if they
        change qualitatively, the extension matters substantively.

    (b) Boundary-case country holdout: drop all rows of BRA, ZAF,
        MEX, ARG (Phase 04 K-means/Ward boundary cases between
        Cluster 1 and Cluster 2; Phase 05 boundary-reassignment
        anchors) from training, re-fit RF and XGBoost on the
        13-feature spec using the Step 04/05 best hyperparameters
        (no re-tuning - pre-registration discipline 6.5), and
        report per-country test metrics and mys SHAP attributions.

    Per Convention 6.7: a single number is not robust; convergence
    across feature-set and country-holdout perturbations is.

Inputs:
    data/processed/panel_ml.csv
    outputs/models/phase06_s04_rf.joblib   (read for best params only)
    outputs/models/phase06_s05_xgb.joblib  (read for best params only)
    outputs/tables/phase06_s06_shap_global.csv

Outputs:
    outputs/tables/phase06_s08_robustness.csv
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import shap

from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestRegressor
from sklearn.impute import SimpleImputer
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import RandomizedSearchCV, TimeSeriesSplit
from sklearn.pipeline import Pipeline

import xgboost as xgb

PROJECT_ROOT_HINT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT_HINT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT_HINT))

from src.paths import find_project_root
from src.io_utils import read_csv_with_encoding_fallback

SEED = 42
np.random.seed(SEED)

NUMERIC_FEATURES_FULL = [
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
SPEC_A_NUMERIC = NUMERIC_FEATURES_FULL[:5]  # first 5 are Spec A
CLUSTER_FEATURE = "cluster_kmeans_k3"
TARGET = "gini"

# Boundary-case anchor countries (Phase 04 §Known Issues, Phase 05 robustness anchors).
BOUNDARY_COUNTRIES = ("BRA", "ZAF", "MEX", "ARG")

# Same pre-registered grids as Steps 04/05.
RF_PARAM_GRID = {
    "rf__n_estimators": [200, 500, 1000],
    "rf__max_depth": [None, 5, 10, 20],
    "rf__min_samples_split": [2, 5, 10],
    "rf__min_samples_leaf": [1, 2, 5],
    "rf__max_features": ["sqrt", "log2", 0.5, 1.0],
}
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


# ---------------------------------------------------------------------------
# Feature-matrix builders
# ---------------------------------------------------------------------------
def build_xgb_X(df: pd.DataFrame, numeric_features: list, include_cluster: bool) -> pd.DataFrame:
    cols = list(numeric_features)
    if include_cluster:
        cols = cols + [CLUSTER_FEATURE]
    X = df[cols].copy().astype(float)
    return X


def build_rf_X(df: pd.DataFrame, numeric_features: list, include_cluster: bool) -> pd.DataFrame:
    X = df[list(numeric_features)].copy()
    if include_cluster:
        X[CLUSTER_FEATURE] = df[CLUSTER_FEATURE].fillna(-1).astype(int)
    return X


def build_rf_pipeline(numeric_features: list, include_cluster: bool, **rf_kwargs) -> Pipeline:
    transformers = [("num", SimpleImputer(strategy="median"), list(numeric_features))]
    if include_cluster:
        transformers.append(("cluster", "passthrough", [CLUSTER_FEATURE]))
    pre = ColumnTransformer(transformers=transformers, remainder="drop")
    return Pipeline(steps=[
        ("preprocessor", pre),
        ("rf", RandomForestRegressor(random_state=SEED, n_jobs=1, **rf_kwargs)),
    ])


# ---------------------------------------------------------------------------
# Metrics helper
# ---------------------------------------------------------------------------
def metrics_dict(y_true, y_pred) -> dict:
    return {
        "rmse": float(np.sqrt(mean_squared_error(y_true, y_pred))),
        "mae": float(mean_absolute_error(y_true, y_pred)),
        "r2": float(r2_score(y_true, y_pred)) if len(y_true) > 1 else float("nan"),
        "n": int(len(y_true)),
    }


def report_metrics(label: str, m: dict) -> None:
    print(f"  {label:<32} RMSE={m['rmse']:.3f}  MAE={m['mae']:.3f}  "
          f"R2={m['r2']:.3f}  N={m['n']}")


# ---------------------------------------------------------------------------
# Check (a) - Spec-A-only refit
# ---------------------------------------------------------------------------
def run_spec_a_check(df: pd.DataFrame, baseline_global_df: pd.DataFrame) -> list:
    print("\n" + "=" * 70)
    print("[phase06_s08] Check (a): Spec-A-only feature-set sensitivity")
    print("=" * 70)
    print(f"  features: {SPEC_A_NUMERIC}")

    train_mask = df["split"].values == "train"
    test_mask = df["split"].values == "test"
    df_train = df.loc[train_mask].reset_index(drop=True)
    df_test = df.loc[test_mask].reset_index(drop=True)
    y_train = df_train[TARGET].astype(float).values
    y_test = df_test[TARGET].astype(float).values

    # Sort train by year for TimeSeriesSplit
    sort_idx = df_train["year"].argsort(kind="mergesort").values

    rows = []

    # ----- Spec-A RF -----
    print("\n  fitting RF (Spec A, 5 features)...")
    X_train_rf = build_rf_X(df_train, SPEC_A_NUMERIC, include_cluster=False)
    X_test_rf = build_rf_X(df_test, SPEC_A_NUMERIC, include_cluster=False)
    pipe = Pipeline(steps=[
        ("preprocessor", ColumnTransformer(
            transformers=[("num", SimpleImputer(strategy="median"), SPEC_A_NUMERIC)],
            remainder="drop",
        )),
        ("rf", RandomForestRegressor(random_state=SEED, n_jobs=1)),
    ])
    tss = TimeSeriesSplit(n_splits=N_SPLITS)
    t0 = time.time()
    rs = RandomizedSearchCV(
        pipe, RF_PARAM_GRID, n_iter=N_ITER,
        scoring="neg_root_mean_squared_error", cv=tss,
        n_jobs=-1, refit=True, random_state=SEED, verbose=0,
    )
    rs.fit(X_train_rf.iloc[sort_idx].reset_index(drop=True),
           pd.Series(y_train).iloc[sort_idx].reset_index(drop=True))
    print(f"  RF search: {time.time() - t0:.1f}s, best CV RMSE={-rs.best_score_:.3f}")
    rf_model = rs.best_estimator_
    test_pred_rf = rf_model.predict(X_test_rf)
    m_rf = metrics_dict(y_test, test_pred_rf)
    report_metrics("RF Spec-A test", m_rf)

    # SHAP global on RF Spec A
    pre = rf_model.named_steps["preprocessor"]
    rf_inner = rf_model.named_steps["rf"]
    X_test_transformed = pre.transform(X_test_rf)
    explainer = shap.TreeExplainer(rf_inner)
    shap_rf_a = explainer.shap_values(X_test_transformed)
    mean_abs_shap_rf = np.abs(shap_rf_a).mean(axis=0)
    mean_signed_shap_rf = shap_rf_a.mean(axis=0)
    shap_rank_rf = (
        pd.DataFrame({"feature": SPEC_A_NUMERIC, "mean_abs_shap": mean_abs_shap_rf})
        .sort_values("mean_abs_shap", ascending=False)
        .reset_index(drop=True)
    )
    shap_rank_rf["rank"] = range(1, len(shap_rank_rf) + 1)
    print(f"  RF Spec-A SHAP ranking:")
    for _, r in shap_rank_rf.iterrows():
        print(f"    {r['rank']}. {r['feature']:<28} mean|SHAP|={r['mean_abs_shap']:.4f}")

    # ----- Spec-A XGBoost -----
    print("\n  fitting XGBoost (Spec A, 5 features)...")
    X_train_xgb = build_xgb_X(df_train, SPEC_A_NUMERIC, include_cluster=False)
    X_test_xgb = build_xgb_X(df_test, SPEC_A_NUMERIC, include_cluster=False)
    base_xgb = xgb.XGBRegressor(
        random_state=SEED, objective="reg:squarederror",
        tree_method="hist", n_jobs=1, verbosity=0,
    )
    t0 = time.time()
    rs_xgb = RandomizedSearchCV(
        base_xgb, XGB_PARAM_GRID, n_iter=N_ITER,
        scoring="neg_root_mean_squared_error", cv=tss,
        n_jobs=-1, refit=True, random_state=SEED, verbose=0,
    )
    rs_xgb.fit(X_train_xgb.iloc[sort_idx].reset_index(drop=True),
               pd.Series(y_train).iloc[sort_idx].reset_index(drop=True))
    print(f"  XGB search: {time.time() - t0:.1f}s, best CV RMSE={-rs_xgb.best_score_:.3f}")
    xgb_model_a = rs_xgb.best_estimator_
    test_pred_xgb = xgb_model_a.predict(X_test_xgb)
    m_xgb = metrics_dict(y_test, test_pred_xgb)
    report_metrics("XGB Spec-A test", m_xgb)

    explainer_xgb = shap.TreeExplainer(xgb_model_a)
    shap_xgb_a = explainer_xgb.shap_values(X_test_xgb)
    mean_abs_shap_xgb = np.abs(shap_xgb_a).mean(axis=0)
    mean_signed_shap_xgb = shap_xgb_a.mean(axis=0)
    shap_rank_xgb = (
        pd.DataFrame({"feature": SPEC_A_NUMERIC, "mean_abs_shap": mean_abs_shap_xgb})
        .sort_values("mean_abs_shap", ascending=False)
        .reset_index(drop=True)
    )
    shap_rank_xgb["rank"] = range(1, len(shap_rank_xgb) + 1)
    print(f"  XGB Spec-A SHAP ranking:")
    for _, r in shap_rank_xgb.iterrows():
        print(f"    {r['rank']}. {r['feature']:<28} mean|SHAP|={r['mean_abs_shap']:.4f}")

    # ----- Comparison vs baseline (13-feature) -----
    print("\n  comparison vs Step 06 baseline (13-feature spec):")
    for model_name, m_a in [("rf", m_rf), ("xgb", m_xgb)]:
        baseline_sub = baseline_global_df[
            baseline_global_df["model"] == model_name
        ].copy().sort_values("mean_abs_shap", ascending=False).reset_index(drop=True)
        # mys rank in baseline (always 1, but verify)
        baseline_mys_rank = int(
            baseline_sub[baseline_sub["feature"] == "mean_years_schooling"].index[0] + 1
        )
        baseline_mys_shap = float(
            baseline_sub[baseline_sub["feature"] == "mean_years_schooling"]["mean_abs_shap"].values[0]
        )
        spec_a_rank = shap_rank_rf if model_name == "rf" else shap_rank_xgb
        a_mys_rank = int(spec_a_rank[spec_a_rank["feature"] == "mean_years_schooling"]["rank"].values[0])
        a_mys_shap = float(spec_a_rank[spec_a_rank["feature"] == "mean_years_schooling"]["mean_abs_shap"].values[0])
        print(f"    {model_name}: mys rank baseline=#{baseline_mys_rank} (|SHAP|={baseline_mys_shap:.4f})  "
              f"-> Spec-A=#{a_mys_rank} (|SHAP|={a_mys_shap:.4f})")

    # ----- Build CSV rows -----
    for model_name, m, shap_rank, shap_signed in [
        ("rf", m_rf, shap_rank_rf, mean_signed_shap_rf),
        ("xgb", m_xgb, shap_rank_xgb, mean_signed_shap_xgb),
    ]:
        for k, v in m.items():
            rows.append({
                "check": "feature_set",
                "subgroup": "spec_a_5_features",
                "model": model_name,
                "metric": f"test_{k}",
                "feature": None,
                "value": v,
            })
        # Per-feature SHAP
        signed_lookup = dict(zip(SPEC_A_NUMERIC, shap_signed))
        for _, r in shap_rank.iterrows():
            rows.append({
                "check": "feature_set",
                "subgroup": "spec_a_5_features",
                "model": model_name,
                "metric": "mean_abs_shap",
                "feature": r["feature"],
                "value": float(r["mean_abs_shap"]),
            })
            rows.append({
                "check": "feature_set",
                "subgroup": "spec_a_5_features",
                "model": model_name,
                "metric": "mean_signed_shap",
                "feature": r["feature"],
                "value": float(signed_lookup[r["feature"]]),
            })
            rows.append({
                "check": "feature_set",
                "subgroup": "spec_a_5_features",
                "model": model_name,
                "metric": "shap_rank",
                "feature": r["feature"],
                "value": float(r["rank"]),
            })
    return rows


# ---------------------------------------------------------------------------
# Check (b) - Boundary-case country holdout
# ---------------------------------------------------------------------------
def extract_best_rf_params(rf_model_loaded) -> dict:
    fitted_rf = rf_model_loaded.named_steps["rf"]
    keys = ("n_estimators", "max_depth", "min_samples_split",
            "min_samples_leaf", "max_features")
    return {k: getattr(fitted_rf, k) for k in keys}


def extract_best_xgb_params(xgb_model_loaded) -> dict:
    keys = ("n_estimators", "max_depth", "learning_rate",
            "subsample", "colsample_bytree", "reg_lambda", "reg_alpha")
    p = xgb_model_loaded.get_params()
    return {k: p[k] for k in keys}


def run_boundary_holdout_check(
    df: pd.DataFrame, rf_best_params: dict, xgb_best_params: dict,
) -> list:
    print("\n" + "=" * 70)
    print("[phase06_s08] Check (b): Boundary-case country holdout")
    print("=" * 70)
    print(f"  holdout countries: {BOUNDARY_COUNTRIES}")
    print(f"  using Step 04/05 best hyperparameters (no re-tuning)")

    # Holdout = ALL rows of these 4 countries (whether train or test).
    holdout_mask = df["iso3"].isin(BOUNDARY_COUNTRIES).values
    fit_mask = ~holdout_mask
    df_fit = df.loc[fit_mask].reset_index(drop=True)
    df_holdout = df.loc[holdout_mask].reset_index(drop=True)

    n_fit = len(df_fit)
    n_holdout = len(df_holdout)
    n_holdout_per_country = (
        df_holdout.groupby("iso3").size().to_dict()
    )
    print(f"  fit rows:     {n_fit}")
    print(f"  holdout rows: {n_holdout}  ({n_holdout_per_country})")

    y_fit = df_fit[TARGET].astype(float).values
    y_holdout = df_holdout[TARGET].astype(float).values

    rows = []

    # ----- RF -----
    print("\n  fitting RF on fit set with Step 04 best params...")
    rf_pipeline = build_rf_pipeline(
        NUMERIC_FEATURES_FULL, include_cluster=True, **rf_best_params,
    )
    X_fit_rf = build_rf_X(df_fit, NUMERIC_FEATURES_FULL, include_cluster=True)
    X_holdout_rf = build_rf_X(df_holdout, NUMERIC_FEATURES_FULL, include_cluster=True)
    rf_pipeline.fit(X_fit_rf, y_fit)
    pred_rf = rf_pipeline.predict(X_holdout_rf)
    m_rf_holdout = metrics_dict(y_holdout, pred_rf)
    report_metrics("RF holdout (4 countries)", m_rf_holdout)

    # SHAP for the holdout rows
    pre = rf_pipeline.named_steps["preprocessor"]
    rf_inner = rf_pipeline.named_steps["rf"]
    X_holdout_transformed = pre.transform(X_holdout_rf)
    explainer_rf = shap.TreeExplainer(rf_inner)
    shap_rf_holdout = explainer_rf.shap_values(X_holdout_transformed)
    mys_idx = NUMERIC_FEATURES_FULL.index("mean_years_schooling")  # 0
    mys_shap_rf = shap_rf_holdout[:, mys_idx]

    # ----- XGBoost -----
    print("\n  fitting XGBoost on fit set with Step 05 best params...")
    xgb_model_holdout = xgb.XGBRegressor(
        random_state=SEED, objective="reg:squarederror",
        tree_method="hist", n_jobs=-1, verbosity=0,
        **xgb_best_params,
    )
    X_fit_xgb = build_xgb_X(df_fit, NUMERIC_FEATURES_FULL, include_cluster=True)
    X_holdout_xgb = build_xgb_X(df_holdout, NUMERIC_FEATURES_FULL, include_cluster=True)
    xgb_model_holdout.fit(X_fit_xgb, y_fit)
    pred_xgb = xgb_model_holdout.predict(X_holdout_xgb)
    m_xgb_holdout = metrics_dict(y_holdout, pred_xgb)
    report_metrics("XGB holdout (4 countries)", m_xgb_holdout)

    explainer_xgb = shap.TreeExplainer(xgb_model_holdout)
    shap_xgb_holdout = explainer_xgb.shap_values(X_holdout_xgb)
    mys_shap_xgb = shap_xgb_holdout[:, mys_idx]

    # ----- Per-country breakdown -----
    print("\n  per-country breakdown:")
    print(f"  {'country':<7} {'n':>3}  "
          f"{'RF RMSE':>8} {'XGB RMSE':>8}  "
          f"{'mys mean SHAP RF':>17} {'mys mean SHAP XGB':>17}")
    for c in BOUNDARY_COUNTRIES:
        idx = (df_holdout["iso3"].values == c)
        n_c = int(idx.sum())
        if n_c == 0:
            print(f"  {c:<7} {n_c:>3}  (no rows)")
            continue
        m_rf_c = metrics_dict(y_holdout[idx], pred_rf[idx])
        m_xgb_c = metrics_dict(y_holdout[idx], pred_xgb[idx])
        mys_rf_c = float(mys_shap_rf[idx].mean())
        mys_xgb_c = float(mys_shap_xgb[idx].mean())
        print(f"  {c:<7} {n_c:>3}  {m_rf_c['rmse']:>8.3f} {m_xgb_c['rmse']:>8.3f}  "
              f"{mys_rf_c:>+17.4f} {mys_xgb_c:>+17.4f}")
        for k, v in m_rf_c.items():
            rows.append({
                "check": "boundary_holdout", "subgroup": c, "model": "rf",
                "metric": f"test_{k}", "feature": None, "value": v,
            })
        for k, v in m_xgb_c.items():
            rows.append({
                "check": "boundary_holdout", "subgroup": c, "model": "xgb",
                "metric": f"test_{k}", "feature": None, "value": v,
            })
        rows.append({
            "check": "boundary_holdout", "subgroup": c, "model": "rf",
            "metric": "mys_mean_signed_shap", "feature": "mean_years_schooling",
            "value": mys_rf_c,
        })
        rows.append({
            "check": "boundary_holdout", "subgroup": c, "model": "xgb",
            "metric": "mys_mean_signed_shap", "feature": "mean_years_schooling",
            "value": mys_xgb_c,
        })

    # Aggregate across all 4 countries
    for k, v in m_rf_holdout.items():
        rows.append({
            "check": "boundary_holdout", "subgroup": "all_4_countries",
            "model": "rf", "metric": f"test_{k}", "feature": None, "value": v,
        })
    for k, v in m_xgb_holdout.items():
        rows.append({
            "check": "boundary_holdout", "subgroup": "all_4_countries",
            "model": "xgb", "metric": f"test_{k}", "feature": None, "value": v,
        })
    rows.append({
        "check": "boundary_holdout", "subgroup": "all_4_countries", "model": "rf",
        "metric": "mys_mean_signed_shap", "feature": "mean_years_schooling",
        "value": float(mys_shap_rf.mean()),
    })
    rows.append({
        "check": "boundary_holdout", "subgroup": "all_4_countries", "model": "xgb",
        "metric": "mys_mean_signed_shap", "feature": "mean_years_schooling",
        "value": float(mys_shap_xgb.mean()),
    })

    return rows


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    project_root = find_project_root()
    in_path = project_root / "data" / "processed" / "panel_ml.csv"
    rf_baseline_path = project_root / "outputs" / "models" / "phase06_s04_rf.joblib"
    xgb_baseline_path = project_root / "outputs" / "models" / "phase06_s05_xgb.joblib"
    baseline_global_path = (
        project_root / "outputs" / "tables" / "phase06_s06_shap_global.csv"
    )
    out_csv = project_root / "outputs" / "tables" / "phase06_s08_robustness.csv"

    print(f"[phase06_s08] project root: {project_root}")
    print(f"[phase06_s08] input:        {in_path}")
    print(f"[phase06_s08] rf baseline:  {rf_baseline_path}")
    print(f"[phase06_s08] xgb baseline: {xgb_baseline_path}")

    # Load
    df, _ = read_csv_with_encoding_fallback(in_path)
    print(f"[phase06_s08] loaded:       {df.shape[0]:,} rows x {df.shape[1]} cols")

    rf_baseline = joblib.load(rf_baseline_path)
    xgb_baseline = joblib.load(xgb_baseline_path)
    rf_best_params = extract_best_rf_params(rf_baseline)
    xgb_best_params = extract_best_xgb_params(xgb_baseline)
    print(f"[phase06_s08] rf best params:  {rf_best_params}")
    print(f"[phase06_s08] xgb best params: {xgb_best_params}")

    baseline_global_df, _ = read_csv_with_encoding_fallback(baseline_global_path)
    print(f"[phase06_s08] baseline global SHAP rows: {len(baseline_global_df)}")

    # Run checks
    rows_a = run_spec_a_check(df, baseline_global_df)
    rows_b = run_boundary_holdout_check(df, rf_best_params, xgb_best_params)

    # Write CSV
    out_df = pd.DataFrame(rows_a + rows_b)
    out_df.to_csv(out_csv, index=False, encoding="utf-8")
    print(f"\n[phase06_s08] wrote: {out_csv}  ({len(out_df)} rows)")

    print("\n[phase06_s08] Step 08 complete.")


if __name__ == "__main__":
    main()
