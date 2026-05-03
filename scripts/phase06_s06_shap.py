"""
Phase 06 - Step 06: SHAP attribution.

Purpose:
    Compute TreeSHAP attributions for the Phase 06 RF and XGBoost
    models. Aggregate at three granularities:
    - Global: mean(|SHAP|) per feature, ranked, plus signed mean
    - Per-cluster: same on test-set subsets where
      cluster_kmeans_k3 in {0, 1, 2}
    - Local: Brazil 2015 (XGBoost only; train block, Phase 04
      Cluster 1/2 boundary case, Phase 05 boundary-reassignment
      anchor)

    Plus dependence plots for the global top-3 numeric features for
    each model.

    Per Decision 6: SHAP interaction values are skipped; dependence
    plots with auto-coloured interacting features carry that role
    at lower interpretation cost.

    Per Convention 6.13: predictive performance is a means; SHAP
    rankings vs Phase 05 coefficient rankings is the deliverable
    (Step 07 consumes these CSVs).

    Per Convention 6.15: SHAP attribution is a feature-importance
    decomposition over predictions, not a causal estimate. Output
    CSVs and figure captions stay attribution-language only.

Inputs:
    data/processed/panel_ml.csv
    outputs/models/phase06_s04_rf.joblib
    outputs/models/phase06_s05_xgb.joblib

Outputs:
    outputs/tables/phase06_s06_shap_global.csv
    outputs/tables/phase06_s06_shap_per_cluster.csv
    outputs/tables/phase06_s06_shap_values_test_rf.csv
    outputs/tables/phase06_s06_shap_values_test_xgb.csv
    outputs/figures/phase06_s06_shap_summary_rf.png
    outputs/figures/phase06_s06_shap_summary_xgb.png
    outputs/figures/phase06_s06_dependence_top3_rf.png
    outputs/figures/phase06_s06_dependence_top3_xgb.png
    outputs/figures/phase06_s06_brazil2015_waterfall.png
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

import joblib
import matplotlib
matplotlib.use("Agg")  # no GUI on Windows runs
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import shap

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

# Brazil 2015 anchor: Phase 04 boundary case, Phase 05 robustness anchor.
LOCAL_ISO3 = "BRA"
LOCAL_YEAR = 2015

DPI = 300


# ---------------------------------------------------------------------------
# Data loading and feature-matrix builders
# ---------------------------------------------------------------------------
def build_xgb_X(df: pd.DataFrame) -> pd.DataFrame:
    """For XGBoost: keep NaN, cast everything to float."""
    X = df[ALL_FEATURES].copy().astype(float)
    return X


def build_rf_X_for_pipeline(df: pd.DataFrame) -> pd.DataFrame:
    """For RF pipeline: NaN preserved here; SimpleImputer inside the
    pipeline does median imputation; cluster column gets -1 sentinel
    via encode_cluster_sentinel."""
    X = df[NUMERIC_FEATURES].copy()
    X[CLUSTER_FEATURE] = df[CLUSTER_FEATURE].fillna(-1).astype(int)
    return X


# ---------------------------------------------------------------------------
# SHAP computation wrappers
# ---------------------------------------------------------------------------
def shap_for_xgb(xgb_model, X: pd.DataFrame) -> np.ndarray:
    """TreeSHAP on raw XGBoost model. Returns (n, n_features) array,
    feature order = X.columns."""
    explainer = shap.TreeExplainer(xgb_model)
    shap_values = explainer.shap_values(X)
    if shap_values.shape != (X.shape[0], X.shape[1]):
        raise AssertionError(
            f"XGB SHAP shape mismatch: got {shap_values.shape}, "
            f"expected {(X.shape[0], X.shape[1])}"
        )
    return shap_values, float(explainer.expected_value)


def shap_for_rf_pipeline(rf_pipeline, X_raw: pd.DataFrame) -> np.ndarray:
    """TreeSHAP on RF inside a sklearn Pipeline.

    The pipeline's preprocessor (ColumnTransformer with
    SimpleImputer + cluster passthrough) is applied first to get the
    actual array the RF sees, then TreeExplainer runs on the bare
    RandomForestRegressor.
    """
    preprocessor = rf_pipeline.named_steps["preprocessor"]
    rf_model = rf_pipeline.named_steps["rf"]

    X_transformed = preprocessor.transform(X_raw)
    # ColumnTransformer order: numeric first, then cluster
    explainer = shap.TreeExplainer(rf_model)
    shap_values = explainer.shap_values(X_transformed)
    if shap_values.shape != (X_raw.shape[0], len(ALL_FEATURES)):
        raise AssertionError(
            f"RF SHAP shape mismatch: got {shap_values.shape}, "
            f"expected {(X_raw.shape[0], len(ALL_FEATURES))}"
        )
    base_value = explainer.expected_value
    if hasattr(base_value, "__len__"):
        base_value = float(base_value[0])
    else:
        base_value = float(base_value)
    return shap_values, base_value, X_transformed


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------
def aggregate_global(
    shap_values: np.ndarray, model_name: str
) -> pd.DataFrame:
    """Return a DataFrame with one row per feature: mean |shap|, mean
    signed shap, rank by mean |shap|."""
    mean_abs = np.abs(shap_values).mean(axis=0)
    mean_signed = shap_values.mean(axis=0)
    df = pd.DataFrame({
        "model": model_name,
        "feature": ALL_FEATURES,
        "mean_abs_shap": mean_abs,
        "mean_signed_shap": mean_signed,
    })
    df = df.sort_values("mean_abs_shap", ascending=False).reset_index(drop=True)
    df.insert(0, "rank", range(1, len(df) + 1))
    return df


def aggregate_per_cluster(
    shap_values: np.ndarray,
    cluster_series: pd.Series,
    model_name: str,
) -> pd.DataFrame:
    """For each cluster in {0, 1, 2} (and 'all'), report
    mean |shap| and mean signed shap per feature."""
    rows = []
    # 'all' baseline
    n_all = shap_values.shape[0]
    for j, feat in enumerate(ALL_FEATURES):
        rows.append({
            "model": model_name,
            "cluster": "all",
            "n": n_all,
            "feature": feat,
            "mean_abs_shap": float(np.abs(shap_values[:, j]).mean()),
            "mean_signed_shap": float(shap_values[:, j].mean()),
        })
    # Per cluster
    for c in (0, 1, 2):
        idx = (cluster_series.values == c)
        n_c = int(idx.sum())
        if n_c == 0:
            continue
        for j, feat in enumerate(ALL_FEATURES):
            rows.append({
                "model": model_name,
                "cluster": str(c),
                "n": n_c,
                "feature": feat,
                "mean_abs_shap": float(np.abs(shap_values[idx, j]).mean()),
                "mean_signed_shap": float(shap_values[idx, j].mean()),
            })
    df = pd.DataFrame(rows)
    return df


# ---------------------------------------------------------------------------
# Plotting
# ---------------------------------------------------------------------------
def plot_summary(
    shap_values: np.ndarray,
    X_for_plot: pd.DataFrame,
    feature_names: list,
    out_path: Path,
    title: str,
) -> None:
    """SHAP beeswarm summary plot, matplotlib backend."""
    plt.figure(figsize=(10, 7))
    shap.summary_plot(
        shap_values,
        X_for_plot,
        feature_names=feature_names,
        show=False,
        plot_size=None,
    )
    plt.title(title, fontsize=12)
    plt.tight_layout()
    plt.savefig(out_path, dpi=DPI, bbox_inches="tight")
    plt.close("all")


def plot_dependence_top3(
    shap_values: np.ndarray,
    X_for_plot: pd.DataFrame,
    global_df: pd.DataFrame,
    out_path: Path,
    title: str,
) -> list:
    """3-panel SHAP dependence plot for the top-3 numeric features
    (excludes cluster_kmeans_k3 from the plotted set)."""
    numeric_only = global_df[
        global_df["feature"].isin(NUMERIC_FEATURES)
    ].sort_values("mean_abs_shap", ascending=False).reset_index(drop=True)
    top3 = numeric_only["feature"].head(3).tolist()

    fig, axes = plt.subplots(1, 3, figsize=(18, 5))
    for ax, feat in zip(axes, top3):
        # shap.dependence_plot uses pyplot's current axes
        plt.sca(ax)
        shap.dependence_plot(
            feat,
            shap_values,
            X_for_plot,
            feature_names=list(X_for_plot.columns),
            ax=ax,
            show=False,
        )
        ax.set_title(feat, fontsize=10)
    fig.suptitle(title, fontsize=12)
    plt.tight_layout()
    plt.savefig(out_path, dpi=DPI, bbox_inches="tight")
    plt.close("all")
    return top3


def plot_local_waterfall_xgb(
    xgb_model,
    X_local: pd.DataFrame,
    feature_names: list,
    out_path: Path,
    title: str,
) -> None:
    """Single-row waterfall plot for Brazil 2015 (or whatever X_local
    is)."""
    if X_local.shape[0] != 1:
        raise ValueError(
            f"plot_local_waterfall_xgb expects 1 row, got {X_local.shape[0]}"
        )
    explainer = shap.TreeExplainer(xgb_model)
    explanation = explainer(X_local)  # returns shap.Explanation
    plt.figure(figsize=(10, 7))
    shap.plots.waterfall(explanation[0], show=False, max_display=14)
    plt.title(title, fontsize=12)
    plt.tight_layout()
    plt.savefig(out_path, dpi=DPI, bbox_inches="tight")
    plt.close("all")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    project_root = find_project_root()

    in_path = project_root / "data" / "processed" / "panel_ml.csv"
    rf_model_path = project_root / "outputs" / "models" / "phase06_s04_rf.joblib"
    xgb_model_path = project_root / "outputs" / "models" / "phase06_s05_xgb.joblib"

    tables_dir = project_root / "outputs" / "tables"
    figures_dir = project_root / "outputs" / "figures"
    tables_dir.mkdir(parents=True, exist_ok=True)
    figures_dir.mkdir(parents=True, exist_ok=True)

    out_global = tables_dir / "phase06_s06_shap_global.csv"
    out_percluster = tables_dir / "phase06_s06_shap_per_cluster.csv"
    out_shap_rf = tables_dir / "phase06_s06_shap_values_test_rf.csv"
    out_shap_xgb = tables_dir / "phase06_s06_shap_values_test_xgb.csv"
    out_summary_rf = figures_dir / "phase06_s06_shap_summary_rf.png"
    out_summary_xgb = figures_dir / "phase06_s06_shap_summary_xgb.png"
    out_depend_rf = figures_dir / "phase06_s06_dependence_top3_rf.png"
    out_depend_xgb = figures_dir / "phase06_s06_dependence_top3_xgb.png"
    out_local = figures_dir / "phase06_s06_brazil2015_waterfall.png"

    print(f"[phase06_s06] project root: {project_root}")
    print(f"[phase06_s06] shap version: {shap.__version__}")
    print(f"[phase06_s06] input:        {in_path}")
    print(f"[phase06_s06] rf model:     {rf_model_path}")
    print(f"[phase06_s06] xgb model:    {xgb_model_path}")

    # ---- Load data ----
    df, enc = read_csv_with_encoding_fallback(in_path)
    print(f"[phase06_s06] loaded panel: {df.shape[0]:,} rows x "
          f"{df.shape[1]} cols (encoding={enc})")

    train_mask = df["split"].values == "train"
    test_mask = df["split"].values == "test"
    df_test = df.loc[test_mask].reset_index(drop=True)
    n_test = len(df_test)
    print(f"[phase06_s06] test rows:    {n_test}")

    # ---- Load models ----
    rf_pipeline = joblib.load(rf_model_path)
    xgb_model = joblib.load(xgb_model_path)
    print(f"[phase06_s06] models loaded")

    # ---- Build feature matrices ----
    X_test_for_rf = build_rf_X_for_pipeline(df_test)
    X_test_for_xgb = build_xgb_X(df_test)
    cluster_test = df_test[CLUSTER_FEATURE]  # NaN preserved here
    print(f"[phase06_s06] X_test for RF (post-sentinel): {X_test_for_rf.shape}")
    print(f"[phase06_s06] X_test for XGB (NaN preserved): {X_test_for_xgb.shape}")

    # =========================================================================
    # XGBoost SHAP
    # =========================================================================
    print(f"\n[phase06_s06] computing XGBoost TreeSHAP on test...")
    t0 = time.time()
    shap_xgb, base_xgb = shap_for_xgb(xgb_model, X_test_for_xgb)
    print(f"  done in {time.time() - t0:.1f}s, base value = {base_xgb:.3f}")

    # Sanity: sum of SHAP + base ~= prediction
    sum_check_xgb = shap_xgb.sum(axis=1) + base_xgb
    pred_check_xgb = xgb_model.predict(X_test_for_xgb)
    diff = float(np.abs(sum_check_xgb - pred_check_xgb).max())
    print(f"  XGB sum-check max abs diff vs predict(): {diff:.6f} (~0 = OK)")

    # =========================================================================
    # RF SHAP
    # =========================================================================
    print(f"\n[phase06_s06] computing RF TreeSHAP on test...")
    t0 = time.time()
    shap_rf, base_rf, X_test_rf_transformed = shap_for_rf_pipeline(
        rf_pipeline, X_test_for_rf
    )
    print(f"  done in {time.time() - t0:.1f}s, base value = {base_rf:.3f}")
    sum_check_rf = shap_rf.sum(axis=1) + base_rf
    pred_check_rf = rf_pipeline.predict(X_test_for_rf)
    diff_rf = float(np.abs(sum_check_rf - pred_check_rf).max())
    print(f"  RF sum-check max abs diff vs predict(): {diff_rf:.6f} (~0 = OK)")

    # =========================================================================
    # Global aggregation
    # =========================================================================
    print(f"\n[phase06_s06] global mean |SHAP|:")
    global_xgb = aggregate_global(shap_xgb, "xgb")
    global_rf = aggregate_global(shap_rf, "rf")
    global_df = pd.concat([global_rf, global_xgb], ignore_index=True)
    print(f"\n  XGBoost ranking:")
    for _, row in global_xgb.iterrows():
        print(f"    {row['rank']:>2}. {row['feature']:<28} "
              f"mean|SHAP|={row['mean_abs_shap']:>7.4f}  "
              f"signed={row['mean_signed_shap']:>+7.4f}")
    print(f"\n  Random Forest ranking:")
    for _, row in global_rf.iterrows():
        print(f"    {row['rank']:>2}. {row['feature']:<28} "
              f"mean|SHAP|={row['mean_abs_shap']:>7.4f}  "
              f"signed={row['mean_signed_shap']:>+7.4f}")
    global_df.to_csv(out_global, index=False, encoding="utf-8")
    print(f"\n[phase06_s06] wrote: {out_global}  ({len(global_df)} rows)")

    # =========================================================================
    # Per-cluster aggregation
    # =========================================================================
    print(f"\n[phase06_s06] per-cluster mean |SHAP| (mys focus):")
    pc_xgb = aggregate_per_cluster(shap_xgb, cluster_test, "xgb")
    pc_rf = aggregate_per_cluster(shap_rf, cluster_test, "rf")
    pc_df = pd.concat([pc_rf, pc_xgb], ignore_index=True)
    # Highlight mys row per cluster.
    print(f"  mys per-cluster signed mean SHAP "
          f"(Phase 05 RE Spec C anchors: c0=-0.80, c1=-1.19**, c2=-0.33):")
    for model_name in ("rf", "xgb"):
        sub = pc_df[
            (pc_df["model"] == model_name)
            & (pc_df["feature"] == "mean_years_schooling")
        ]
        for _, row in sub.iterrows():
            print(f"    {model_name} cluster={row['cluster']:<3} "
                  f"n={row['n']:>3}  "
                  f"mean|SHAP|={row['mean_abs_shap']:>6.4f}  "
                  f"signed={row['mean_signed_shap']:>+6.4f}")
    pc_df.to_csv(out_percluster, index=False, encoding="utf-8")
    print(f"[phase06_s06] wrote: {out_percluster}  ({len(pc_df)} rows)")

    # =========================================================================
    # SHAP values wide CSVs
    # =========================================================================
    def build_shap_wide(shap_arr, model_name):
        meta = df_test[["iso3", "year", CLUSTER_FEATURE]].reset_index(drop=True)
        wide = pd.DataFrame(shap_arr, columns=[f"shap_{f}" for f in ALL_FEATURES])
        wide.insert(0, "model", model_name)
        wide = pd.concat([wide.reset_index(drop=True), meta], axis=1)
        col_order = ["model", "iso3", "year", CLUSTER_FEATURE] + \
                    [f"shap_{f}" for f in ALL_FEATURES]
        return wide[col_order]

    wide_rf = build_shap_wide(shap_rf, "rf")
    wide_xgb = build_shap_wide(shap_xgb, "xgb")
    wide_rf.to_csv(out_shap_rf, index=False, encoding="utf-8")
    wide_xgb.to_csv(out_shap_xgb, index=False, encoding="utf-8")
    print(f"\n[phase06_s06] wrote: {out_shap_rf}  ({len(wide_rf)} rows)")
    print(f"[phase06_s06] wrote: {out_shap_xgb}  ({len(wide_xgb)} rows)")

    # =========================================================================
    # Figures: summary plots
    # =========================================================================
    print(f"\n[phase06_s06] plotting SHAP summary (beeswarm)...")
    # For RF, X_for_plot must match the SHAP feature order (transformed).
    X_test_rf_df = pd.DataFrame(X_test_rf_transformed, columns=ALL_FEATURES)
    plot_summary(
        shap_rf, X_test_rf_df, ALL_FEATURES, out_summary_rf,
        title="SHAP summary - Random Forest (test set)",
    )
    print(f"  wrote: {out_summary_rf}")
    plot_summary(
        shap_xgb, X_test_for_xgb, ALL_FEATURES, out_summary_xgb,
        title="SHAP summary - XGBoost (test set)",
    )
    print(f"  wrote: {out_summary_xgb}")

    # =========================================================================
    # Figures: dependence plots (top-3 numeric features)
    # =========================================================================
    print(f"\n[phase06_s06] plotting SHAP dependence (top-3 numeric)...")
    top3_rf = plot_dependence_top3(
        shap_rf, X_test_rf_df, global_rf, out_depend_rf,
        title="SHAP dependence - Random Forest (top-3 numeric features)",
    )
    print(f"  RF top-3: {top3_rf}")
    print(f"  wrote: {out_depend_rf}")
    top3_xgb = plot_dependence_top3(
        shap_xgb, X_test_for_xgb, global_xgb, out_depend_xgb,
        title="SHAP dependence - XGBoost (top-3 numeric features)",
    )
    print(f"  XGB top-3: {top3_xgb}")
    print(f"  wrote: {out_depend_xgb}")

    # =========================================================================
    # Figure: local waterfall - Brazil 2015 (XGBoost only)
    # =========================================================================
    print(f"\n[phase06_s06] local: {LOCAL_ISO3} {LOCAL_YEAR} (XGBoost)...")
    df_full = df  # original loaded panel (1,642 rows)
    local_row = df_full[
        (df_full["iso3"] == LOCAL_ISO3) & (df_full["year"] == LOCAL_YEAR)
    ]
    if len(local_row) != 1:
        raise ValueError(
            f"Expected exactly 1 row for {LOCAL_ISO3} {LOCAL_YEAR}, "
            f"got {len(local_row)}"
        )
    local_split = local_row["split"].values[0]
    local_cluster = local_row[CLUSTER_FEATURE].values[0]
    local_gini = float(local_row[TARGET].values[0])
    print(f"  row split={local_split}, cluster={local_cluster}, "
          f"gini={local_gini:.2f}")

    X_local = build_xgb_X(local_row)
    plot_local_waterfall_xgb(
        xgb_model, X_local, ALL_FEATURES, out_local,
        title=f"SHAP waterfall - {LOCAL_ISO3} {LOCAL_YEAR} "
              f"(XGBoost, true gini={local_gini:.2f})",
    )
    print(f"  wrote: {out_local}")

    print("\n[phase06_s06] Step 06 complete.")


if __name__ == "__main__":
    main()
