"""
Phase 06 - Step 07: Phase 05 vs Phase 06 comparison.

Purpose:
    Implement the three-axis comparison defined in Decision 7:

    (i) Spearman rho between Phase 05 RE Spec A absolute coefficient
        ranking and Phase 06 mean |SHAP| ranking on the five common
        Spec A features, computed separately for RF and XGBoost.

    (ii) Per-cluster mys SHAP slope vs Phase 05 RE Spec C
         per-cluster slopes. Slope-of-SHAP is the linear-regression
         slope of SHAP_mys on mys within each test-set cluster, which
         is the SHAP-based analog of the Phase 05 within-cluster
         coefficient.

    (iii) Qualitative dependence-plot interpretation is deferred to
          Step 09 notebook prose.

    Per Convention 6.15: SHAP attributions and Phase 05 coefficients
    are both non-causal characterisations of the same data; the
    comparison concerns where ML and panel methods agree on
    descriptive feature importance, not causal effect.

Inputs:
    outputs/tables/phase05_s05_re_results.csv
    outputs/tables/phase05_s06_per_cluster_slopes.csv
    outputs/tables/phase06_s06_shap_global.csv
    outputs/tables/phase06_s06_shap_values_test_rf.csv
    outputs/tables/phase06_s06_shap_values_test_xgb.csv
    data/processed/panel_ml.csv

Outputs:
    outputs/tables/phase06_s07_comparison.csv
    outputs/figures/phase06_s07_ranking_comparison.png
    outputs/figures/phase06_s07_per_cluster_slopes.png
"""

from __future__ import annotations

import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy.stats import linregress, spearmanr

PROJECT_ROOT_HINT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT_HINT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT_HINT))

from src.paths import find_project_root
from src.io_utils import read_csv_with_encoding_fallback

SEED = 42
np.random.seed(SEED)

DPI = 300

# Common features for Decision 7-(i) Spearman comparison.
SPEC_A_FEATURES = [
    "mean_years_schooling",
    "enrol_secondary",
    "log_gdp_per_capita_ppp",
    "log_population",
    "urban_population_pct",
]

CLUSTER_FEATURE = "cluster_kmeans_k3"


def _read_csv(path: Path) -> pd.DataFrame:
    df, _ = read_csv_with_encoding_fallback(path)
    return df


def load_phase05_re_spec_a_coefs(path: Path) -> pd.DataFrame:
    """Filter phase05_s05_re_results to RE Spec A coefficients.

    Returns one row per feature: feature, coef, abs_coef, rank_by_abs.
    """
    df = _read_csv(path)
    sub = df[
        (df["estimator"] == "RE")
        & (df["spec"] == "A")
        & (df["kind"] == "coef")
        & (df["variable"] != "Intercept")
    ].copy()
    sub = sub[["variable", "value", "std_error", "pvalue"]].rename(
        columns={"variable": "feature", "value": "coef"}
    )
    sub["abs_coef"] = sub["coef"].abs()
    sub = sub.sort_values("abs_coef", ascending=False).reset_index(drop=True)
    sub["phase05_rank"] = range(1, len(sub) + 1)
    return sub


def load_phase05_re_per_cluster_slopes(path: Path) -> pd.DataFrame:
    """Filter to RE within-cluster mys slopes."""
    df = _read_csv(path)
    sub = df[df["estimator"] == "RE"].copy()
    sub = sub[[
        "cluster", "slope", "std_error", "tstat", "pvalue",
        "ci_low", "ci_high", "n_obs",
    ]]
    sub["cluster"] = sub["cluster"].astype(int)
    sub = sub.sort_values("cluster").reset_index(drop=True)
    return sub


def load_phase06_shap_global(path: Path) -> pd.DataFrame:
    """Load Step 06 global ranking CSV. Already in long format."""
    df = _read_csv(path)
    return df


def compute_spearman(
    phase05_coefs: pd.DataFrame,
    phase06_global: pd.DataFrame,
    model_name: str,
    common_features: list,
) -> tuple:
    """Compute Spearman rho between |Phase 05 coef| and Phase 06 mean
    |SHAP| over a set of common features.
    """
    p5 = phase05_coefs[phase05_coefs["feature"].isin(common_features)].copy()
    p5_ranked = p5.sort_values("abs_coef", ascending=False).reset_index(drop=True)
    p5_ranked["p05_rank"] = range(1, len(p5_ranked) + 1)

    p6 = phase06_global[
        (phase06_global["model"] == model_name)
        & (phase06_global["feature"].isin(common_features))
    ].copy()
    p6_ranked = p6.sort_values("mean_abs_shap", ascending=False).reset_index(drop=True)
    p6_ranked["p06_rank"] = range(1, len(p6_ranked) + 1)

    if len(p5_ranked) != len(common_features) or len(p6_ranked) != len(common_features):
        raise ValueError(
            f"Feature subset mismatch for {model_name}: "
            f"p05 has {len(p5_ranked)}, p06 has {len(p6_ranked)}, "
            f"expected {len(common_features)}"
        )

    merged = p5_ranked[["feature", "coef", "abs_coef", "p05_rank"]].merge(
        p6_ranked[["feature", "mean_abs_shap", "mean_signed_shap", "p06_rank"]],
        on="feature",
        how="inner",
    )
    rho, p_rho = spearmanr(merged["abs_coef"].values,
                           merged["mean_abs_shap"].values)
    return float(rho), float(p_rho), merged


def compute_per_cluster_shap_slope(
    shap_wide: pd.DataFrame,
    panel_ml: pd.DataFrame,
    feature: str,
    model_name: str,
) -> pd.DataFrame:
    """For each cluster in {0, 1, 2}, regress SHAP_<feature> on
    <feature> within that cluster's test-set rows.

    Returns long-format DataFrame:
        model, cluster, n, slope, intercept, slope_se, slope_tstat,
        slope_pvalue, r2
    """
    shap_col = f"shap_{feature}"
    if shap_col not in shap_wide.columns:
        raise KeyError(
            f"{shap_col} not in SHAP wide CSV columns: "
            f"{shap_wide.columns.tolist()}"
        )

    test_rows = panel_ml[panel_ml["split"] == "test"][
        ["iso3", "year", feature, CLUSTER_FEATURE]
    ].reset_index(drop=True)

    merged = shap_wide.merge(
        test_rows,
        on=["iso3", "year"],
        how="inner",
        validate="one_to_one",
        suffixes=("", "_panel"),
    )
    # The cluster column may collide; both sources should agree by design,
    # but use the panel_ml version as authoritative.
    if f"{CLUSTER_FEATURE}_panel" in merged.columns:
        merged[CLUSTER_FEATURE] = merged[f"{CLUSTER_FEATURE}_panel"]
        merged = merged.drop(columns=[f"{CLUSTER_FEATURE}_panel"])

    rows = []
    for c in (0, 1, 2):
        sub = merged[merged[CLUSTER_FEATURE] == c]
        n = len(sub)
        if n < 5:
            print(f"  cluster {c}: n={n} too small for slope estimation; skipping")
            continue
        x = sub[feature].values.astype(float)
        y = sub[shap_col].values.astype(float)
        # Drop any residual NaN (shouldn't happen for mys but be defensive)
        mask = np.isfinite(x) & np.isfinite(y)
        x = x[mask]
        y = y[mask]
        if len(x) < 5:
            print(f"  cluster {c}: n_finite={len(x)} too small; skipping")
            continue
        result = linregress(x, y)
        rows.append({
            "model": model_name,
            "feature": feature,
            "cluster": c,
            "n": int(len(x)),
            "slope": float(result.slope),
            "intercept": float(result.intercept),
            "slope_se": float(result.stderr),
            "slope_tstat": float(result.slope / result.stderr) if result.stderr > 0 else np.nan,
            "slope_pvalue": float(result.pvalue),
            "r2": float(result.rvalue ** 2),
        })
    return pd.DataFrame(rows)


def plot_ranking_comparison(
    merged_rf: pd.DataFrame,
    merged_xgb: pd.DataFrame,
    rho_rf: float,
    rho_xgb: float,
    p_rho_rf: float,
    p_rho_xgb: float,
    out_path: Path,
) -> None:
    """Two-panel scatter: x = Phase 05 |coef| rank, y = Phase 06
    mean|SHAP| rank, one panel per model. Diagonal = perfect rank
    agreement."""
    fig, axes = plt.subplots(1, 2, figsize=(12, 5.5), sharey=True)
    titles = [
        f"Random Forest\nSpearman rho = {rho_rf:.3f}  (p = {p_rho_rf:.3f})",
        f"XGBoost\nSpearman rho = {rho_xgb:.3f}  (p = {p_rho_xgb:.3f})",
    ]
    n_features = len(merged_rf)

    for ax, df, title in zip(axes, [merged_rf, merged_xgb], titles):
        ax.scatter(
            df["p05_rank"], df["p06_rank"],
            s=120, c="#1f77b4", edgecolor="white", zorder=3,
        )
        for _, row in df.iterrows():
            ax.annotate(
                row["feature"],
                xy=(row["p05_rank"], row["p06_rank"]),
                xytext=(5, 5), textcoords="offset points",
                fontsize=8,
            )
        # Identity line.
        ax.plot([0.5, n_features + 0.5], [0.5, n_features + 0.5],
                color="grey", linestyle="--", alpha=0.5, zorder=1,
                label="perfect rank agreement")
        ax.set_xlim(0.5, n_features + 0.5)
        ax.set_ylim(0.5, n_features + 0.5)
        ax.set_xticks(range(1, n_features + 1))
        ax.set_yticks(range(1, n_features + 1))
        ax.invert_xaxis()
        ax.invert_yaxis()
        ax.set_xlabel("Phase 05 |coef| rank (1 = strongest)")
        ax.set_ylabel("Phase 06 mean |SHAP| rank (1 = strongest)")
        ax.set_title(title)
        ax.grid(True, alpha=0.3)
        ax.legend(loc="lower right", fontsize=8)
    fig.suptitle(
        "Phase 05 vs Phase 06: ranking of 5 Spec A features",
        fontsize=12, y=1.02,
    )
    plt.tight_layout()
    plt.savefig(out_path, dpi=DPI, bbox_inches="tight")
    plt.close("all")


def plot_per_cluster_slopes(
    p05_slopes: pd.DataFrame,
    p06_slopes_rf: pd.DataFrame,
    p06_slopes_xgb: pd.DataFrame,
    out_path: Path,
) -> None:
    """Side-by-side bar chart per cluster: Phase 05 RE slope vs
    Phase 06 SHAP-on-mys slope (RF) vs SHAP-on-mys slope (XGB)."""
    clusters = [0, 1, 2]
    labels = [
        "Cluster 0\n(low-dev)",
        "Cluster 1\n(Kuznets transition)",
        "Cluster 2\n(mature)",
    ]

    p05_vals, p05_se = [], []
    p06_rf_vals, p06_rf_se = [], []
    p06_xgb_vals, p06_xgb_se = [], []

    for c in clusters:
        r5 = p05_slopes[p05_slopes["cluster"] == c]
        rrf = p06_slopes_rf[p06_slopes_rf["cluster"] == c]
        rxgb = p06_slopes_xgb[p06_slopes_xgb["cluster"] == c]
        p05_vals.append(float(r5["slope"].values[0]) if len(r5) else np.nan)
        p05_se.append(float(r5["std_error"].values[0]) if len(r5) else np.nan)
        p06_rf_vals.append(float(rrf["slope"].values[0]) if len(rrf) else np.nan)
        p06_rf_se.append(float(rrf["slope_se"].values[0]) if len(rrf) else np.nan)
        p06_xgb_vals.append(float(rxgb["slope"].values[0]) if len(rxgb) else np.nan)
        p06_xgb_se.append(float(rxgb["slope_se"].values[0]) if len(rxgb) else np.nan)

    x = np.arange(len(clusters))
    width = 0.27
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.bar(x - width, p05_vals, width, yerr=p05_se,
           label="Phase 05 RE Spec C slope",
           color="#1f77b4", capsize=4)
    ax.bar(x, p06_rf_vals, width, yerr=p06_rf_se,
           label="Phase 06 SHAP-on-mys slope (RF)",
           color="#2ca02c", capsize=4)
    ax.bar(x + width, p06_xgb_vals, width, yerr=p06_xgb_se,
           label="Phase 06 SHAP-on-mys slope (XGB)",
           color="#d62728", capsize=4)
    ax.axhline(0, color="black", linewidth=0.5)
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.set_ylabel("slope: dGini / d(mean_years_schooling)")
    ax.set_title(
        "Per-cluster mys-Gini slope: Phase 05 RE coefficient vs "
        "Phase 06 SHAP regression slope"
    )
    ax.legend(loc="best", fontsize=9)
    ax.grid(True, axis="y", alpha=0.3)
    plt.tight_layout()
    plt.savefig(out_path, dpi=DPI, bbox_inches="tight")
    plt.close("all")


def main() -> None:
    project_root = find_project_root()
    tables_dir = project_root / "outputs" / "tables"
    figures_dir = project_root / "outputs" / "figures"
    tables_dir.mkdir(parents=True, exist_ok=True)
    figures_dir.mkdir(parents=True, exist_ok=True)

    # Inputs
    p05_re_path = tables_dir / "phase05_s05_re_results.csv"
    p05_pcs_path = tables_dir / "phase05_s06_per_cluster_slopes.csv"
    p06_global_path = tables_dir / "phase06_s06_shap_global.csv"
    p06_shap_rf_path = tables_dir / "phase06_s06_shap_values_test_rf.csv"
    p06_shap_xgb_path = tables_dir / "phase06_s06_shap_values_test_xgb.csv"
    panel_ml_path = project_root / "data" / "processed" / "panel_ml.csv"

    # Outputs
    out_csv = tables_dir / "phase06_s07_comparison.csv"
    out_rank_fig = figures_dir / "phase06_s07_ranking_comparison.png"
    out_slope_fig = figures_dir / "phase06_s07_per_cluster_slopes.png"

    print(f"[phase06_s07] project root: {project_root}")
    for p in [p05_re_path, p05_pcs_path, p06_global_path,
              p06_shap_rf_path, p06_shap_xgb_path, panel_ml_path]:
        print(f"  input: {p}  exists={p.exists()}")

    # ---- Load Phase 05 anchors ----
    p05_coefs = load_phase05_re_spec_a_coefs(p05_re_path)
    p05_pcs = load_phase05_re_per_cluster_slopes(p05_pcs_path)
    print(f"\n[phase06_s07] Phase 05 RE Spec A coefficients (5 features):")
    for _, r in p05_coefs.iterrows():
        print(f"  {r['phase05_rank']:>2}. {r['feature']:<28} "
              f"coef={r['coef']:>+8.4f}  |coef|={r['abs_coef']:.4f}  "
              f"p={r['pvalue']:.4f}")
    print(f"\n[phase06_s07] Phase 05 RE Spec C per-cluster mys slopes:")
    for _, r in p05_pcs.iterrows():
        sig = "**" if r["pvalue"] < 0.05 else ("*" if r["pvalue"] < 0.10 else "")
        print(f"  cluster {r['cluster']}: slope={r['slope']:>+7.4f}  "
              f"SE={r['std_error']:.4f}  p={r['pvalue']:.4f}{sig}")

    # ---- Load Phase 06 ----
    p06_global = load_phase06_shap_global(p06_global_path)
    panel_ml, _ = read_csv_with_encoding_fallback(panel_ml_path)
    shap_rf_wide = _read_csv(p06_shap_rf_path)
    shap_xgb_wide = _read_csv(p06_shap_xgb_path)
    print(f"\n[phase06_s07] Phase 06 inputs loaded:")
    print(f"  shap global rows:   {len(p06_global)}")
    print(f"  shap rf wide rows:  {len(shap_rf_wide)}")
    print(f"  shap xgb wide rows: {len(shap_xgb_wide)}")
    print(f"  panel_ml rows:      {len(panel_ml)}")

    # =========================================================================
    # Decision 7-(i): Spearman rho on the 5 Spec A features
    # =========================================================================
    print(f"\n[phase06_s07] Decision 7-(i): Spearman rho on 5 Spec A features")
    print(f"  features: {SPEC_A_FEATURES}")
    rho_rf, p_rho_rf, merged_rf = compute_spearman(
        p05_coefs, p06_global, "rf", SPEC_A_FEATURES,
    )
    rho_xgb, p_rho_xgb, merged_xgb = compute_spearman(
        p05_coefs, p06_global, "xgb", SPEC_A_FEATURES,
    )
    print(f"\n  RF rank table:")
    for _, r in merged_rf.iterrows():
        print(f"    {r['feature']:<28} p05_rank={r['p05_rank']}  "
              f"|coef|={r['abs_coef']:.4f}    "
              f"p06_rank={r['p06_rank']}  mean|SHAP|={r['mean_abs_shap']:.4f}")
    print(f"  Spearman rho (RF):  {rho_rf:+.4f}  (p={p_rho_rf:.4f})")
    print(f"\n  XGB rank table:")
    for _, r in merged_xgb.iterrows():
        print(f"    {r['feature']:<28} p05_rank={r['p05_rank']}  "
              f"|coef|={r['abs_coef']:.4f}    "
              f"p06_rank={r['p06_rank']}  mean|SHAP|={r['mean_abs_shap']:.4f}")
    print(f"  Spearman rho (XGB): {rho_xgb:+.4f}  (p={p_rho_xgb:.4f})")

    # =========================================================================
    # Decision 7-(ii): per-cluster mys SHAP slope vs Phase 05 slope
    # =========================================================================
    print(f"\n[phase06_s07] Decision 7-(ii): per-cluster slope-of-SHAP-on-mys")
    p06_slopes_rf = compute_per_cluster_shap_slope(
        shap_rf_wide, panel_ml, "mean_years_schooling", "rf",
    )
    p06_slopes_xgb = compute_per_cluster_shap_slope(
        shap_xgb_wide, panel_ml, "mean_years_schooling", "xgb",
    )

    print(f"\n  Combined per-cluster slope table (Phase 05 vs Phase 06):")
    print(f"  {'cluster':<8} {'P05 RE slope':>14} {'P06 RF slope':>14} "
          f"{'P06 XGB slope':>14}")
    for c in (0, 1, 2):
        r5 = p05_pcs[p05_pcs["cluster"] == c]
        rrf = p06_slopes_rf[p06_slopes_rf["cluster"] == c]
        rxgb = p06_slopes_xgb[p06_slopes_xgb["cluster"] == c]
        v5 = f"{r5['slope'].values[0]:+.4f}" if len(r5) else "n/a"
        vrf = (f"{rrf['slope'].values[0]:+.4f}" if len(rrf) else "n/a")
        vxgb = (f"{rxgb['slope'].values[0]:+.4f}" if len(rxgb) else "n/a")
        print(f"  {c:<8} {v5:>14} {vrf:>14} {vxgb:>14}")

    print(f"\n  Sign-agreement (Phase 05 sign vs Phase 06 SHAP-slope sign):")
    for c in (0, 1, 2):
        r5 = p05_pcs[p05_pcs["cluster"] == c]
        rrf = p06_slopes_rf[p06_slopes_rf["cluster"] == c]
        rxgb = p06_slopes_xgb[p06_slopes_xgb["cluster"] == c]
        if not len(r5) or not len(rrf) or not len(rxgb):
            continue
        s5 = np.sign(r5["slope"].values[0])
        srf = np.sign(rrf["slope"].values[0])
        sxgb = np.sign(rxgb["slope"].values[0])
        agree_rf = "YES" if s5 == srf else "NO"
        agree_xgb = "YES" if s5 == sxgb else "NO"
        print(f"  cluster {c}: RF agreement={agree_rf}, "
              f"XGB agreement={agree_xgb}")

    # =========================================================================
    # Build comparison CSV (long format)
    # =========================================================================
    rows = []
    # 7-(i) Spearman summary rows
    for model_name, rho, p_rho in [
        ("rf", rho_rf, p_rho_rf), ("xgb", rho_xgb, p_rho_xgb),
    ]:
        rows.append({
            "comparison_id": f"7i_spearman_{model_name}",
            "axis": "ranking_spearman",
            "model": model_name,
            "feature_or_cluster": "spec_a_5_features",
            "phase05_value": "|coef| ranking on 5 features",
            "phase06_value": "mean|SHAP| ranking on 5 features",
            "metric_value": rho,
            "metric_pvalue": p_rho,
            "n": len(SPEC_A_FEATURES),
        })
    # 7-(i) per-feature rank rows for both models
    for model_name, merged in [("rf", merged_rf), ("xgb", merged_xgb)]:
        for _, r in merged.iterrows():
            rows.append({
                "comparison_id": f"7i_rank_{model_name}_{r['feature']}",
                "axis": "ranking_per_feature",
                "model": model_name,
                "feature_or_cluster": r["feature"],
                "phase05_value": f"rank={r['p05_rank']} |coef|={r['abs_coef']:.4f}",
                "phase06_value": f"rank={r['p06_rank']} mean|SHAP|={r['mean_abs_shap']:.4f}",
                "metric_value": float(r["p06_rank"] - r["p05_rank"]),
                "metric_pvalue": np.nan,
                "n": np.nan,
            })
    # 7-(ii) per-cluster slope comparison rows
    for c in (0, 1, 2):
        r5 = p05_pcs[p05_pcs["cluster"] == c]
        rrf = p06_slopes_rf[p06_slopes_rf["cluster"] == c]
        rxgb = p06_slopes_xgb[p06_slopes_xgb["cluster"] == c]
        if not len(r5):
            continue
        p05_slope = float(r5["slope"].values[0])
        p05_p = float(r5["pvalue"].values[0])
        for model_name, sub in [("rf", rrf), ("xgb", rxgb)]:
            if not len(sub):
                continue
            p06_slope = float(sub["slope"].values[0])
            p06_p = float(sub["slope_pvalue"].values[0])
            n_c = int(sub["n"].values[0])
            sign_agree = bool(np.sign(p05_slope) == np.sign(p06_slope))
            rows.append({
                "comparison_id": f"7ii_mys_slope_cluster_{c}_{model_name}",
                "axis": "per_cluster_mys_slope",
                "model": model_name,
                "feature_or_cluster": f"cluster_{c}",
                "phase05_value": f"RE slope={p05_slope:+.4f} (p={p05_p:.4f})",
                "phase06_value": f"SHAP-on-mys slope={p06_slope:+.4f} (p={p06_p:.4f})",
                "metric_value": p06_slope - p05_slope,
                "metric_pvalue": np.nan,
                "n": n_c,
                "sign_agreement": sign_agree,
            })
    comparison_df = pd.DataFrame(rows)
    comparison_df.to_csv(out_csv, index=False, encoding="utf-8")
    print(f"\n[phase06_s07] wrote: {out_csv}  ({len(comparison_df)} rows)")

    # =========================================================================
    # Figures
    # =========================================================================
    plot_ranking_comparison(
        merged_rf, merged_xgb, rho_rf, rho_xgb, p_rho_rf, p_rho_xgb,
        out_rank_fig,
    )
    print(f"[phase06_s07] wrote: {out_rank_fig}")

    plot_per_cluster_slopes(
        p05_pcs, p06_slopes_rf, p06_slopes_xgb, out_slope_fig,
    )
    print(f"[phase06_s07] wrote: {out_slope_fig}")

    print("\n[phase06_s07] Step 07 complete.")


if __name__ == "__main__":
    main()
