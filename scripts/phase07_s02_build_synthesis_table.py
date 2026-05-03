"""
Phase 07 - Step 02: Build cross-method synthesis table.

Purpose:
    Consolidate cross-method mys-Gini estimates from Phase 03-06 into a
    single long-format CSV. This table is the data source for the
    Step 03 convergence figure and the Step 04 notebook synthesis
    presentation.

    Per Convention 6.4 (cite, do not recompute), all values are sourced
    from phase-summary anchors and PROJECT_LOG entries documented during
    Phases 03-06. This script does NOT re-fit any model or recompute any
    coefficient. The `source_artefact` column on each row provides
    traceability to the Phase 03-06 CSV / PROJECT_LOG entry that is the
    on-disk source of truth for that value.

    The schema is mys_effect-focused. Boundary-holdout rows are
    deliberately excluded because the Phase 06 anchor reports the
    boundary mys SHAP as a range (+1.62 to +2.46 for BRA/ZAF/MEX) rather
    than a precisely-anchored point estimate; including a hardcoded
    point estimate would risk Convention 6.12 (don't fabricate
    references). Boundary caveats are cited from
    `outputs/tables/phase06_s08_robustness.csv` directly in the notebook
    prose. Model-performance R^2 values are likewise out of scope for
    this mys_effect-focused table.

Inputs:
    None at runtime. Values hardcoded with citations to Phase 03-06
    anchors. See SOURCES tuple below for the full citation manifest.

Outputs:
    outputs/tables/phase07_s02_synthesis_table.csv
        16 rows x 10 columns. Long-format. UTF-8.
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

# Make src/ importable when run from any working directory.
_HERE = Path(__file__).resolve().parent
_PROJECT_ROOT_GUESS = _HERE.parent
if str(_PROJECT_ROOT_GUESS) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT_GUESS))

from src.paths import find_project_root  # noqa: E402


# -----------------------------------------------------------------------------
# Source manifest (for audit / docstring purposes)
# -----------------------------------------------------------------------------

SOURCES = (
    "phase03_summary.md - Pearson r mys-Gini, single-regressor OLS R^2 = 0.27",
    "phase05_summary.md / outputs/tables/phase05_s03_ols_results.csv - Pooled OLS Spec A",
    "phase05_summary.md / outputs/tables/phase05_s04_fe_results.csv - FE Spec A",
    "phase05_summary.md / outputs/tables/phase05_s05_re_results.csv - RE Spec A",
    "phase05_summary.md / outputs/tables/phase05_s06_per_cluster_slopes.csv - RE Spec C per cluster",
    "phase06_summary.md / outputs/tables/phase06_s03_linear_baseline.csv - Ridge raw-scale coefficient",
    "phase06_summary.md / outputs/tables/phase06_s06_shap_global.csv - RF/XGB mean signed SHAP for mys",
    "phase06_summary.md / outputs/tables/phase06_s07_comparison.csv - RF/XGB SHAP-on-mys per cluster",
    "PROJECT_LOG.md 2026-05-02 Phase 05 Step 09 entry - Cluster 1 95% CI [-2.09, -0.28]",
)


# -----------------------------------------------------------------------------
# Schema
# -----------------------------------------------------------------------------

COLUMNS = [
    "phase",
    "estimator",
    "scope",
    "mys_effect",
    "se",
    "ci_lower",
    "ci_upper",
    "p",
    "n",
    "source_artefact",
]


# -----------------------------------------------------------------------------
# Records (anchored values - do not modify without verifying against
# phase summaries and/or PROJECT_LOG)
# -----------------------------------------------------------------------------

NA = np.nan

RECORDS = [
    # ----- Phase 03: univariate Pearson correlation -----
    # Note: Pearson r is on a different scale from regression slopes
    # (unitless [-1, 1] vs Gini-points-per-mys-year). The convergence
    # figure (Step 03) handles this with an explicit annotation.
    {
        "phase": "03",
        "estimator": "univariate_pearson_r",
        "scope": "aggregate",
        "mys_effect": -0.52,
        "se": NA,
        "ci_lower": NA,
        "ci_upper": NA,
        "p": NA,
        "n": NA,  # pairwise-complete count not anchored as a single number
        "source_artefact": "phase03_summary.md (Pearson r mys-Gini, single-regressor OLS R^2 = 0.27)",
    },

    # ----- Phase 05: aggregate Spec A -----
    # Spec A canonical sample: 1,642 country-years from 153 countries
    # (phase05_summary.md Step 02; also PROJECT_LOG Phase 05 Step 09 finding 8).
    {
        "phase": "05",
        "estimator": "pooled_ols_spec_a",
        "scope": "aggregate",
        "mys_effect": -1.328,
        "se": 0.275,
        "ci_lower": -1.87,
        "ci_upper": -0.79,
        "p": 0.000,  # phase05 anchor reports p < 0.001
        "n": 1642,
        "source_artefact": "outputs/tables/phase05_s03_ols_results.csv (Spec A row)",
    },
    {
        "phase": "05",
        "estimator": "fe_spec_a",
        "scope": "aggregate",
        "mys_effect": -0.384,
        "se": 0.425,
        "ci_lower": -1.22,
        "ci_upper": 0.45,
        "p": 0.366,
        "n": 1642,
        "source_artefact": "outputs/tables/phase05_s04_fe_results.csv (Spec A row)",
    },
    {
        "phase": "05",
        "estimator": "re_spec_a",
        "scope": "aggregate",
        "mys_effect": -0.688,
        "se": 0.285,
        "ci_lower": -1.25,
        "ci_upper": -0.13,
        "p": 0.016,
        "n": 1642,
        "source_artefact": "outputs/tables/phase05_s05_re_results.csv (Spec A row)",
    },

    # ----- Phase 05: per-cluster RE Spec C within-country slopes -----
    # Cluster IDs canonicalised by ascending mean_years_schooling
    # (Phase 04 Step 04). Cluster 1 95% CI from PROJECT_LOG 2026-05-02
    # Phase 05 Step 09 finding 4. Other clusters' CIs not anchored.
    {
        "phase": "05",
        "estimator": "re_spec_c_within_country",
        "scope": "cluster_0",
        "mys_effect": -0.80,
        "se": NA,
        "ci_lower": NA,
        "ci_upper": NA,
        "p": 0.13,
        "n": NA,  # cluster-specific cy count not separately anchored
        "source_artefact": "outputs/tables/phase05_s06_per_cluster_slopes.csv (RE Spec C, cluster_kmeans_k3=0)",
    },
    {
        "phase": "05",
        "estimator": "re_spec_c_within_country",
        "scope": "cluster_1",
        "mys_effect": -1.19,
        "se": NA,
        "ci_lower": -2.09,
        "ci_upper": -0.28,
        "p": 0.010,
        "n": NA,
        "source_artefact": "outputs/tables/phase05_s06_per_cluster_slopes.csv (RE Spec C, cluster_kmeans_k3=1)",
    },
    {
        "phase": "05",
        "estimator": "re_spec_c_within_country",
        "scope": "cluster_2",
        "mys_effect": -0.33,
        "se": NA,
        "ci_lower": NA,
        "ci_upper": NA,
        "p": 0.42,
        "n": NA,
        "source_artefact": "outputs/tables/phase05_s06_per_cluster_slopes.csv (RE Spec C, cluster_kmeans_k3=2)",
    },

    # ----- Phase 06: aggregate -----
    # Ridge: raw-scale coefficient on mys. SHAP: mean signed across test set.
    # Test n = 310 = 17 + 105 + 188 (cluster 0/1/2 test counts from
    # phase06_summary.md).
    {
        "phase": "06",
        "estimator": "ridge_raw_scale_coef",
        "scope": "aggregate",
        "mys_effect": -1.42,
        "se": NA,
        "ci_lower": NA,
        "ci_upper": NA,
        "p": NA,
        "n": 1642,  # Phase 06 Spec A listwise sample (training + test)
        "source_artefact": "outputs/tables/phase06_s03_linear_baseline.csv (Ridge raw-scale, mys row)",
    },
    {
        "phase": "06",
        "estimator": "rf_mean_signed_shap",
        "scope": "aggregate",
        "mys_effect": -1.130,
        "se": NA,
        "ci_lower": NA,
        "ci_upper": NA,
        "p": NA,
        "n": 310,  # test set: cluster 0/1/2 = 17/105/188
        "source_artefact": "outputs/tables/phase06_s06_shap_global.csv (RF, mys mean signed SHAP)",
    },
    {
        "phase": "06",
        "estimator": "xgb_mean_signed_shap",
        "scope": "aggregate",
        "mys_effect": -1.060,
        "se": NA,
        "ci_lower": NA,
        "ci_upper": NA,
        "p": NA,
        "n": 310,
        "source_artefact": "outputs/tables/phase06_s06_shap_global.csv (XGB, mys mean signed SHAP)",
    },

    # ----- Phase 06: per-cluster SHAP-on-mys regression slope -----
    # Slope of (per-row signed SHAP for mys) on (per-row mys), regressed
    # within each test-set cluster (scipy.stats.linregress). See
    # phase06_summary.md Step 07 and outputs/tables/phase06_s07_comparison.csv.
    {
        "phase": "06",
        "estimator": "rf_shap_on_mys_slope",
        "scope": "cluster_0",
        "mys_effect": -0.08,
        "se": NA,
        "ci_lower": NA,
        "ci_upper": NA,
        "p": NA,
        "n": 17,
        "source_artefact": "outputs/tables/phase06_s07_comparison.csv (model=RF, cluster=0)",
    },
    {
        "phase": "06",
        "estimator": "rf_shap_on_mys_slope",
        "scope": "cluster_1",
        "mys_effect": -1.92,
        "se": NA,
        "ci_lower": NA,
        "ci_upper": NA,
        "p": NA,
        "n": 105,
        "source_artefact": "outputs/tables/phase06_s07_comparison.csv (model=RF, cluster=1)",
    },
    {
        "phase": "06",
        "estimator": "rf_shap_on_mys_slope",
        "scope": "cluster_2",
        "mys_effect": -0.84,
        "se": NA,
        "ci_lower": NA,
        "ci_upper": NA,
        "p": NA,
        "n": 188,
        "source_artefact": "outputs/tables/phase06_s07_comparison.csv (model=RF, cluster=2)",
    },
    {
        "phase": "06",
        "estimator": "xgb_shap_on_mys_slope",
        "scope": "cluster_0",
        "mys_effect": +0.16,
        "se": NA,
        "ci_lower": NA,
        "ci_upper": NA,
        "p": NA,
        "n": 17,
        "source_artefact": "outputs/tables/phase06_s07_comparison.csv (model=XGB, cluster=0)",
    },
    {
        "phase": "06",
        "estimator": "xgb_shap_on_mys_slope",
        "scope": "cluster_1",
        "mys_effect": -2.00,
        "se": NA,
        "ci_lower": NA,
        "ci_upper": NA,
        "p": NA,
        "n": 105,
        "source_artefact": "outputs/tables/phase06_s07_comparison.csv (model=XGB, cluster=1)",
    },
    {
        "phase": "06",
        "estimator": "xgb_shap_on_mys_slope",
        "scope": "cluster_2",
        "mys_effect": -0.85,
        "se": NA,
        "ci_lower": NA,
        "ci_upper": NA,
        "p": NA,
        "n": 188,
        "source_artefact": "outputs/tables/phase06_s07_comparison.csv (model=XGB, cluster=2)",
    },
]


# -----------------------------------------------------------------------------
# Build, validate, and write the synthesis table
# -----------------------------------------------------------------------------

def build_synthesis_table() -> pd.DataFrame:
    """Construct the long-format synthesis DataFrame from RECORDS."""
    df = pd.DataFrame.from_records(RECORDS, columns=COLUMNS)

    # Type discipline: numeric columns float64, descriptive columns string.
    numeric_cols = ["mys_effect", "se", "ci_lower", "ci_upper", "p", "n"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="raise")

    string_cols = ["phase", "estimator", "scope", "source_artefact"]
    for col in string_cols:
        df[col] = df[col].astype("string")

    return df


def validate(df: pd.DataFrame) -> None:
    """Run sanity checks against expected anchors."""
    # Expected row count
    assert len(df) == 16, f"Expected 16 rows, got {len(df)}"

    # Expected column order
    assert list(df.columns) == COLUMNS, "Column order mismatch"

    # Headline anchor: Cluster 1 RE Spec C = -1.19, p = 0.010
    cluster_1_re = df[
        (df["estimator"] == "re_spec_c_within_country")
        & (df["scope"] == "cluster_1")
    ]
    assert len(cluster_1_re) == 1, "Expected exactly one cluster_1 RE Spec C row"
    assert abs(cluster_1_re["mys_effect"].iloc[0] - (-1.19)) < 1e-9
    assert abs(cluster_1_re["p"].iloc[0] - 0.010) < 1e-9

    # Three-strategy chained finding: cluster_1 in P05 RE, P06 RF, P06 XGB
    # all negative
    cluster_1 = df[df["scope"] == "cluster_1"]
    assert (cluster_1["mys_effect"] < 0).all(), (
        "Cluster 1 chained finding broken: not all three estimators negative"
    )

    # Phase coverage
    expected_phases = {"03", "05", "06"}
    actual_phases = set(df["phase"].unique())
    assert actual_phases == expected_phases, (
        f"Phase coverage mismatch: expected {expected_phases}, got {actual_phases}"
    )

    # Every row has a non-empty source_artefact
    assert df["source_artefact"].notna().all(), "Missing source_artefact"
    assert (df["source_artefact"].str.len() > 0).all(), "Empty source_artefact"


def main() -> None:
    project_root = find_project_root()
    out_dir = project_root / "outputs" / "tables"
    out_path = out_dir / "phase07_s02_synthesis_table.csv"

    out_dir.mkdir(parents=True, exist_ok=True)

    df = build_synthesis_table()
    validate(df)

    df.to_csv(out_path, index=False, encoding="utf-8")

    print(f"Project root : {project_root}")
    print(f"Output path  : {out_path}")
    print(f"Rows         : {len(df)}")
    print(f"Columns      : {len(df.columns)}")
    print()
    print("Phase coverage:")
    print(df["phase"].value_counts().sort_index().to_string())
    print()
    print("Scope coverage:")
    print(df["scope"].value_counts().sort_index().to_string())
    print()
    print("Cluster 1 chained finding (the project headline):")
    headline = (
        df[df["scope"] == "cluster_1"]
        [["phase", "estimator", "mys_effect", "p"]]
        .to_string(index=False)
    )
    print(headline)
    print()
    print(f"[OK] Wrote synthesis table to {out_path.relative_to(project_root)}")


if __name__ == "__main__":
    main()
