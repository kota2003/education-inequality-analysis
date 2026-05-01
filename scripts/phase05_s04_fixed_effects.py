"""
Phase 05 - Step 04: Fixed Effects.

Purpose:
    Fit Spec A, B, C under `linearmodels.PanelOLS` with country (entity)
    and year (time) fixed effects, country-clustered standard errors.
    This is the second of three estimators in the Step 01 Decision 3
    sequence (Pooled OLS -> FE -> RE + Hausman). FE is the headline
    estimator: it absorbs time-invariant country characteristics and
    common year shocks, isolating within-country variation in the
    education-Gini relationship.

    Spec C adds the `mean_years_schooling × cluster_kmeans_k3`
    interaction (Step 01 Decision 4). The cluster main-effect dummies
    are time-invariant (each country has a single cluster from Phase
    04) and so get absorbed by EntityEffects; `drop_absorbed=True`
    silently drops them. Only the interaction terms remain identified -
    which is exactly the "does the within-country slope differ across
    development regimes" test we want.

Inputs:
    data/processed/panel_modelling.csv

Outputs:
    outputs/tables/phase05_s04_fe_results.csv
        Long-format with `estimator` column.
        Schema: estimator, spec, kind, variable, value, std_error,
                tstat, pvalue, ci_low, ci_high.
        kind in {"coef", "fit"}; fit rows: n_obs, n_countries,
        n_time_periods, rsquared, rsquared_within, rsquared_between,
        rsquared_overall, f_statistic, f_pvalue, f_statistic_robust,
        f_pvalue_robust.

Headline number:
    Spec A `mean_years_schooling` coefficient under FE - the
    within-country slope estimate after country and year shocks are
    absorbed. This is THE Phase 05 headline coefficient per
    kickoff §7.
"""

from __future__ import annotations

import random
import sys
from pathlib import Path

import numpy as np
import pandas as pd

# Reproducibility
SEED = 42
random.seed(SEED)
np.random.seed(SEED)

# Make `src` importable when run from anywhere under the project root.
_SCRIPT_DIR = Path(__file__).resolve().parent
_PROJECT_ROOT_GUESS = _SCRIPT_DIR.parent
if str(_PROJECT_ROOT_GUESS) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT_GUESS))

from src.io_utils import read_csv_with_encoding_fallback  # noqa: E402
from src.paths import find_project_root  # noqa: E402

from linearmodels import PanelOLS  # noqa: E402


# ----- Specifications ------------------------------------------------------

DEPENDENT: str = "gini"

SPEC_A_RHS: tuple[str, ...] = (
    "mean_years_schooling",
    "enrol_secondary",
    "log_gdp_per_capita_ppp",
    "log_population",
    "urban_population_pct",
)

SPEC_B_RHS: tuple[str, ...] = SPEC_A_RHS + (
    "agri_value_added_gdp",
    "manu_value_added_gdp",
    "services_value_added_gdp",
    "trade_openness",
    "gov_expenditure_gdp",
)

# Spec C listwise requirement: Spec A's RHS plus cluster_kmeans_k3.
# cluster_kmeans_k3 itself is not a separate regressor in the formula -
# it appears wrapped in C() and interacted with mean_years_schooling.
SPEC_C_LISTWISE: tuple[str, ...] = SPEC_A_RHS + ("cluster_kmeans_k3",)

# Spec C formula. Formulaic syntax:
#   `a * C(b)` expands to: a + C(b) + a:C(b)
#   - a (continuous, identified)
#   - C(b)[T.k] dummies (time-invariant -> absorbed by EntityEffects)
#   - a:C(b)[T.k] interactions (time-varying via a, identified)
# `drop_absorbed=True` lets the absorbed cluster dummies fall through
# silently rather than raising a singular-design error.
SPEC_C_FORMULA: str = (
    "gini ~ 1 "
    "+ mean_years_schooling * C(cluster_kmeans_k3) "
    "+ enrol_secondary "
    "+ log_gdp_per_capita_ppp "
    "+ log_population "
    "+ urban_population_pct "
    "+ EntityEffects + TimeEffects"
)

ESTIMATOR_LABEL: str = "FE"
HEADLINE_VAR: str = "mean_years_schooling"


# ----- Helpers -------------------------------------------------------------


def _build_simple_formula(rhs: tuple[str, ...]) -> str:
    """Formula for Spec A and B: 1 + RHS + entity FE + year FE."""
    return (
        f"{DEPENDENT} ~ 1 + "
        + " + ".join(rhs)
        + " + EntityEffects + TimeEffects"
    )


def fit_panel_ols(
    panel_indexed: pd.DataFrame,
    formula: str,
    listwise_cols: tuple[str, ...],
    *,
    cast_cluster_to_int: bool,
):
    """Fit PanelOLS with country + year FE and country-clustered SE.

    Parameters
    ----------
    panel_indexed
        Panel with MultiIndex (iso3, year), sorted.
    formula
        linearmodels formulaic-style formula string. Must include
        EntityEffects and/or TimeEffects.
    listwise_cols
        Columns required to be non-NaN. Typically [DEPENDENT] + RHS,
        plus cluster_kmeans_k3 for Spec C.
    cast_cluster_to_int
        If True, cast cluster_kmeans_k3 to int after dropna, so that
        formulaic's C(...) labels render as [T.1] rather than [T.1.0].
    """
    df_listwise = panel_indexed.dropna(subset=list(listwise_cols)).copy()
    if cast_cluster_to_int and "cluster_kmeans_k3" in df_listwise.columns:
        df_listwise["cluster_kmeans_k3"] = (
            df_listwise["cluster_kmeans_k3"].astype(int)
        )

    mod = PanelOLS.from_formula(
        formula,
        data=df_listwise,
        drop_absorbed=True,
    )
    res = mod.fit(cov_type="clustered", cluster_entity=True)

    n_obs = int(res.nobs)
    n_countries = int(df_listwise.index.get_level_values("iso3").nunique())
    n_time_periods = int(df_listwise.index.get_level_values("year").nunique())
    return res, n_obs, n_countries, n_time_periods


def results_to_long_records(
    res,
    n_obs: int,
    n_countries: int,
    n_time_periods: int,
    spec_name: str,
) -> list[dict]:
    """Extract coefficients and fit-stats into long-format records."""
    records: list[dict] = []

    params = res.params
    std_errors = res.std_errors
    tstats = res.tstats
    pvalues = res.pvalues
    ci = res.conf_int()

    for var in params.index:
        records.append(
            {
                "estimator": ESTIMATOR_LABEL,
                "spec": spec_name,
                "kind": "coef",
                "variable": var,
                "value": float(params[var]),
                "std_error": float(std_errors[var]),
                "tstat": float(tstats[var]),
                "pvalue": float(pvalues[var]),
                "ci_low": float(ci.loc[var, "lower"]),
                "ci_high": float(ci.loc[var, "upper"]),
            }
        )

    f_stat = res.f_statistic
    f_stat_robust = res.f_statistic_robust
    fit_rows: list[tuple[str, float]] = [
        ("n_obs", float(n_obs)),
        ("n_countries", float(n_countries)),
        ("n_time_periods", float(n_time_periods)),
        ("rsquared", float(res.rsquared)),
        ("rsquared_within", float(res.rsquared_within)),
        ("rsquared_between", float(res.rsquared_between)),
        ("rsquared_overall", float(res.rsquared_overall)),
        ("f_statistic", float(f_stat.stat)),
        ("f_pvalue", float(f_stat.pval)),
        ("f_statistic_robust", float(f_stat_robust.stat)),
        ("f_pvalue_robust", float(f_stat_robust.pval)),
    ]
    for name, value in fit_rows:
        records.append(
            {
                "estimator": ESTIMATOR_LABEL,
                "spec": spec_name,
                "kind": "fit",
                "variable": name,
                "value": value,
                "std_error": np.nan,
                "tstat": np.nan,
                "pvalue": np.nan,
                "ci_low": np.nan,
                "ci_high": np.nan,
            }
        )

    return records


def _sig_stars(p: float) -> str:
    if p < 0.001:
        return "***"
    if p < 0.01:
        return "**"
    if p < 0.05:
        return "*"
    if p < 0.10:
        return "."
    return ""


def print_spec_summary(
    spec_name: str,
    res,
    n_obs: int,
    n_countries: int,
    n_time_periods: int,
) -> None:
    """Compact stdout summary; includes interaction terms for Spec C."""
    print(f"  N (country-years): {n_obs:,}")
    print(f"  N (countries):     {n_countries:,}")
    print(f"  N (years):         {n_time_periods:,}")
    print(f"  R² within:         {res.rsquared_within:+.4f}  ← FE headline")
    print(f"  R² overall:        {res.rsquared_overall:+.4f}")
    print(f"  R² between:        {res.rsquared_between:+.4f}")
    print(f"  F-stat (robust):   {float(res.f_statistic_robust.stat):.2f}  "
          f"(p={float(res.f_statistic_robust.pval):.3g})")

    if HEADLINE_VAR in res.params.index:
        coef = float(res.params[HEADLINE_VAR])
        se = float(res.std_errors[HEADLINE_VAR])
        p = float(res.pvalues[HEADLINE_VAR])
        ci = res.conf_int().loc[HEADLINE_VAR]
        stars = _sig_stars(p)
        print(
            f"  {HEADLINE_VAR}: {coef:+.4f}{stars}  "
            f"(SE {se:.4f}, p={p:.4f}, "
            f"95% CI [{float(ci['lower']):+.4f}, {float(ci['upper']):+.4f}])"
        )

    # Match interaction terms in either operand order:
    #   "mean_years_schooling:C(cluster_kmeans_k3)[T.k]"
    #   "C(cluster_kmeans_k3)[T.k]:mean_years_schooling"
    interaction_vars = [
        v for v in res.params.index
        if (v.startswith(HEADLINE_VAR + ":") or v.endswith(":" + HEADLINE_VAR))
        and "cluster_kmeans_k3" in v
    ]
    if interaction_vars:
        print(f"  Interaction terms ({len(interaction_vars)}):")
        for var in interaction_vars:
            coef = float(res.params[var])
            se = float(res.std_errors[var])
            p = float(res.pvalues[var])
            stars = _sig_stars(p)
            print(f"    {var}: {coef:+.4f}{stars}  "
                  f"(SE {se:.4f}, p={p:.4f})")


# ----- Main ---------------------------------------------------------------


def main() -> None:
    project_root = find_project_root()
    panel_path = project_root / "data" / "processed" / "panel_modelling.csv"
    output_path = (
        project_root / "outputs" / "tables" / "phase05_s04_fe_results.csv"
    )

    print(f"[INFO] Project root: {project_root}")
    print(f"[INFO] Input:        {panel_path}")
    print(f"[INFO] Output:       {output_path}")
    print()

    # ---------- Load --------------------------------------------------------

    panel, enc = read_csv_with_encoding_fallback(panel_path)
    print(
        f"[LOAD] panel_modelling.csv ({enc}): "
        f"{panel.shape[0]:,} rows x {panel.shape[1]} cols"
    )

    needed_cols: set[str] = {DEPENDENT}
    needed_cols.update(SPEC_A_RHS)
    needed_cols.update(SPEC_B_RHS)
    needed_cols.update(SPEC_C_LISTWISE)
    missing = needed_cols - set(panel.columns)
    if missing:
        raise KeyError(
            f"panel_modelling.csv missing required columns: {sorted(missing)}"
        )

    if not pd.api.types.is_integer_dtype(panel["year"]):
        panel["year"] = panel["year"].astype(int)
    panel_indexed = panel.set_index(["iso3", "year"]).sort_index()

    # ---------- Fit each specification --------------------------------------

    spec_configs = [
        ("A", _build_simple_formula(SPEC_A_RHS),
         (DEPENDENT, *SPEC_A_RHS), False),
        ("B", _build_simple_formula(SPEC_B_RHS),
         (DEPENDENT, *SPEC_B_RHS), False),
        ("C", SPEC_C_FORMULA,
         (DEPENDENT, *SPEC_C_LISTWISE), True),
    ]

    all_records: list[dict] = []
    for spec_name, formula, listwise_cols, cast_cluster in spec_configs:
        print()
        print(
            f"[FIT] Spec {spec_name} - PanelOLS, country + year FE, "
            f"country-clustered SE"
        )
        print(f"  Formula: {formula}")
        print(f"  Listwise on: {list(listwise_cols)}")
        res, n_obs, n_countries, n_time_periods = fit_panel_ols(
            panel_indexed,
            formula,
            listwise_cols,
            cast_cluster_to_int=cast_cluster,
        )
        print_spec_summary(
            spec_name, res, n_obs, n_countries, n_time_periods
        )
        all_records.extend(
            results_to_long_records(
                res, n_obs, n_countries, n_time_periods, spec_name
            )
        )

    # ---------- Write long-format CSV --------------------------------------

    out_df = pd.DataFrame(all_records)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(output_path, index=False, encoding="utf-8")

    n_coef_rows = int((out_df["kind"] == "coef").sum())
    n_fit_rows = int((out_df["kind"] == "fit").sum())

    print()
    print(
        f"[OK] Wrote {output_path.name}: "
        f"{len(out_df):,} rows ({n_coef_rows} coef + {n_fit_rows} fit)"
    )
    print(f"[OK] Specs fitted: A, B, C  (estimator='{ESTIMATOR_LABEL}')")


if __name__ == "__main__":
    main()
