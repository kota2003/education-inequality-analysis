"""
Phase 05 - Step 06: Heterogeneity (Spec C, mys x cluster_kmeans_k3).

Purpose:
    Synthesise per-cluster within-country slopes of `mean_years_schooling`
    on `gini` from the Spec C interaction model. Spec C is:

        gini ~ 1 + mean_years_schooling * C(cluster_kmeans_k3)
               + enrol_secondary + log_gdp_per_capita_ppp
               + log_population + urban_population_pct
               + (FE: EntityEffects + TimeEffects |
                  RE: C(year_factor))

    Per-cluster slopes are linear combinations of the fitted parameters:

        slope_cluster0 = beta_mys
        slope_cluster1 = beta_mys + beta_{mys:C(...)[T.1]}
        slope_cluster2 = beta_mys + beta_{mys:C(...)[T.2]}

    Standard errors come from the delta method on each estimator's
    coefficient covariance matrix:

        Var(slope_k) = Var(beta_mys) + Var(beta_int_k)
                       + 2 * Cov(beta_mys, beta_int_k)
        SE(slope_k)  = sqrt(Var(slope_k))

    The FE Spec C fit is loaded from Step 04 output and re-instantiated
    here; the RE Spec C fit is computed fresh, mirroring the Step 05
    `C(year_factor)` time-control choice. The Phase 05 narrative
    question this answers: "Does the education-Gini relationship vary
    across the K=3 development regimes from Phase 04?"

Inputs:
    data/processed/panel_modelling.csv
    outputs/tables/phase05_s04_fe_results.csv  (Spec C FE coefficients)

Outputs:
    outputs/tables/phase05_s06_heterogeneity_results.csv
        RE Spec C coefficient + fit table. Schema matches Step 04/05.
        Year dummies excluded.
    outputs/tables/phase05_s06_per_cluster_slopes.csv
        Per-cluster slope estimates for FE and RE.
        Schema: estimator, cluster, slope, std_error, tstat, pvalue,
                ci_low, ci_high, n_obs.
"""

from __future__ import annotations

import random
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats as sp_stats

SEED = 42
random.seed(SEED)
np.random.seed(SEED)

_SCRIPT_DIR = Path(__file__).resolve().parent
_PROJECT_ROOT_GUESS = _SCRIPT_DIR.parent
if str(_PROJECT_ROOT_GUESS) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT_GUESS))

from src.io_utils import read_csv_with_encoding_fallback  # noqa: E402
from src.paths import find_project_root  # noqa: E402

from linearmodels import PanelOLS, RandomEffects  # noqa: E402


# ----- Specifications ------------------------------------------------------

DEPENDENT: str = "gini"

SPEC_A_RHS: tuple[str, ...] = (
    "mean_years_schooling",
    "enrol_secondary",
    "log_gdp_per_capita_ppp",
    "log_population",
    "urban_population_pct",
)

SPEC_C_LISTWISE: tuple[str, ...] = SPEC_A_RHS + ("cluster_kmeans_k3",)

SPEC_C_FE_FORMULA: str = (
    "gini ~ 1 "
    "+ mean_years_schooling * C(cluster_kmeans_k3) "
    "+ enrol_secondary "
    "+ log_gdp_per_capita_ppp "
    "+ log_population "
    "+ urban_population_pct "
    "+ EntityEffects + TimeEffects"
)

SPEC_C_RE_FORMULA: str = (
    "gini ~ 1 "
    "+ mean_years_schooling * C(cluster_kmeans_k3) "
    "+ enrol_secondary "
    "+ log_gdp_per_capita_ppp "
    "+ log_population "
    "+ urban_population_pct "
    "+ C(year_factor)"
)

HEADLINE_VAR: str = "mean_years_schooling"
INTERACTION_PREFIX: str = "mean_years_schooling:C(cluster_kmeans_k3)[T."
INTERACTION_PREFIX_REVERSED: str = "C(cluster_kmeans_k3)[T."  # ":mean_years_schooling"


# ----- Helpers ------------------------------------------------------------


def _find_interaction_var(param_names, cluster_id: int) -> str | None:
    """Locate the interaction term for cluster k in either operand order."""
    target_a = f"{INTERACTION_PREFIX}{cluster_id}]"  # mys:C(...)[T.k]
    target_b = (
        f"{INTERACTION_PREFIX_REVERSED}{cluster_id}]:{HEADLINE_VAR}"
    )
    for v in param_names:
        if v == target_a or v == target_b:
            return v
    return None


def fit_fe_spec_c(panel_indexed: pd.DataFrame):
    """Re-fit Step 04 Spec C FE for delta-method SE computation."""
    df = panel_indexed.dropna(subset=list(SPEC_C_LISTWISE)).copy()
    df["cluster_kmeans_k3"] = df["cluster_kmeans_k3"].astype(int)
    mod = PanelOLS.from_formula(SPEC_C_FE_FORMULA, data=df, drop_absorbed=True)
    res = mod.fit(cov_type="clustered", cluster_entity=True)
    return res, int(res.nobs), int(df.index.get_level_values("iso3").nunique())


def fit_re_spec_c(panel_indexed: pd.DataFrame):
    """Fit Spec C RE with year dummies as explicit regressors."""
    df = panel_indexed.dropna(subset=list(SPEC_C_LISTWISE)).copy()
    df["cluster_kmeans_k3"] = df["cluster_kmeans_k3"].astype(int)
    mod = RandomEffects.from_formula(SPEC_C_RE_FORMULA, data=df)
    res = mod.fit(cov_type="clustered", cluster_entity=True)
    return res, int(res.nobs), int(df.index.get_level_values("iso3").nunique())


def per_cluster_slopes(res, n_obs: int, estimator_label: str) -> list[dict]:
    """Compute per-cluster slope, SE, t-stat, p-value, CI via delta method.

    For cluster k, slope = beta_mys (+ beta_int_k for k != 0).
    Variance is read from the result's `cov` (DataFrame) attribute.
    Confidence intervals use the normal approximation; t-stat reported
    using residual df from `res.df_resid`.
    """
    params = res.params
    cov = res.cov  # DataFrame indexed by parameter name on both axes

    if HEADLINE_VAR not in params.index:
        raise KeyError(f"{HEADLINE_VAR} not found in params index.")

    beta_mys = float(params[HEADLINE_VAR])
    var_mys = float(cov.loc[HEADLINE_VAR, HEADLINE_VAR])

    df_resid = float(getattr(res, "df_resid", n_obs - len(params)))

    records: list[dict] = []
    for cluster_id in (0, 1, 2):
        if cluster_id == 0:
            slope = beta_mys
            variance = var_mys
            interaction_var = None
        else:
            interaction_var = _find_interaction_var(params.index, cluster_id)
            if interaction_var is None:
                # Should not happen if the formula expanded correctly.
                records.append({
                    "estimator": estimator_label,
                    "cluster": cluster_id,
                    "slope": float("nan"),
                    "std_error": float("nan"),
                    "tstat": float("nan"),
                    "pvalue": float("nan"),
                    "ci_low": float("nan"),
                    "ci_high": float("nan"),
                    "n_obs": float(n_obs),
                    "interaction_var": "MISSING",
                })
                continue
            beta_int = float(params[interaction_var])
            var_int = float(cov.loc[interaction_var, interaction_var])
            cov_term = float(cov.loc[HEADLINE_VAR, interaction_var])
            slope = beta_mys + beta_int
            variance = var_mys + var_int + 2.0 * cov_term

        if variance < 0:
            # Numerical instability under cluster SEs - flag but continue.
            std_error = float("nan")
            tstat = float("nan")
            pvalue = float("nan")
            ci_low = float("nan")
            ci_high = float("nan")
        else:
            std_error = float(np.sqrt(variance))
            tstat = slope / std_error if std_error > 0 else float("nan")
            if df_resid > 0 and np.isfinite(tstat):
                pvalue = float(
                    2.0 * (1.0 - sp_stats.t.cdf(abs(tstat), df=df_resid))
                )
                tcrit = float(sp_stats.t.ppf(0.975, df=df_resid))
            else:
                pvalue = float("nan")
                tcrit = 1.96
            ci_low = slope - tcrit * std_error
            ci_high = slope + tcrit * std_error

        records.append({
            "estimator": estimator_label,
            "cluster": cluster_id,
            "slope": float(slope),
            "std_error": float(std_error) if np.isfinite(std_error) else float("nan"),
            "tstat": float(tstat) if np.isfinite(tstat) else float("nan"),
            "pvalue": float(pvalue) if np.isfinite(pvalue) else float("nan"),
            "ci_low": float(ci_low) if np.isfinite(ci_low) else float("nan"),
            "ci_high": float(ci_high) if np.isfinite(ci_high) else float("nan"),
            "n_obs": float(n_obs),
            "interaction_var": interaction_var if interaction_var else "(baseline)",
        })

    return records


def re_results_to_long_records(
    res,
    n_obs: int,
    n_countries: int,
    n_time_periods: int,
    spec_name: str,
) -> list[dict]:
    """Long-format records for the Spec C RE fit; year dummies excluded."""
    records: list[dict] = []
    params = res.params
    std_errors = res.std_errors
    tstats = res.tstats
    pvalues = res.pvalues
    ci = res.conf_int()

    for var in params.index:
        if var.startswith("C(year_factor)"):
            continue
        records.append({
            "estimator": "RE",
            "spec": spec_name,
            "kind": "coef",
            "variable": var,
            "value": float(params[var]),
            "std_error": float(std_errors[var]),
            "tstat": float(tstats[var]),
            "pvalue": float(pvalues[var]),
            "ci_low": float(ci.loc[var, "lower"]),
            "ci_high": float(ci.loc[var, "upper"]),
        })

    theta_value: float
    theta_attr = getattr(res, "theta", None)
    if theta_attr is None:
        theta_value = float("nan")
    else:
        try:
            theta_value = float(np.asarray(theta_attr).astype(float).mean())
        except (TypeError, ValueError):
            theta_value = float("nan")

    fit_rows: list[tuple[str, float]] = [
        ("n_obs", float(n_obs)),
        ("n_countries", float(n_countries)),
        ("n_time_periods", float(n_time_periods)),
        ("rsquared", float(res.rsquared)),
        ("rsquared_within", float(res.rsquared_within)),
        ("rsquared_between", float(res.rsquared_between)),
        ("rsquared_overall", float(res.rsquared_overall)),
        ("theta_mean", theta_value),
    ]
    for name, value in fit_rows:
        records.append({
            "estimator": "RE",
            "spec": spec_name,
            "kind": "fit",
            "variable": name,
            "value": value,
            "std_error": np.nan,
            "tstat": np.nan,
            "pvalue": np.nan,
            "ci_low": np.nan,
            "ci_high": np.nan,
        })
    return records


def _sig_stars(p: float) -> str:
    if not np.isfinite(p):
        return ""
    if p < 0.001:
        return "***"
    if p < 0.01:
        return "**"
    if p < 0.05:
        return "*"
    if p < 0.10:
        return "."
    return ""


# ----- Stdout helpers -----------------------------------------------------


def print_per_cluster_table(estimator_label: str, records: list[dict]) -> None:
    print(f"  {'Cluster':<8} {'Slope':>10} {'SE':>9} {'p':>8}  {'95% CI':>22}")
    for r in records:
        slope = r["slope"]
        se = r["std_error"]
        p = r["pvalue"]
        stars = _sig_stars(p)
        if np.isfinite(r["ci_low"]) and np.isfinite(r["ci_high"]):
            ci_text = f"[{r['ci_low']:+.3f}, {r['ci_high']:+.3f}]"
        else:
            ci_text = "[--, --]"
        print(
            f"  {r['cluster']:<8} {slope:+10.4f} {se:>9.4f} {p:>7.4f}{stars}  "
            f"{ci_text}"
        )


# ----- Main ---------------------------------------------------------------


def main() -> None:
    project_root = find_project_root()
    panel_path = project_root / "data" / "processed" / "panel_modelling.csv"
    re_output = (
        project_root / "outputs" / "tables"
        / "phase05_s06_heterogeneity_results.csv"
    )
    slopes_output = (
        project_root / "outputs" / "tables"
        / "phase05_s06_per_cluster_slopes.csv"
    )

    print(f"[INFO] Project root:   {project_root}")
    print(f"[INFO] Input:          {panel_path}")
    print(f"[INFO] RE output:      {re_output}")
    print(f"[INFO] Slopes output:  {slopes_output}")
    print()

    panel, enc = read_csv_with_encoding_fallback(panel_path)
    print(
        f"[LOAD] panel_modelling.csv ({enc}): "
        f"{panel.shape[0]:,} rows x {panel.shape[1]} cols"
    )

    if not pd.api.types.is_integer_dtype(panel["year"]):
        panel["year"] = panel["year"].astype(int)
    panel["year_factor"] = panel["year"].astype("category")
    panel_indexed = panel.set_index(["iso3", "year"]).sort_index()

    # ---------- Spec C FE re-fit (matches Step 04 output) ------------------

    print()
    print("[FIT] Spec C FE (re-fit; matches Step 04 for delta-method SE)...")
    res_fe_c, n_fe_c, ncn_fe_c = fit_fe_spec_c(panel_indexed)
    print(f"  N (country-years): {n_fe_c:,}")
    print(f"  N (countries):     {ncn_fe_c:,}")
    if HEADLINE_VAR in res_fe_c.params.index:
        coef = float(res_fe_c.params[HEADLINE_VAR])
        print(f"  FE Spec C beta_mys (cluster 0 baseline): {coef:+.4f}")
        for k in (1, 2):
            ivar = _find_interaction_var(res_fe_c.params.index, k)
            if ivar:
                print(f"  FE Spec C interaction[T.{k}]: "
                      f"{float(res_fe_c.params[ivar]):+.4f}  ({ivar})")

    fe_slopes = per_cluster_slopes(res_fe_c, n_fe_c, "FE")
    print()
    print("[SLOPES] FE Spec C - per-cluster within-country slope of mys on gini:")
    print_per_cluster_table("FE", fe_slopes)

    # ---------- Spec C RE fit ---------------------------------------------

    print()
    print("[FIT] Spec C RE (year dummies as regressors, country-clustered SE)...")
    res_re_c, n_re_c, n_countries_re_c = fit_re_spec_c(panel_indexed)
    n_time_periods_re_c = int(
        panel_indexed.dropna(subset=list(SPEC_C_LISTWISE))
        .index.get_level_values("year").nunique()
    )
    print(f"  N (country-years): {n_re_c:,}")
    print(f"  N (countries):     {n_countries_re_c:,}")
    print(f"  R² overall:        {res_re_c.rsquared_overall:+.4f}")
    print(f"  R² within:         {res_re_c.rsquared_within:+.4f}")
    print(f"  R² between:        {res_re_c.rsquared_between:+.4f}")
    theta_attr = getattr(res_re_c, "theta", None)
    if theta_attr is not None:
        try:
            theta_mean = float(np.asarray(theta_attr).astype(float).mean())
            print(f"  theta (mean):      {theta_mean:.4f}")
        except (TypeError, ValueError):
            pass

    if HEADLINE_VAR in res_re_c.params.index:
        coef = float(res_re_c.params[HEADLINE_VAR])
        se = float(res_re_c.std_errors[HEADLINE_VAR])
        p = float(res_re_c.pvalues[HEADLINE_VAR])
        stars = _sig_stars(p)
        print(f"  RE Spec C beta_mys (cluster 0 baseline): "
              f"{coef:+.4f}{stars}  (SE {se:.4f}, p={p:.4f})")
        for k in (1, 2):
            ivar = _find_interaction_var(res_re_c.params.index, k)
            if ivar:
                ic = float(res_re_c.params[ivar])
                ise = float(res_re_c.std_errors[ivar])
                ip = float(res_re_c.pvalues[ivar])
                istars = _sig_stars(ip)
                print(f"  RE Spec C interaction[T.{k}]: "
                      f"{ic:+.4f}{istars}  (SE {ise:.4f}, p={ip:.4f})  ({ivar})")

    re_slopes = per_cluster_slopes(res_re_c, n_re_c, "RE")
    print()
    print("[SLOPES] RE Spec C - per-cluster within-country slope of mys on gini:")
    print_per_cluster_table("RE", re_slopes)

    # ---------- Outputs ---------------------------------------------------

    re_records = re_results_to_long_records(
        res_re_c, n_re_c, n_countries_re_c, n_time_periods_re_c, "C"
    )
    re_df = pd.DataFrame(re_records)
    re_output.parent.mkdir(parents=True, exist_ok=True)
    re_df.to_csv(re_output, index=False, encoding="utf-8")

    slopes_df = pd.DataFrame(fe_slopes + re_slopes)
    slopes_df.to_csv(slopes_output, index=False, encoding="utf-8")

    n_coef = int((re_df["kind"] == "coef").sum())
    n_fit = int((re_df["kind"] == "fit").sum())
    print()
    print(
        f"[OK] Wrote {re_output.name}: "
        f"{len(re_df):,} rows ({n_coef} coef + {n_fit} fit)"
    )
    print(f"[OK] Wrote {slopes_output.name}: {len(slopes_df):,} rows")


if __name__ == "__main__":
    main()
