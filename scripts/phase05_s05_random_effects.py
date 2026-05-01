"""
Phase 05 - Step 05: Random Effects + Hausman test.

Purpose:
    Fit Spec A and Spec B under `linearmodels.RandomEffects` with
    country-clustered SE, then compute the Hausman test against the
    two-way FE results from Step 04. RE is the third estimator in the
    Step 01 Decision 3 sequence; Hausman determines whether the
    country-specific effects are correlated with the regressors.

Design choices:
    1. RE specifications include `C(year)` as explicit regressors so
       that the time-shock control matches Step 04's TimeEffects.
       (`RandomEffects` has no special TimeEffects variable.)
    2. The Hausman test compares only the time-varying slope
       coefficients (the RHS of Spec A / Spec B). Intercept and year
       dummies are excluded - their interpretations differ across
       estimators.
    3. The covariance-difference matrix `Σ_FE - Σ_RE` is positive-
       semidefinite under H0 but often not strictly PD in finite
       samples with clustered SEs. Falls back to the Moore-Penrose
       pseudoinverse with df = matrix_rank when PD fails (standard
       practice; flagged in stdout).
    4. Spec C is intentionally excluded from RE/Hausman: its
       interaction terms complicate the slope-coefficient comparison
       and are not part of the headline FE-vs-RE choice.

Inputs:
    data/processed/panel_modelling.csv

Outputs:
    outputs/tables/phase05_s05_re_results.csv
        Long-format. Schema matches Step 04. estimator='RE'.
        Year-dummy coefficients are NOT stored (output cleanliness;
        they are nuisance regressors for the Hausman comparison).
    outputs/tables/phase05_s05_hausman_test.csv
        Schema: spec, statistic, df, pvalue, conclusion,
                inversion_method.
"""

from __future__ import annotations

import random
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats as sp_stats

# Reproducibility
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

SPEC_B_RHS: tuple[str, ...] = SPEC_A_RHS + (
    "agri_value_added_gdp",
    "manu_value_added_gdp",
    "services_value_added_gdp",
    "trade_openness",
    "gov_expenditure_gdp",
)

SPECS_FOR_HAUSMAN: dict[str, tuple[str, ...]] = {
    "A": SPEC_A_RHS,
    "B": SPEC_B_RHS,
}

HEADLINE_VAR: str = "mean_years_schooling"


# ----- Formula builders ---------------------------------------------------


def _build_fe_formula(rhs: tuple[str, ...]) -> str:
    """Two-way FE formula (matches Step 04 exactly for Spec A and B)."""
    return (
        f"{DEPENDENT} ~ 1 + "
        + " + ".join(rhs)
        + " + EntityEffects + TimeEffects"
    )


def _build_re_formula(rhs: tuple[str, ...]) -> str:
    """RE formula with year dummies as explicit regressors.

    `RandomEffects` has no TimeEffects special variable; year shocks
    must enter as `C(year)` to match the FE side's time control.
    """
    return (
        f"{DEPENDENT} ~ 1 + "
        + " + ".join(rhs)
        + " + C(year_factor)"
    )


# ----- Fitters ------------------------------------------------------------


def fit_fe(panel_indexed: pd.DataFrame, rhs: tuple[str, ...]):
    """Re-fit Step 04 two-way FE for Hausman comparison."""
    needed = [DEPENDENT, *rhs]
    df = panel_indexed.dropna(subset=needed)
    formula = _build_fe_formula(rhs)
    mod = PanelOLS.from_formula(formula, data=df, drop_absorbed=True)
    res = mod.fit(cov_type="clustered", cluster_entity=True)
    return res, int(res.nobs), int(df.index.get_level_values("iso3").nunique())


def fit_re(panel_indexed: pd.DataFrame, rhs: tuple[str, ...]):
    """Fit RandomEffects with year dummies as explicit regressors.

    `year_factor` column must exist on `panel_indexed`; main() adds it
    before calling. Listwise on RHS only - year_factor is never null.
    """
    needed = [DEPENDENT, *rhs]
    df = panel_indexed.dropna(subset=needed)
    formula = _build_re_formula(rhs)
    mod = RandomEffects.from_formula(formula, data=df)
    res = mod.fit(cov_type="clustered", cluster_entity=True)
    return res, int(res.nobs), int(df.index.get_level_values("iso3").nunique())


# ----- Hausman -----------------------------------------------------------


def hausman_test(
    res_fe,
    res_re,
    common_vars: tuple[str, ...],
    *,
    pd_tol: float = 1e-10,
) -> dict:
    """Compute the Hausman test on common slope coefficients.

    H = (β_FE - β_RE)' [Σ_FE - Σ_RE]^(-1) (β_FE - β_RE)
    Under H0: H ~ χ²(k) where k = #common time-varying coefficients.

    Falls back to Moore-Penrose pseudoinverse when the covariance
    difference is not strictly PD; df becomes matrix_rank(diff_cov)
    in that branch.
    """
    missing_fe = [v for v in common_vars if v not in res_fe.params.index]
    missing_re = [v for v in common_vars if v not in res_re.params.index]
    if missing_fe or missing_re:
        raise KeyError(
            f"Common vars not present - FE missing {missing_fe}, "
            f"RE missing {missing_re}"
        )

    beta_fe = res_fe.params.loc[list(common_vars)].to_numpy()
    beta_re = res_re.params.loc[list(common_vars)].to_numpy()
    cov_fe = res_fe.cov.loc[list(common_vars), list(common_vars)].to_numpy()
    cov_re = res_re.cov.loc[list(common_vars), list(common_vars)].to_numpy()

    diff = beta_fe - beta_re
    cov_diff = cov_fe - cov_re

    # Symmetrise to suppress numerical asymmetry from clustered SE.
    cov_diff = (cov_diff + cov_diff.T) / 2.0

    eigvals = np.linalg.eigvalsh(cov_diff)
    is_pd = bool(np.all(eigvals > pd_tol))

    if is_pd:
        inv_cov_diff = np.linalg.inv(cov_diff)
        df = len(common_vars)
        inversion_method = "inv"
    else:
        inv_cov_diff = np.linalg.pinv(cov_diff, hermitian=True)
        df = int(np.linalg.matrix_rank(cov_diff, tol=pd_tol))
        inversion_method = "pinv"

    statistic = float(diff @ inv_cov_diff @ diff)
    if df > 0:
        pvalue = float(1.0 - sp_stats.chi2.cdf(statistic, df=df))
    else:
        pvalue = float("nan")

    if not np.isfinite(statistic) or statistic < 0:
        # Negative test statistic indicates severe non-PD; report but flag.
        conclusion = "Inconclusive (negative or non-finite statistic)"
    elif df == 0 or not np.isfinite(pvalue):
        conclusion = "Inconclusive (degenerate covariance difference)"
    elif pvalue < 0.05:
        conclusion = "Reject H0 at 5%: prefer FE"
    elif pvalue < 0.10:
        conclusion = "Reject H0 at 10% only: borderline; lean FE"
    else:
        conclusion = "Fail to reject H0: RE is consistent and efficient"

    return {
        "statistic": statistic,
        "df": df,
        "pvalue": pvalue,
        "conclusion": conclusion,
        "inversion_method": inversion_method,
        "eigvals_min": float(eigvals.min()),
        "eigvals_max": float(eigvals.max()),
        "common_vars": list(common_vars),
        "beta_diff": diff,
    }


# ----- Long-format records (mirrors Step 04 schema) ----------------------


def re_results_to_long_records(
    res,
    n_obs: int,
    n_countries: int,
    n_time_periods: int,
    spec_name: str,
    keep_vars_only: tuple[str, ...],
) -> list[dict]:
    """Filter out year-dummy coefficients before serialising."""
    records: list[dict] = []
    params = res.params
    std_errors = res.std_errors
    tstats = res.tstats
    pvalues = res.pvalues
    ci = res.conf_int()

    keep_set = set(keep_vars_only)
    for var in params.index:
        if var.startswith("C(year_factor)"):
            continue
        if var not in keep_set and var != "Intercept":
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
            # `theta` is typically a Series indexed by entity; take mean
            # for unbalanced panels. If it's a scalar, this still works.
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


# ----- Stdout helpers -----------------------------------------------------


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


def print_re_summary(spec_name: str, res, n_obs: int, n_countries: int) -> None:
    print(f"  N (country-years): {n_obs:,}")
    print(f"  N (countries):     {n_countries:,}")
    print(f"  R² overall:        {res.rsquared_overall:+.4f}")
    print(f"  R² within:         {res.rsquared_within:+.4f}")
    print(f"  R² between:        {res.rsquared_between:+.4f}")
    theta_attr = getattr(res, "theta", None)
    if theta_attr is not None:
        try:
            theta_mean = float(np.asarray(theta_attr).astype(float).mean())
            print(f"  theta (mean):      {theta_mean:.4f}  "
                  f"(0=Pooled OLS, 1=FE)")
        except (TypeError, ValueError):
            pass

    if HEADLINE_VAR in res.params.index:
        coef = float(res.params[HEADLINE_VAR])
        se = float(res.std_errors[HEADLINE_VAR])
        p = float(res.pvalues[HEADLINE_VAR])
        ci = res.conf_int().loc[HEADLINE_VAR]
        stars = _sig_stars(p)
        print(
            f"  {HEADLINE_VAR}: {coef:+.4f}{stars}  "
            f"(SE {se:.4f}, p={p:.4f}, "
            f"95% CI [{float(ci['lower']):+.4f}, "
            f"{float(ci['upper']):+.4f}])"
        )


def print_hausman_summary(spec_name: str, res_fe, res_re, h: dict) -> None:
    print(f"[HAUSMAN] Spec {spec_name}")
    print(f"  Common time-varying vars ({len(h['common_vars'])}): "
          f"{h['common_vars']}")
    print(f"  Largest |β_FE - β_RE|: ", end="")
    diffs = h["beta_diff"]
    abs_diffs = np.abs(diffs)
    idx_max = int(np.argmax(abs_diffs))
    var_max = h["common_vars"][idx_max]
    fe_v = float(res_fe.params[var_max])
    re_v = float(res_re.params[var_max])
    print(f"{var_max} ({fe_v:+.4f} - {re_v:+.4f} = {diffs[idx_max]:+.4f})")
    print(f"  cov_diff eigenvalues: "
          f"min={h['eigvals_min']:+.4g}, max={h['eigvals_max']:+.4g}")
    print(f"  Inversion:         {h['inversion_method']}")
    print(f"  Hausman statistic: {h['statistic']:.4f}")
    print(f"  df:                {h['df']}")
    print(f"  p-value:           {h['pvalue']:.4f}")
    print(f"  Conclusion:        {h['conclusion']}")


# ----- Main ---------------------------------------------------------------


def main() -> None:
    project_root = find_project_root()
    panel_path = project_root / "data" / "processed" / "panel_modelling.csv"
    re_output = (
        project_root / "outputs" / "tables" / "phase05_s05_re_results.csv"
    )
    hausman_output = (
        project_root / "outputs" / "tables" / "phase05_s05_hausman_test.csv"
    )

    print(f"[INFO] Project root:    {project_root}")
    print(f"[INFO] Input:           {panel_path}")
    print(f"[INFO] RE output:       {re_output}")
    print(f"[INFO] Hausman output:  {hausman_output}")
    print()

    panel, enc = read_csv_with_encoding_fallback(panel_path)
    print(
        f"[LOAD] panel_modelling.csv ({enc}): "
        f"{panel.shape[0]:,} rows x {panel.shape[1]} cols"
    )

    if not pd.api.types.is_integer_dtype(panel["year"]):
        panel["year"] = panel["year"].astype(int)

    # Year-as-regressor for the RE side. After set_index moves `year`
    # into the MultiIndex, formulaic can't reach it via `C(year)` from
    # the column space - so we materialise a duplicate column.
    panel["year_factor"] = panel["year"].astype("category")

    panel_indexed = panel.set_index(["iso3", "year"]).sort_index()

    re_records: list[dict] = []
    hausman_records: list[dict] = []

    for spec_name, rhs in SPECS_FOR_HAUSMAN.items():
        print()
        print(f"[FIT] Spec {spec_name} - re-fitting two-way FE for Hausman...")
        res_fe, n_fe, _ = fit_fe(panel_indexed, rhs)
        if HEADLINE_VAR in res_fe.params.index:
            coef_fe = float(res_fe.params[HEADLINE_VAR])
            print(f"  FE  {HEADLINE_VAR}: {coef_fe:+.4f}  (N={n_fe:,})")

        print(
            f"[FIT] Spec {spec_name} - RandomEffects with year dummies, "
            f"country-clustered SE"
        )
        res_re, n_re, n_countries_re = fit_re(panel_indexed, rhs)
        n_time_periods_re = int(
            panel_indexed.dropna(
                subset=[DEPENDENT, *rhs]
            ).index.get_level_values("year").nunique()
        )
        print_re_summary(spec_name, res_re, n_re, n_countries_re)

        if n_fe != n_re:
            print(
                f"[WARN] FE and RE sample sizes differ: "
                f"FE N={n_fe}, RE N={n_re}. "
                f"Hausman comparison should use identical samples."
            )

        re_records.extend(
            re_results_to_long_records(
                res_re, n_re, n_countries_re, n_time_periods_re,
                spec_name, rhs,
            )
        )

        print()
        h = hausman_test(res_fe, res_re, rhs)
        print_hausman_summary(spec_name, res_fe, res_re, h)

        hausman_records.append({
            "spec": spec_name,
            "statistic": h["statistic"],
            "df": h["df"],
            "pvalue": h["pvalue"],
            "conclusion": h["conclusion"],
            "inversion_method": h["inversion_method"],
            "eigvals_min": h["eigvals_min"],
            "eigvals_max": h["eigvals_max"],
            "n_common_vars": len(h["common_vars"]),
        })

    re_df = pd.DataFrame(re_records)
    re_output.parent.mkdir(parents=True, exist_ok=True)
    re_df.to_csv(re_output, index=False, encoding="utf-8")

    hausman_df = pd.DataFrame(hausman_records)
    hausman_df.to_csv(hausman_output, index=False, encoding="utf-8")

    print()
    n_coef = int((re_df["kind"] == "coef").sum())
    n_fit = int((re_df["kind"] == "fit").sum())
    print(
        f"[OK] Wrote {re_output.name}: "
        f"{len(re_df):,} rows ({n_coef} coef + {n_fit} fit)"
    )
    print(f"[OK] Wrote {hausman_output.name}: {len(hausman_df):,} rows")


if __name__ == "__main__":
    main()
