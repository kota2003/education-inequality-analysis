"""
Phase 05 - Step 03: Pooled OLS baselines.

Purpose:
    Fit the Phase 05 Spec A (parsimonious) and Spec B (full controls)
    via `linearmodels.PooledOLS` with country-clustered standard errors
    (HC heteroscedasticity- and within-country-correlation-robust). This
    is the first of three estimators in the Step 01 Decision 3 sequence
    (Pooled OLS -> FE -> RE + Hausman); each subsequent step shares the
    `linearmodels` comparison API so that Step 08 can render
    OLS/FE/RE side-by-side via `linearmodels.compare()` (Decision 8).

    Use of `linearmodels.PooledOLS` rather than `statsmodels.OLS` is
    deliberate: kickoff §5 Step 03 mentioned `statsmodels`, but the
    locked Step 01 Decision 3 specifies `linearmodels.PooledOLS` for
    API uniformity. Locked decisions take precedence over the proposed
    plan (PROJECT_WORKFLOW §1 Precedence rule).

Inputs:
    data/processed/panel_modelling.csv

Outputs:
    outputs/tables/phase05_s03_ols_results.csv
        Long-format coefficient + fit-statistics table.
        Schema: spec, kind, variable, value, std_error, tstat,
                pvalue, ci_low, ci_high.
        kind in {"coef", "fit"}; fit rows carry only `value`
        (n_obs, n_countries, rsquared, rsquared_within,
        rsquared_between, rsquared_overall, f_statistic, f_pvalue).

Headline number to inspect in stdout:
    Spec A `mean_years_schooling` coefficient under Pooled OLS - the
    naive (panel-structure-ignoring) baseline against which the
    Step 04 FE coefficient is compared.
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

from linearmodels import PooledOLS  # noqa: E402


# ----- Specifications ------------------------------------------------------

DEPENDENT: str = "gini"

# Spec A (parsimonious) - Step 01 Decision 1.
SPEC_A_RHS: tuple[str, ...] = (
    "mean_years_schooling",
    "enrol_secondary",
    "log_gdp_per_capita_ppp",
    "log_population",
    "urban_population_pct",
)

# Spec B (full controls) - Step 01 Decision 2.
# Spec A + sector trio + trade_openness + gov_expenditure_gdp.
SPEC_B_RHS: tuple[str, ...] = SPEC_A_RHS + (
    "agri_value_added_gdp",
    "manu_value_added_gdp",
    "services_value_added_gdp",
    "trade_openness",
    "gov_expenditure_gdp",
)

SPECS: dict[str, tuple[str, ...]] = {
    "A": SPEC_A_RHS,
    "B": SPEC_B_RHS,
}

# Headline coefficient in Phase 05 narrative (kickoff §7).
HEADLINE_VAR: str = "mean_years_schooling"


# ----- Helpers -------------------------------------------------------------


def fit_pooled_ols(
    panel_indexed: pd.DataFrame,
    rhs: tuple[str, ...],
):
    """Fit Pooled OLS for one specification with country-clustered SE.

    Returns
    -------
    res : linearmodels PanelResults
        Fitted result.
    n_obs : int
        Number of country-year observations after listwise deletion.
    n_countries : int
        Number of unique entities in the fit sample.
    """
    needed = [DEPENDENT, *rhs]
    df_listwise = panel_indexed.dropna(subset=needed)

    formula = f"{DEPENDENT} ~ 1 + " + " + ".join(rhs)
    mod = PooledOLS.from_formula(formula, data=df_listwise)
    res = mod.fit(cov_type="clustered", cluster_entity=True)

    n_obs = int(res.nobs)
    n_countries = int(df_listwise.index.get_level_values("iso3").nunique())
    return res, n_obs, n_countries


def results_to_long_records(
    res, n_obs: int, n_countries: int, spec_name: str
) -> list[dict]:
    """Extract a fit's coefficients and fit-stats into long-format records.

    R² components are linearmodels.PanelResults attributes:
    rsquared / rsquared_within / rsquared_between / rsquared_overall.
    For PooledOLS these are computable but the within/between split is
    primarily diagnostic; they become substantively important under FE
    (Step 04) where rsquared_within is the headline goodness-of-fit.
    """
    records: list[dict] = []

    params = res.params
    std_errors = res.std_errors
    tstats = res.tstats
    pvalues = res.pvalues
    ci = res.conf_int()  # columns: 'lower', 'upper'

    for var in params.index:
        records.append(
            {
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
    fit_rows: list[tuple[str, float]] = [
        ("n_obs", float(n_obs)),
        ("n_countries", float(n_countries)),
        ("rsquared", float(res.rsquared)),
        ("rsquared_within", float(res.rsquared_within)),
        ("rsquared_between", float(res.rsquared_between)),
        ("rsquared_overall", float(res.rsquared_overall)),
        ("f_statistic", float(f_stat.stat)),
        ("f_pvalue", float(f_stat.pval)),
    ]
    for name, value in fit_rows:
        records.append(
            {
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


def print_spec_summary(spec_name: str, res, n_obs: int, n_countries: int) -> None:
    """Print a compact summary of the fit, focusing on the headline coefficient."""
    print(f"  N (country-years): {n_obs:,}")
    print(f"  N (countries):     {n_countries:,}")
    print(f"  R² (overall):      {res.rsquared:.4f}")
    print(f"  R² within:         {res.rsquared_within:.4f}")
    print(f"  R² between:        {res.rsquared_between:.4f}")
    print(f"  F-stat:            {float(res.f_statistic.stat):.2f}  "
          f"(p={float(res.f_statistic.pval):.3g})")

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


# ----- Main ---------------------------------------------------------------


def main() -> None:
    project_root = find_project_root()
    panel_path = project_root / "data" / "processed" / "panel_modelling.csv"
    output_path = (
        project_root / "outputs" / "tables" / "phase05_s03_ols_results.csv"
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

    all_needed: set[str] = {DEPENDENT}
    for rhs in SPECS.values():
        all_needed.update(rhs)
    missing = all_needed - set(panel.columns)
    if missing:
        raise KeyError(
            f"panel_modelling.csv missing required columns: {sorted(missing)}"
        )

    # ---------- Set MultiIndex (entity, time) for linearmodels --------------

    if not pd.api.types.is_integer_dtype(panel["year"]):
        panel["year"] = panel["year"].astype(int)
    panel_indexed = panel.set_index(["iso3", "year"]).sort_index()

    # ---------- Fit each specification --------------------------------------

    all_records: list[dict] = []
    for spec_name, rhs in SPECS.items():
        print()
        print(
            f"[FIT] Spec {spec_name} - Pooled OLS, country-clustered SE  "
            f"({len(rhs)} RHS variables)"
        )
        print(f"  RHS: {list(rhs)}")
        res, n_obs, n_countries = fit_pooled_ols(panel_indexed, rhs)
        print_spec_summary(spec_name, res, n_obs, n_countries)
        all_records.extend(
            results_to_long_records(res, n_obs, n_countries, spec_name)
        )

    # ---------- Write long-format CSV --------------------------------------

    out_df = pd.DataFrame(all_records)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(output_path, index=False, encoding="utf-8")

    n_coef_rows = int((out_df["kind"] == "coef").sum())
    n_fit_rows = int((out_df["kind"] == "fit").sum())

    print()
    print(f"[OK] Wrote {output_path.name}: "
          f"{len(out_df):,} rows ({n_coef_rows} coef + {n_fit_rows} fit)")
    print(f"[OK] Specs fitted: {list(SPECS.keys())}")


if __name__ == "__main__":
    main()
