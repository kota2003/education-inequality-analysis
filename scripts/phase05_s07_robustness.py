"""
Phase 05 - Step 07: Robustness checks.

Four checks:

1. Mundlak alternative-Hausman test (Spec A and B).
   Add country-mean of each time-varying RHS as auxiliary regressors
   in a RandomEffects model, then test whether their joint coefficient
   vector is zero. Asymptotically equivalent to the Hausman test but
   numerically stable under cluster-robust SE - which addressed Step 05's
   Spec B degeneracy (negative Hausman statistic from a non-PD covariance
   difference). Test result: reject H0 -> FE preferred; fail to reject ->
   RE consistent. Implemented as a manual Wald statistic on the
   subvector of country-mean coefficients (W = b' V^-1 b ~ chi^2(q)).

2. Boundary-case reassignment (Decision 6).
   BRA, ZAF, MEX, ARG sit on the Phase 04 K-means Cluster 1/2 boundary
   in PCA space. Re-fit Spec C RE with these four reassigned from
   Cluster 2 to Cluster 1, recompute per-cluster slopes via delta
   method, and compare to the baseline Step 06 RE Spec C result. Tests
   whether the headline heterogeneity finding (RE Spec C Cluster 1
   slope -1.19, p=0.010) survives algorithm-induced uncertainty in
   cluster assignment.

3. MNAR selection diagnostic (Decision 7).
   Compare the country-year sample where Spec A is listwise complete
   against the complement.

   Continuous variables (mean_years_schooling, log_gdp_per_capita_ppp,
   urban_population_pct) are tested at the country-year level via
   Welch t-test, Mann-Whitney U, and Kolmogorov-Smirnov.

   Categorical variables (region_name, income_level_name) are tested
   at the COUNTRY level: each of the 217 panel countries is classified
   as group A (>=1 country-year complete on Spec A) or group B (zero
   complete years), then chi-square tests whether the regional /
   income-group distribution differs. Country-year level is wrong here
   because these attributes are time-invariant per country - using
   country-year would inflate N by ~34 and bias p-values toward
   significance.

   Whitespace-stripped per Phase 03 §Known Issues. The merge from
   country metadata uses suffixes=("","_meta") so that pre-existing
   panel-side columns are preserved without silent collision.

4. Sub-period 2010-2019 (Decision 5).
   Re-fit Spec A under both FE and RE on year in [2010, 2019]. The
   period matches the Phase 04 clustering window. Tests whether the
   headline RE coefficient on mys (-0.69 from Step 05 Spec A on the
   full panel) holds up when restricted to the more recent decade.

Inputs:
    data/processed/panel_modelling.csv
    data/raw/world_bank/wb_country_metadata.csv

Outputs:
    outputs/tables/phase05_s07_robustness_results.csv
        Long-format table with `check` column identifying the check.
        Schema: check, subgroup, item_kind, item_name, value,
                std_error, statistic, df, pvalue, ci_low, ci_high,
                n_obs, n_countries, notes.
        Field population is heterogeneous across check types - see
        item_kind for what's relevant.
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

from src.country_metadata import load_country_metadata  # noqa: E402
from src.io_utils import read_csv_with_encoding_fallback  # noqa: E402
from src.paths import find_project_root  # noqa: E402

from linearmodels import PanelOLS, RandomEffects  # noqa: E402


# ----- Specifications (mirror Steps 03/04/05/06) -------------------------

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

SPEC_C_LISTWISE: tuple[str, ...] = SPEC_A_RHS + ("cluster_kmeans_k3",)

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

BOUNDARY_ISO3: tuple[str, ...] = ("BRA", "ZAF", "MEX", "ARG")

MNAR_CONTINUOUS_VARS: tuple[str, ...] = (
    "mean_years_schooling",
    "log_gdp_per_capita_ppp",
    "urban_population_pct",
)
MNAR_CATEGORICAL_VARS: tuple[str, ...] = ("region_name", "income_level_name")


# ----- Output schema helper ----------------------------------------------


def _empty_record(**overrides) -> dict:
    """Return a record with all schema keys; override populated fields."""
    base = {
        "check": np.nan,
        "subgroup": np.nan,
        "item_kind": np.nan,
        "item_name": np.nan,
        "value": np.nan,
        "std_error": np.nan,
        "statistic": np.nan,
        "df": np.nan,
        "pvalue": np.nan,
        "ci_low": np.nan,
        "ci_high": np.nan,
        "n_obs": np.nan,
        "n_countries": np.nan,
        "notes": "",
    }
    base.update(overrides)
    return base


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


def _find_interaction_var(param_names, cluster_id: int) -> str | None:
    target_a = f"{INTERACTION_PREFIX}{cluster_id}]"
    target_b = f"C(cluster_kmeans_k3)[T.{cluster_id}]:{HEADLINE_VAR}"
    for v in param_names:
        if v == target_a or v == target_b:
            return v
    return None


def _strip_whitespace_inplace(df: pd.DataFrame, cols: list[str]) -> None:
    """Strip leading/trailing whitespace; preserves NaN."""
    for c in cols:
        if c not in df.columns:
            continue
        mask = df[c].notna()
        if mask.any():
            df.loc[mask, c] = df.loc[mask, c].astype(str).str.strip()


# ----- Check 1: Mundlak --------------------------------------------------


def mundlak_check(panel_indexed: pd.DataFrame, spec_name: str,
                  rhs: tuple[str, ...]) -> list[dict]:
    needed = [DEPENDENT, *rhs]
    df = panel_indexed.dropna(subset=needed).copy()

    cmean_names: list[str] = []
    for var in rhs:
        cmean = df.groupby(level="iso3")[var].transform("mean")
        cname = f"{var}_cmean"
        df[cname] = cmean
        cmean_names.append(cname)

    formula = (
        f"{DEPENDENT} ~ 1 + "
        + " + ".join(rhs)
        + " + "
        + " + ".join(cmean_names)
        + " + C(year_factor)"
    )

    mod = RandomEffects.from_formula(formula, data=df)
    res = mod.fit(cov_type="clustered", cluster_entity=True)

    b = res.params.loc[cmean_names].to_numpy()
    V = res.cov.loc[cmean_names, cmean_names].to_numpy()
    V = (V + V.T) / 2.0
    eigvals = np.linalg.eigvalsh(V)
    pd_tol = 1e-12

    if np.all(eigvals > pd_tol):
        Vinv = np.linalg.inv(V)
        W = float(b @ Vinv @ b)
        df_test = len(cmean_names)
        inversion = "inv"
    else:
        Vinv = np.linalg.pinv(V, hermitian=True)
        W = float(b @ Vinv @ b)
        df_test = int(np.linalg.matrix_rank(V, tol=pd_tol))
        inversion = "pinv"

    pvalue = (
        float(1.0 - sp_stats.chi2.cdf(W, df=df_test))
        if df_test > 0 and np.isfinite(W) and W >= 0
        else float("nan")
    )

    if not np.isfinite(W) or W < 0 or not np.isfinite(pvalue):
        conclusion = "Inconclusive (degenerate)"
    elif pvalue < 0.05:
        conclusion = "Reject H0 at 5%: prefer FE"
    elif pvalue < 0.10:
        conclusion = "Reject H0 at 10% only"
    else:
        conclusion = "Fail to reject H0: RE consistent"

    n_obs = int(res.nobs)
    n_countries = int(df.index.get_level_values("iso3").nunique())

    print(f"  Spec {spec_name}: Wald={W:.4f}, df={df_test}, "
          f"p={pvalue:.4f}, inversion={inversion}")
    print(f"    Conclusion: {conclusion}")

    return [
        _empty_record(
            check="mundlak",
            subgroup=f"Spec_{spec_name}",
            item_kind="wald_test",
            item_name="country_means_jointly_zero",
            statistic=W,
            df=df_test,
            pvalue=pvalue,
            n_obs=n_obs,
            n_countries=n_countries,
            notes=f"inversion={inversion}; {conclusion}",
        )
    ]


# ----- Check 2: Boundary-case reassignment -------------------------------


def per_cluster_slope_records_from_re(
    res, n_obs: int, subgroup_label: str, check_label: str
) -> list[dict]:
    params = res.params
    cov = res.cov
    df_resid = float(getattr(res, "df_resid", n_obs - len(params)))

    if HEADLINE_VAR not in params.index:
        raise KeyError(f"{HEADLINE_VAR} not found in Spec C params")
    beta_mys = float(params[HEADLINE_VAR])
    var_mys = float(cov.loc[HEADLINE_VAR, HEADLINE_VAR])

    records: list[dict] = []
    for cid in (0, 1, 2):
        if cid == 0:
            slope = beta_mys
            variance = var_mys
            ivar = "(baseline)"
        else:
            ivar_match = _find_interaction_var(params.index, cid)
            if ivar_match is None:
                records.append(_empty_record(
                    check=check_label, subgroup=subgroup_label,
                    item_kind="slope", item_name=f"cluster_{cid}",
                    notes="interaction term not found",
                    n_obs=n_obs,
                ))
                continue
            beta_int = float(params[ivar_match])
            var_int = float(cov.loc[ivar_match, ivar_match])
            cov_term = float(cov.loc[HEADLINE_VAR, ivar_match])
            slope = beta_mys + beta_int
            variance = var_mys + var_int + 2.0 * cov_term
            ivar = ivar_match

        if variance < 0 or not np.isfinite(variance):
            std_error = float("nan"); tstat = float("nan")
            pvalue = float("nan"); ci_low = float("nan"); ci_high = float("nan")
        else:
            std_error = float(np.sqrt(variance))
            tstat = slope / std_error if std_error > 0 else float("nan")
            tcrit = float(sp_stats.t.ppf(0.975, df=df_resid)) if df_resid > 0 else 1.96
            if df_resid > 0 and np.isfinite(tstat):
                pvalue = float(2.0 * (1.0 - sp_stats.t.cdf(abs(tstat), df=df_resid)))
            else:
                pvalue = float("nan")
            ci_low = slope - tcrit * std_error
            ci_high = slope + tcrit * std_error

        records.append(_empty_record(
            check=check_label,
            subgroup=subgroup_label,
            item_kind="slope",
            item_name=f"cluster_{cid}",
            value=float(slope),
            std_error=std_error,
            statistic=tstat,
            pvalue=pvalue,
            ci_low=ci_low,
            ci_high=ci_high,
            n_obs=float(n_obs),
            notes=ivar,
        ))
    return records


def boundary_case_check(panel_indexed: pd.DataFrame) -> list[dict]:
    df_base = panel_indexed.dropna(subset=list(SPEC_C_LISTWISE)).copy()
    df_base["cluster_kmeans_k3"] = df_base["cluster_kmeans_k3"].astype(int)

    iso_in_panel = df_base.index.get_level_values("iso3").unique()
    boundary_present = [c for c in BOUNDARY_ISO3 if c in iso_in_panel]
    boundary_missing = [c for c in BOUNDARY_ISO3 if c not in iso_in_panel]
    print(f"  Boundary countries present: {boundary_present}")
    if boundary_missing:
        print(f"  Boundary countries missing from Spec C sample: {boundary_missing}")

    pre_assignments = (
        df_base.reset_index()
        .drop_duplicates("iso3")
        .set_index("iso3")
        .loc[boundary_present, "cluster_kmeans_k3"]
        .to_dict()
    )
    print(f"  Pre-reassignment cluster IDs: {pre_assignments}")

    print("  [BASELINE] Spec C RE on original cluster assignments...")
    mod_base = RandomEffects.from_formula(SPEC_C_RE_FORMULA, data=df_base)
    res_base = mod_base.fit(cov_type="clustered", cluster_entity=True)
    n_obs_base = int(res_base.nobs)

    records: list[dict] = []
    records.extend(per_cluster_slope_records_from_re(
        res_base, n_obs_base, "baseline", "boundary_case"
    ))

    df_reassigned = df_base.copy()
    iso_idx = df_reassigned.index.get_level_values("iso3")
    mask = iso_idx.isin(boundary_present)
    df_reassigned.loc[mask, "cluster_kmeans_k3"] = 1
    df_reassigned["cluster_kmeans_k3"] = df_reassigned["cluster_kmeans_k3"].astype(int)
    n_changed_rows = int(mask.sum())
    print(f"  [REASSIGNED] {n_changed_rows:,} country-year rows reassigned to "
          f"Cluster 1 (covering {len(boundary_present)} countries)")

    mod_re = RandomEffects.from_formula(SPEC_C_RE_FORMULA, data=df_reassigned)
    res_re = mod_re.fit(cov_type="clustered", cluster_entity=True)
    n_obs_re = int(res_re.nobs)

    records.extend(per_cluster_slope_records_from_re(
        res_re, n_obs_re, "reassigned", "boundary_case"
    ))

    print("  Per-cluster slopes (baseline vs reassigned):")
    for r in records:
        slope = r["value"]; se = r["std_error"]; p = r["pvalue"]
        stars = _sig_stars(p) if np.isfinite(p) else ""
        print(f"    [{r['subgroup']:>10}] {r['item_name']}: "
              f"{slope:+.4f}{stars} (SE {se:.4f}, p={p:.4f})")

    return records


# ----- Check 3: MNAR selection diagnostic --------------------------------


def mnar_check(
    panel: pd.DataFrame, country_meta: pd.DataFrame
) -> list[dict]:
    """MNAR diagnostic: continuous tests at country-year level,
    categorical tests at country level."""
    spec_a_cols = [DEPENDENT, *SPEC_A_RHS]
    is_complete = panel[spec_a_cols].notna().all(axis=1)
    n_a = int(is_complete.sum())
    n_b = int((~is_complete).sum())
    print(f"  Group A (Spec A complete): {n_a:,} country-years")
    print(f"  Group B (incomplete):       {n_b:,} country-years")

    # Merge categoricals from country_meta WITHOUT colliding with any
    # panel-side columns of the same name. suffixes=("","_meta") keeps
    # panel's existing columns named as-is; meta's columns get "_meta"
    # only if there is a collision.
    panel_for_test = panel.merge(
        country_meta[["iso3", *MNAR_CATEGORICAL_VARS]],
        on="iso3",
        how="left",
        validate="many_to_one",
        suffixes=("", "_meta"),
    )
    _strip_whitespace_inplace(panel_for_test, list(MNAR_CATEGORICAL_VARS))

    # Per-row column resolution: prefer panel-side; fall back to _meta.
    cat_col_resolved: dict[str, str] = {}
    for c in MNAR_CATEGORICAL_VARS:
        if c in panel_for_test.columns:
            cat_col_resolved[c] = c
        elif f"{c}_meta" in panel_for_test.columns:
            cat_col_resolved[c] = f"{c}_meta"
        else:
            cat_col_resolved[c] = ""

    records: list[dict] = []

    # ----- Continuous: country-year level -------------------------------
    print("  Continuous-variable distribution tests (country-year level):")
    for var in MNAR_CONTINUOUS_VARS:
        a = panel_for_test.loc[is_complete, var].dropna()
        b = panel_for_test.loc[~is_complete, var].dropna()
        n_a_v = len(a); n_b_v = len(b)

        t_res = sp_stats.ttest_ind(a, b, equal_var=False, nan_policy="omit")
        try:
            mw_res = sp_stats.mannwhitneyu(a, b, alternative="two-sided")
            mw_stat, mw_p = float(mw_res.statistic), float(mw_res.pvalue)
        except ValueError:
            mw_stat, mw_p = float("nan"), float("nan")
        ks_res = sp_stats.ks_2samp(a, b)
        ks_stat, ks_p = float(ks_res.statistic), float(ks_res.pvalue)

        a_mean = float(a.mean()); b_mean = float(b.mean())
        diff = a_mean - b_mean
        print(f"    {var}:")
        print(f"      mean(A)={a_mean:.3f} (n={n_a_v}), "
              f"mean(B)={b_mean:.3f} (n={n_b_v}), diff={diff:+.3f}")
        print(f"      Welch t: t={float(t_res.statistic):.3f}, "
              f"p={float(t_res.pvalue):.4g}{_sig_stars(float(t_res.pvalue))}")
        print(f"      Mann-Whitney: U={mw_stat:.0f}, p={mw_p:.4g}{_sig_stars(mw_p)}")
        print(f"      KS: D={ks_stat:.4f}, p={ks_p:.4g}{_sig_stars(ks_p)}")

        records.append(_empty_record(
            check="mnar_selection", subgroup="continuous_cy",
            item_kind="welch_t", item_name=var,
            value=diff, statistic=float(t_res.statistic),
            pvalue=float(t_res.pvalue),
            n_obs=float(n_a_v), n_countries=float(n_b_v),
            notes=f"mean_A={a_mean:.4f}; mean_B={b_mean:.4f}",
        ))
        records.append(_empty_record(
            check="mnar_selection", subgroup="continuous_cy",
            item_kind="mann_whitney", item_name=var,
            statistic=mw_stat, pvalue=mw_p,
            n_obs=float(n_a_v), n_countries=float(n_b_v),
        ))
        records.append(_empty_record(
            check="mnar_selection", subgroup="continuous_cy",
            item_kind="ks_test", item_name=var,
            statistic=ks_stat, pvalue=ks_p,
            n_obs=float(n_a_v), n_countries=float(n_b_v),
        ))

    # ----- Categorical: country level -----------------------------------
    # Time-invariant per country, so country-year would inflate N by ~34.
    # A country is in group A if at least one of its country-years has
    # Spec A complete; group B otherwise.
    country_a_set = set(panel.loc[is_complete, "iso3"].unique())
    panel_iso_set = set(panel["iso3"].unique())
    n_country_a = len(country_a_set)
    n_country_b = len(panel_iso_set) - n_country_a
    print(f"  Categorical-variable contingency tests (country level):")
    print(f"    Group A countries: {n_country_a}; "
          f"Group B countries: {n_country_b} (sum={len(panel_iso_set)})")

    # Build a 1-row-per-country table from the panel (using the first
    # observation of each iso3) so that whichever of region_name /
    # income_level_name is panel-side is preserved.
    country_table = (
        panel_for_test
        .drop_duplicates("iso3")
        .copy()
        .reset_index(drop=True)
    )
    country_table = country_table[country_table["iso3"].isin(panel_iso_set)].copy()
    country_table["mnar_group"] = np.where(
        country_table["iso3"].isin(country_a_set), "A", "B"
    )

    for var, resolved_col in cat_col_resolved.items():
        if not resolved_col or resolved_col not in country_table.columns:
            print(f"    [SKIP] {var}: not present in panel or metadata")
            records.append(_empty_record(
                check="mnar_selection", subgroup="categorical_country",
                item_kind="chi2", item_name=var,
                notes="column not available",
            ))
            continue

        ct = pd.crosstab(country_table[resolved_col], country_table["mnar_group"])
        if ct.shape[0] < 2 or ct.shape[1] < 2:
            print(f"    [SKIP] {var}: degenerate contingency "
                  f"(shape={ct.shape})")
            records.append(_empty_record(
                check="mnar_selection", subgroup="categorical_country",
                item_kind="chi2", item_name=var,
                notes=f"degenerate contingency shape={ct.shape}",
            ))
            continue

        chi2, p, dof, _ = sp_stats.chi2_contingency(ct)
        n_in_test_a = int(ct.sum(axis=0).get("A", 0))
        n_in_test_b = int(ct.sum(axis=0).get("B", 0))
        print(f"    {var}: chi2={chi2:.3f}, df={dof}, "
              f"p={p:.4g}{_sig_stars(p)}  "
              f"(categories={ct.shape[0]}, "
              f"n_A_countries={n_in_test_a}, n_B_countries={n_in_test_b})")
        # Also print the top-line contingency for portfolio narrative.
        print(f"      contingency:")
        for category, row in ct.iterrows():
            print(f"        {str(category):<32} A={row.get('A', 0):>3} "
                  f"B={row.get('B', 0):>3}")
        records.append(_empty_record(
            check="mnar_selection", subgroup="categorical_country",
            item_kind="chi2", item_name=var,
            statistic=float(chi2), df=float(dof), pvalue=float(p),
            n_obs=float(n_in_test_a), n_countries=float(n_in_test_b),
            notes=f"n_categories={ct.shape[0]}; resolved_col={resolved_col}",
        ))

    return records


# ----- Check 4: Sub-period 2010-2019 -------------------------------------


def subperiod_check(panel: pd.DataFrame) -> list[dict]:
    if not pd.api.types.is_integer_dtype(panel["year"]):
        panel["year"] = panel["year"].astype(int)
    sub = panel.loc[(panel["year"] >= 2010) & (panel["year"] <= 2019)].copy()
    sub["year_factor"] = sub["year"].astype("category")
    sub_indexed = sub.set_index(["iso3", "year"]).sort_index()

    needed = [DEPENDENT, *SPEC_A_RHS]
    df_l = sub_indexed.dropna(subset=needed)
    n_obs = len(df_l)
    n_countries = int(df_l.index.get_level_values("iso3").nunique())
    n_years = int(df_l.index.get_level_values("year").nunique())
    print(f"  Sub-period sample: N={n_obs:,} country-years, "
          f"{n_countries} countries, {n_years} years")

    fe_formula = (
        f"{DEPENDENT} ~ 1 + " + " + ".join(SPEC_A_RHS)
        + " + EntityEffects + TimeEffects"
    )
    re_formula = (
        f"{DEPENDENT} ~ 1 + " + " + ".join(SPEC_A_RHS)
        + " + C(year_factor)"
    )

    mod_fe = PanelOLS.from_formula(fe_formula, data=sub_indexed, drop_absorbed=True)
    res_fe = mod_fe.fit(cov_type="clustered", cluster_entity=True)
    mod_re = RandomEffects.from_formula(re_formula, data=sub_indexed)
    res_re = mod_re.fit(cov_type="clustered", cluster_entity=True)

    records: list[dict] = []
    for label, res in [("FE", res_fe), ("RE", res_re)]:
        if HEADLINE_VAR not in res.params.index:
            continue
        coef = float(res.params[HEADLINE_VAR])
        se = float(res.std_errors[HEADLINE_VAR])
        p = float(res.pvalues[HEADLINE_VAR])
        ci = res.conf_int().loc[HEADLINE_VAR]
        ci_low = float(ci["lower"]); ci_high = float(ci["upper"])
        stars = _sig_stars(p)
        print(f"  Sub-period {label} mys: {coef:+.4f}{stars}  "
              f"(SE {se:.4f}, p={p:.4f}, "
              f"CI [{ci_low:+.4f}, {ci_high:+.4f}])")
        records.append(_empty_record(
            check="subperiod_2010_2019",
            subgroup=f"Spec_A_{label}",
            item_kind="coef",
            item_name=HEADLINE_VAR,
            value=coef,
            std_error=se,
            statistic=float(res.tstats[HEADLINE_VAR]),
            pvalue=p,
            ci_low=ci_low,
            ci_high=ci_high,
            n_obs=float(int(res.nobs)),
            n_countries=float(n_countries),
            notes=f"R²_within={res.rsquared_within:+.4f}",
        ))

    return records


# ----- Main ---------------------------------------------------------------


def main() -> None:
    project_root = find_project_root()
    panel_path = project_root / "data" / "processed" / "panel_modelling.csv"
    metadata_path = (
        project_root / "data" / "raw" / "world_bank" / "wb_country_metadata.csv"
    )
    output_path = (
        project_root / "outputs" / "tables" / "phase05_s07_robustness_results.csv"
    )

    print(f"[INFO] Project root: {project_root}")
    print(f"[INFO] Panel:        {panel_path}")
    print(f"[INFO] Metadata:     {metadata_path}")
    print(f"[INFO] Output:       {output_path}")
    print()

    panel, enc = read_csv_with_encoding_fallback(panel_path)
    print(f"[LOAD] panel_modelling.csv ({enc}): "
          f"{panel.shape[0]:,} rows x {panel.shape[1]} cols")

    if not pd.api.types.is_integer_dtype(panel["year"]):
        panel["year"] = panel["year"].astype(int)
    panel["year_factor"] = panel["year"].astype("category")
    panel_indexed = panel.set_index(["iso3", "year"]).sort_index()

    country_meta = load_country_metadata(metadata_path)
    print(f"[LOAD] country metadata: {len(country_meta):,} countries "
          f"(aggregates dropped)")

    panel_has_region = "region_name" in panel.columns
    panel_has_income = "income_level_name" in panel.columns
    print(f"[INFO] panel-side region_name={panel_has_region}, "
          f"income_level_name={panel_has_income}")
    print()

    all_records: list[dict] = []

    print("[CHECK 1] Mundlak alternative-Hausman (Wald on country means)")
    for spec_name, rhs in [("A", SPEC_A_RHS), ("B", SPEC_B_RHS)]:
        all_records.extend(mundlak_check(panel_indexed, spec_name, rhs))
    print()

    print("[CHECK 2] Boundary-case reassignment "
          "(BRA/ZAF/MEX/ARG -> Cluster 1)")
    all_records.extend(boundary_case_check(panel_indexed))
    print()

    print("[CHECK 3] MNAR selection diagnostic "
          "(Spec A complete vs incomplete)")
    all_records.extend(mnar_check(panel, country_meta))
    print()

    print("[CHECK 4] Sub-period 2010-2019 (Spec A FE + RE)")
    all_records.extend(subperiod_check(panel))
    print()

    out_df = pd.DataFrame(all_records)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(output_path, index=False, encoding="utf-8")

    counts_by_check = out_df["check"].value_counts().to_dict()
    print(f"[OK] Wrote {output_path.name}: {len(out_df):,} rows")
    print(f"     Rows per check: {counts_by_check}")


if __name__ == "__main__":
    main()
