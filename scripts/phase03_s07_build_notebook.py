"""
Phase 03 - Step 07: Build the portfolio notebook 03_eda.ipynb.

Purpose:
    Construct notebooks/03_eda.ipynb programmatically via nbformat, then
    execute it top-to-bottom using nbconvert's ExecutePreprocessor against
    the project's kernel. The notebook is the portfolio-facing artefact for
    Phase 03 EDA. It loads `data/processed/panel.csv` and the Phase 03
    output tables, embeds the Phase 03 figures, and presents a structured
    narrative with 3-layer interpretation (Observation / Explanation /
    Implication) for each finding.

Inputs:
    data/processed/panel.csv
    outputs/figures/phase03_s02_univariate_distributions.png
    outputs/figures/phase03_s03_bivariate_gini.png
    outputs/figures/phase03_s04_correlation_matrix.png
    outputs/figures/phase03_s05_timeseries_by_region.png
    outputs/figures/phase03_s05_timeseries_by_income.png
    outputs/figures/phase03_s06_choropleth_gini.html
    outputs/figures/phase03_s06_choropleth_mys.html
    outputs/tables/phase03_s02_skewness.csv
    outputs/tables/phase03_s03_lowess_vs_linear.csv
    outputs/tables/phase03_s04_vif.csv

Outputs:
    notebooks/03_eda.ipynb (~17 cells, fully executed)
"""

from __future__ import annotations

import sys
from pathlib import Path

import nbformat
from nbformat.v4 import new_code_cell, new_markdown_cell, new_notebook
from nbconvert.preprocessors import ExecutePreprocessor

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.paths import find_project_root  # noqa: E402

# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------

KERNEL_NAME = "python3"   # works inside p4_education when launched in that env
EXECUTE_TIMEOUT = 600     # seconds


# -----------------------------------------------------------------------------
# Cell content
# -----------------------------------------------------------------------------

CELL_01_TITLE = """\
# Phase 03 — Exploratory Data Analysis

**Project:** Education and Income Inequality — A Cross-Country Panel Analysis
**Phase:** 03 — Exploratory Data Analysis
**Author:** Kota
**Last executed:** auto-stamped at notebook execution time (see footer)

---

## Abstract

This notebook characterises the analytical panel constructed in Phase 02
(`data/processed/panel.csv`, 7,378 country-year rows × 19 declared variables
covering 217 countries from 1990 to 2023) along five axes — distributions,
bivariate relationships, multicollinearity, temporal patterns, and geographic
patterns. The goal is to surface the structural facts that constrain Phase
05's econometric specifications and motivate the Phase 04 country
clustering.

The headline findings preview the synthesis at the foot of this notebook:
**mean years of schooling is the strongest single linear predictor of Gini**
(r = −0.52, OLS R² = 0.27); **the three secondary-enrolment variables are
arithmetically nested** with VIFs in the 9,000–40,000 range and must be
deconfounded before modelling; **the secondary-vs-tertiary enrolment story
is well captured linearly** but `enrol_primary` and `mean_years_schooling`
show meaningful non-linearity (LOWESS Δ R² > 0.10) that a linear baseline
will under-fit; **`edu_expenditure_gdp` is the only candidate predictor with
a null bivariate signal**, suggesting policy framing should distinguish
input intensity from attainment outcomes; and **the income-group view of
Gini is consistent with a Kuznets-type inverted-U** in which middle-income
countries are most unequal, not the lowest-income ones.

This notebook does *not* make causal claims. EDA describes; Phase 05's
panel econometrics estimate associations under stated assumptions; Phase 07
discusses what those associations can and cannot license.
"""

CELL_02_FRAMING = """\
## 1. Framing

Phase 03 sits between data infrastructure (Phases 01–02) and analytical
modelling (Phases 05–06). Its output is not a model but a **diagnostic
landscape**: per-variable shape, pairwise relationships, joint
multicollinearity structure, temporal evolution, and geography. Each of
these directly informs at least one downstream design choice.

| Phase 03 finding axis | Phase 04/05/07 design choice it informs |
|---|---|
| Univariate distribution (skew, modality) | log-transform decisions; robustness to outliers |
| Bivariate (LOWESS vs linear) | linear baseline vs polynomial; transformation choice |
| Correlation / VIF | variable selection in regression specifications |
| Time-series by region / income | clustering features; convergence narrative |
| Geographic | cross-country heterogeneity claims; missingness narrative |

The interpretation of every figure follows a three-layer structure:
**Observation** (what the figure literally shows), **Explanation** (the
likely structural cause), **Implication** (what this changes for downstream
phases). EDA describes; it does not conclude. Where natural language
suggests causation we hedge with "consistent with" or "may indicate".

The seven Phase 03 design decisions logged at the top of Step 01
(`PROJECT_LOG.md`, entry dated 2026-04-30) are taken as given here. In
particular: choropleths are rendered with plotly to avoid the geopandas
dependency surface; both `region_name` and `income_level_name` carry
narrative load (geography vs structure); skewed economic variables are
shown raw alongside log; VIF flagging is stratified at thresholds 5 (watch)
and 10 (concern); pre-2000 enrolment sparsity is shown not truncated;
and unemployment lines start at 1991 (ILO modeled-series start year) with
a footnote rather than restricting the entire panel.
"""

CELL_03_SETUP = """\
## 2. Setup
"""

CELL_04_SETUP_CODE = """\
from pathlib import Path

import numpy as np
import pandas as pd
from IPython.display import Image, IFrame, display, Markdown

import sys
sys.path.insert(0, str(Path.cwd().parent))
from src.paths import find_project_root
from src.manifest import load_manifest, manifest_variable_order

PROJECT_ROOT = find_project_root(Path.cwd())
PANEL_PATH = PROJECT_ROOT / "data" / "processed" / "panel.csv"
MANIFEST_PATH = PROJECT_ROOT / "data" / "raw" / "manifest.yaml"
FIG_DIR = PROJECT_ROOT / "outputs" / "figures"
TBL_DIR = PROJECT_ROOT / "outputs" / "tables"

panel = pd.read_csv(PANEL_PATH)
manifest = load_manifest(MANIFEST_PATH)
var_order = manifest_variable_order(manifest)

print(f"Panel shape: {panel.shape[0]:,} rows x {panel.shape[1]} columns")
print(f"Year range: {panel['year'].min()}-{panel['year'].max()}")
print(f"Countries: {panel['iso3'].nunique()}")
print(f"Declared variables (manifest order): {len(var_order)}")
"""

CELL_05_UNIVARIATE = """\
## 3. Univariate distributions

The first diagnostic is the per-variable shape: where each of the 19
declared variables sits, how concentrated or dispersed its values are, and
whether a transformation is warranted before modelling. The grid below
shows histograms for all 19 variables. For three economic variables whose
raw distributions are heavily right-skewed
(`gdp_per_capita_usd`, `gdp_per_capita_ppp`, `population`), the log-
transformed view is shown alongside the raw view. Each subplot annotates
its observed N and Fisher–Pearson skewness.
"""

CELL_06_UNIVARIATE_CODE = """\
display(Image(filename=str(FIG_DIR / "phase03_s02_univariate_distributions.png")))

skew_df = pd.read_csv(TBL_DIR / "phase03_s02_skewness.csv")

# Pivot to compare raw vs log for the three log-paired variables.
log_pairs = (
    skew_df[skew_df["variable"].isin(["gdp_per_capita_usd",
                                       "gdp_per_capita_ppp",
                                       "population"])]
    .pivot(index="variable", columns="transform", values="skewness")
    .rename(columns={"raw": "skew_raw", "log": "skew_log"})
    .reset_index()
)
display(Markdown("**Skewness reduction under log transform:**"))
display(log_pairs)
"""

CELL_07_UNIVARIATE_INTERP = """\
**Observation.** Three variables stand out as severely right-skewed in raw
form: `inflation_cpi` (skewness 52.5, an extreme spike driven by a small
number of hyperinflation episodes), `population` (9.0), and
`gdp_per_capita_usd` (3.5). The three skewed economic variables become
near-symmetric under log transform: `gdp_per_capita_usd` collapses from
3.53 to −0.01, `gdp_per_capita_ppp` from 1.87 to −0.16, and `population`
from 9.03 to −0.40. `enrol_primary` shows the opposite pattern — a left
skew (−1.06) — driven by the 100% gross-enrolment ceiling. `gini` itself
is mildly right-skewed (0.76).

**Explanation.** Per-capita GDP and population are multiplicative
quantities by their nature, so distributions over countries are
approximately log-normal — the log transform recovers near-Gaussian shape
mechanically rather than as a modelling trick. The inflation distribution
is dominated by a handful of hyperinflation country-years (most plausibly
Zimbabwe, Venezuela, mid-1990s post-Soviet states); these are real
observations, not data errors, but a single-coefficient linear treatment
would let them dominate the fit. `enrol_primary` shows a ceiling because
gross enrolment ratios above 100% reflect over-age enrolment, not coverage
beyond 100%, and many countries have stabilised near or just above this
ceiling.

**Implication.** Phase 05 will apply log transformations to
`gdp_per_capita_*` and `population` at modelling time (consistent with the
Phase 02 schema decision to store raw values). `inflation_cpi` requires
either a log transform or winsorisation; the presence of a non-trivial set
of country-years with inflation above 1,000% means a naive log can still
leave the distribution heavy-tailed and a robust regression diagnostic
will be needed in Phase 05. `enrol_primary`'s ceiling means the linear
relationship between primary enrolment and Gini is structurally bounded
above and is unlikely to be informative at the high end — a finding that
recurs in the bivariate analysis below.
"""

CELL_08_BIVARIATE = """\
## 4. Bivariate analysis: Gini vs candidate predictors

The next diagnostic asks how each candidate predictor relates to Gini in
isolation. Six predictors are shown — three enrolment levels, mean years
of schooling, education expenditure as a share of GDP, and (as a benchmark
control) GDP per capita PPP. Points are coloured by region; a LOWESS
smoother is overlaid on each panel. The annotation reports the linear
(OLS) R², the LOWESS R², and their difference Δ. Δ above ~0.02 is treated
as a hint of meaningful non-linearity.
"""

CELL_09_BIVARIATE_CODE = """\
display(Image(filename=str(FIG_DIR / "phase03_s03_bivariate_gini.png")))

lowess_df = pd.read_csv(TBL_DIR / "phase03_s03_lowess_vs_linear.csv")
display(Markdown("**LOWESS vs OLS linear fit, Gini as response:**"))
display(lowess_df.style.format({
    "ols_slope": "{:+.4f}",
    "ols_r2": "{:.3f}",
    "lowess_r2": "{:.3f}",
    "delta_r2": "{:+.3f}",
}))
"""

CELL_10_BIVARIATE_INTERP = """\
**Observation.** `mean_years_schooling` is the strongest single linear
predictor (OLS R² = 0.27, slope = −1.49 Gini points per additional year
of schooling). `enrol_secondary` and `enrol_tertiary` show clean negative
linear relationships (OLS R² ≈ 0.16 each). `gdp_per_capita_ppp` (raw)
gives R² = 0.19 with a near-zero linear slope reflecting its scale rather
than its effect. `enrol_primary` has the lowest OLS R² (0.03) but the
largest LOWESS-vs-linear gap (Δ = 0.166). `edu_expenditure_gdp` is the
only predictor where the LOWESS fit is *worse* than linear (Δ = −0.014),
i.e. there is essentially no structure for the smoother to capture.

**Explanation.** The rank ordering is consistent with the standard story
that *attainment* (years of schooling actually accumulated) carries more
information about a country's stratification than any single *flow*
indicator (current enrolment) — a stock vs flow distinction. The
near-zero slope on `gdp_per_capita_ppp` reflects the units (Gini is in
0–100 percentage-point units, GDP per capita in tens of thousands of
dollars), not the lack of association; the R² is the meaningful number.
The large LOWESS gap for `enrol_primary` is an artefact: the relationship
is non-monotonic because both the lowest-Gini countries (high-income, near
universal primary enrolment) and the highest-Gini countries (Latin
America, also near universal primary enrolment) cluster around 100% gross
enrolment, so a linear fit produces a near-zero coefficient with the
"wrong" sign. The null result for `edu_expenditure_gdp` is real — public
education expenditure as a share of GDP is not, by itself, predictive of
the income distribution.

**Implication.** Phase 05's baseline specification should anchor on
`mean_years_schooling` as the primary education variable. `enrol_secondary`
and `enrol_tertiary` work well as linear regressors. `enrol_primary`
should not enter a linear specification as a single term — its
relationship is non-monotonic and a misleading positive coefficient is
likely; if retained at all it should be in a polynomial or interaction
form. The null on `edu_expenditure_gdp` is itself a Phase 07 narrative:
input intensity is not the same as attainment, and the policy
implications differ correspondingly.
"""

CELL_11_CORR = """\
## 5. Correlation and multicollinearity

Pairwise correlations and Variance Inflation Factors (VIF) measure the
joint structure of the predictor set. Pearson correlation is computed
pairwise-complete across all 19 declared variables (so each cell uses the
maximum N for that pair). VIF is computed on the listwise-complete subset
of the 18 right-hand-side variables (gini excluded, as it is the target),
and is stratified at two thresholds per Phase 03 design Decision 4: VIF
above 5 is "watch", above 10 is "concern".
"""

CELL_12_CORR_CODE = """\
display(Image(filename=str(FIG_DIR / "phase03_s04_correlation_matrix.png")))

vif_df = pd.read_csv(TBL_DIR / "phase03_s04_vif.csv")
display(Markdown("**VIF table (sorted descending):**"))
display(vif_df.style.format({"vif": "{:,.2f}"}))

# Strongest |Pearson r| with gini.
corr = pd.read_csv(TBL_DIR / "phase03_s04_correlation_matrix.csv", index_col=0)
gini_corrs = corr["gini"].drop("gini").abs().sort_values(ascending=False)
top5 = pd.DataFrame({
    "variable": gini_corrs.head(5).index,
    "pearson_r_with_gini": [corr.loc[v, "gini"] for v in gini_corrs.head(5).index],
})
display(Markdown("**Top 5 |Pearson r| with gini:**"))
display(top5)
"""

CELL_13_CORR_INTERP = """\
**Observation.** The correlation heatmap shows three structurally tight
blocks: the secondary-enrolment trio (`enrol_secondary`,
`enrol_secondary_female`, `enrol_secondary_male` correlate at 0.97–0.99
with each other), the GDP duo (`gdp_per_capita_ppp` and
`gdp_per_capita_usd` at r = 0.90), and `agri_value_added_gdp` correlates
strongly negatively with most "development" indicators (−0.72, −0.69,
−0.68 with the secondary-enrolment trio, mean years of schooling, and
urban population share respectively). The top-5 |Pearson r| with gini are
`mean_years_schooling` (−0.52), `gdp_per_capita_ppp` (−0.43),
`enrol_secondary_male` (−0.43), `enrol_secondary` (−0.40), and
`enrol_tertiary` (−0.40) — all negative, all education or development
indicators, all comparable in magnitude.

The VIF table makes the structural issue concrete. The secondary-enrolment
trio carries VIFs in the 9,000–40,000 range — three orders of magnitude
above the conventional concern threshold. The two GDP variables sit at
14.4 and 10.5. Below those, the highest VIF is `agri_value_added_gdp` at
5.4 (watch). Crucially, **the sector-share trio itself
(`agri/manu/services_value_added_gdp`) does not all sit at the concern
level** — `manu` is at 1.4 and `services` at 2.9.

**Explanation.** The four-digit VIFs on the secondary-enrolment trio
reflect an arithmetic identity: the female and male series are nested
inside the aggregate (`enrol_secondary` ≈ a population-weighted average
of the gender-split series). This is not a statistical accident, it is a
definition. The GDP duo is a measurement choice: PPP and USD are two
representations of the same underlying quantity. The sector-share trio
*sounds* like it should be near-singular because the three shares sum to
near 100%, but in practice the WB sector classification leaves room for
"net taxes on products" and other residuals, and the three series carry
enough cross-country and within-country independent variation to avoid
extreme collinearity. The pre-Phase-03 expectation that sector shares
would dominate the multicollinearity diagnostics turns out to be
overstated.

**Implication.** Phase 05's baseline specification will use exactly one
of the secondary-enrolment trio (`enrol_secondary` is the natural choice;
the gender-split variables will appear only in heterogeneity
specifications). Exactly one of the GDP duo will be used (`ppp` is
preferred per Scope §6.3 for cross-country comparability; `usd` is
available as a robustness check). The sector shares are usable as three
regressors in the baseline — a finding that allows a cleaner specification
than initially expected.
"""

CELL_14_TIMESERIES = """\
## 6. Time-series by region and by income group

Cross-country mean lines with IQR bands show how `gini`,
`mean_years_schooling`, and `enrol_secondary` evolve from 1990 to 2023,
grouped first by World Bank region and then by income group. Strata with
fewer than five countries are drawn as individual country lines (notably
North America at n = 3, and a small "Not classified" income stratum at
n = 2). The N_observed range printed in each subplot title indicates the
year-by-year coverage of the variable within the figure.
"""

CELL_15_TIMESERIES_CODE = """\
display(Markdown("### By region"))
display(Image(filename=str(FIG_DIR / "phase03_s05_timeseries_by_region.png")))

display(Markdown("### By income group"))
display(Image(filename=str(FIG_DIR / "phase03_s05_timeseries_by_income.png")))
"""

CELL_16_TIMESERIES_INTERP = """\
**Observation (region).** The Gini panel shows Latin America & Caribbean
at the top of the cross-country distribution throughout the period
(regional mean drifting from ~50 to ~45), Europe & Central Asia anchoring
the bottom (~30 throughout), and the other regions clustered between 33
and 42. North America's three individual lines (USA, CAN, BMU) show
gradual divergence: the United States rises from the low-30s in the
early 1990s to mid-30s in the most recent decade. The
`mean_years_schooling` panel is monotonically increasing in every region
but the regional gaps do not visibly close. The `enrol_secondary` panel
shows the most dramatic change: Sub-Saharan Africa's regional mean
roughly triples from the early 1990s, and South Asia tracks a similar
trajectory.

**Observation (income group).** Plotted by income group rather than
geography, the Gini panel reveals a structurally different pattern. High
income countries sit at the *bottom* of the distribution (~30, with a
narrow IQR), upper-middle income at the top (~40, falling toward 35),
and low income in between with substantial year-on-year noise. The mean
years of schooling panel shows a clean stepped gradient (high ≫ upper
middle ≫ lower middle ≫ low) with the gradient roughly preserved over
time. The `enrol_secondary` panel shows a clear convergence: low-income
countries have moved from ~15 toward ~45 secondary gross enrolment,
narrowing the gap with high-income countries.

**Explanation.** The region-vs-income contrast is the most analytically
important pattern in the time-series data. Geography sorts countries by
the *direction* of inequality (Latin America high, Europe low) but the
income-group cut suggests Gini follows a *non-monotonic* relationship
with development — high-inequality countries are concentrated at the
upper-middle income level rather than at the bottom. This is consistent
with a Kuznets-type inverted-U, in which inequality rises during the
transition from agrarian to industrial economies and falls again as
service-sector economies mature. The convergence in secondary enrolment
reflects the global push for universal secondary education following the
Millennium Development Goals (1990–2015) and Sustainable Development
Goals (2015 onward).

**Implication.** The region and income views answer different questions
and motivate different downstream uses. Phase 04's clustering should not
treat region as the primary structural axis — the Kuznets-type pattern
in the income view suggests development-level features (`mys`,
`gdp_per_capita_ppp`, sector shares) will carry the cluster-defining
signal more cleanly than geographic dummies. Phase 05's heterogeneity
specifications should consider income-group interactions because the
Gini–education relationship plausibly has different slopes across the
inverted-U. Phase 07's policy framing should be income-group rather than
region first, and should be careful about the implicit "low-income =
high-Gini" claim that the Kuznets pattern contradicts.

A footnote on the time-series construction. `enrol_secondary` is sparse
before 2000 (Phase 02 missingness report: 4 countries with full 1990–2023
secondary enrolment coverage). The IQR bands shown for that variable in
the early 1990s reflect a small and shifting set of countries and
should be read with that in mind. `unemployment_rate` is not plotted
here, but for any time-series including it the line should start at 1991
because the ILO modeled-series start year is 1991, not 1990.
"""

CELL_17_GEOGRAPHY = """\
## 7. Geographic visualisation

The choropleth maps below render `gini` and `mean_years_schooling` as
country-level fills using ISO-3 country codes. For each variable the
left panel shows the most recent year with broad coverage (Gini: 2021
with N = 81 country observations; mean years of schooling: 2022 with
N = 193), and the right panel shows the panel-period mean across all
years where the country has at least one observation. Maps are rendered
with plotly and saved as standalone HTML files; the embedded interactive
versions below are visible when this notebook is opened in a Jupyter
runtime, and the linked HTML files can be opened directly in any
browser.
"""

CELL_18_GEOGRAPHY_CODE = """\
gini_html = FIG_DIR / "phase03_s06_choropleth_gini.html"
mys_html = FIG_DIR / "phase03_s06_choropleth_mys.html"

# IFrame src is interpreted relative to the notebook's own location, so
# from notebooks/03_eda.ipynb we need ../outputs/figures/<file>.html with
# forward slashes (HTML attribute convention) regardless of host OS.
GINI_IFRAME_SRC = "../outputs/figures/phase03_s06_choropleth_gini.html"
MYS_IFRAME_SRC = "../outputs/figures/phase03_s06_choropleth_mys.html"

display(Markdown(
    f"### Gini\\n\\n"
    f"Interactive map: [`phase03_s06_choropleth_gini.html`]({gini_html.relative_to(PROJECT_ROOT).as_posix()})"
))
try:
    display(IFrame(src=GINI_IFRAME_SRC, width="100%", height=480))
except Exception as exc:
    print(f"(IFrame embed not available in this rendering context: {exc}. "
          f"Open the HTML file directly via the link above.)")

display(Markdown(
    f"### Mean years of schooling\\n\\n"
    f"Interactive map: [`phase03_s06_choropleth_mys.html`]({mys_html.relative_to(PROJECT_ROOT).as_posix()})"
))
try:
    display(IFrame(src=MYS_IFRAME_SRC, width="100%", height=480))
except Exception as exc:
    print(f"(IFrame embed not available in this rendering context: {exc}. "
          f"Open the HTML file directly via the link above.)")
"""

CELL_19_GEOGRAPHY_INTERP = """\
**Observation.** The Gini map shows the highest Gini values concentrated
in southern Africa and across much of Latin America; Europe and the
post-Soviet states form a low-Gini band; Sub-Saharan Africa carries
substantial within-region heterogeneity, partly because Gini coverage is
genuinely sparse for several countries in the region. Mean years of
schooling correlates visibly with high-income geography: North America,
Europe, the Antipodes, and parts of East Asia in the highest band; South
Asia and most of Sub-Saharan Africa in the lowest band; Latin America in
the middle band.

**Explanation.** The visual division between the latest-year and panel-
mean Gini maps is informative: countries that disappear in the
latest-year map (visible in the panel mean but blank in 2021) are
countries with no Gini observation in or near 2021 — predominantly low-
and lower-middle income economies for which household-survey-based Gini
calculations are infrequent or unavailable. This is the same MNAR pattern
quantified in Phase 02 (Gini missingness report, 30% country-year
density, only 3 countries with 34-year coverage), now visible
geographically.

**Implication.** Geography, like income group, is a useful *display* axis
but is not the cleanest *modelling* axis: the regional mean masks
substantial within-region heterogeneity, and the geographic gaps in the
Gini map are exactly where a fixed-effects panel specification will lose
its identifying variation. Phase 04 clustering will work with the panel-
mean values rather than a single year, which the right-panel maps
visualise. Phase 07's policy framing should foreground rather than gloss
over the spatial structure of missingness — the absent countries are not
randomly absent.
"""

CELL_20_SYNTHESIS = """\
## 8. Phase 03 findings — synthesis

Bringing the five diagnostic axes together, nine concrete findings carry
forward into Phases 04, 05, and 07. They are listed in approximate order
of analytical weight rather than by section.

1. **Mean years of schooling is the dominant single predictor.** OLS
   R² = 0.27 against Gini, slope ≈ −1.5 Gini points per additional year
   of schooling, top of the |Pearson r| ranking at −0.52. This is the
   anchor variable for any Phase 05 baseline specification.
2. **The secondary-enrolment trio is arithmetically nested.** VIFs in the
   9,000–40,000 range force a choice: Phase 05 uses one of the three in
   the baseline (most likely `enrol_secondary`) and reserves the gender
   split for heterogeneity-only specifications.
3. **`enrol_primary` is non-monotonic with Gini.** The bivariate slope is
   essentially zero (R² = 0.03) with the wrong sign in linear form because
   high-Gini Latin American countries cluster at universal primary
   enrolment alongside low-Gini high-income countries. A linear primary-
   enrolment term will mislead.
4. **`edu_expenditure_gdp` is a null bivariate result.** R² = 0.05,
   negative LOWESS-vs-linear gap. Public spending share does not predict
   Gini directly — a finding that is itself a Phase 07 narrative point.
5. **Sector shares are not as collinear as expected.** Only
   `agri_value_added_gdp` enters the watch tier (VIF ≈ 5). All three
   sector shares are usable in a single specification, contrary to the
   *a priori* concern about sum-to-100 structural collinearity.
6. **`inflation_cpi` has an extreme right tail (skewness 52).** Treatment
   in Phase 05 will require log transformation, winsorisation, or a
   robust regression diagnostic.
7. **The income-group view of Gini is consistent with Kuznets.** Upper-
   middle income countries are most unequal; high-income least; low-
   income in between. This contradicts the casual "low-income = high-
   inequality" framing.
8. **Secondary enrolment shows clear convergence; mean years of schooling
   does not.** Phase 04 clustering should expect *level* differences in
   `mys` to persist as a cluster-defining feature, while *trajectory* in
   `enrol_secondary` should look more uniform.
9. **Two countries are "Not classified" in the WB income taxonomy.** A
   small structural finding inherited into Phase 02 only at this stage.
   It does not affect the baseline analysis but should be flagged in any
   income-stratified table.
"""

CELL_21_LIMITATIONS = """\
## 9. Limitations

Three known constraints inherited from earlier phases shape the
interpretation of every figure above, and three new caveats surface in
Phase 03 itself.

**Inherited.** First, Gini is the binding sample constraint at 30%
country-year completeness; only 3 of 217 countries have full 1990–2023
coverage (Phase 02). Phase 05's listwise samples will be roughly an
order of magnitude smaller than the 7,378-row panel. Second,
Gini missingness is plausibly MNAR — concentrated in lower-income
countries — so any conclusion drawn from a Gini-conditioned sample
inherits a selection bias that no listwise procedure can correct.
Third, cross-country Gini values are measured heterogeneously
(consumption- vs income-based; pre- vs post-tax) and these differences
will be partly absorbed by the fixed-effects specification in Phase 05
but cannot be eliminated.

**Surfaced in Phase 03.** Fourth, `enrol_primary` is structurally bounded
above by the 100% gross-enrolment ceiling and below by the developing-
world floor, producing a non-linear relationship that any single-term
linear specification will mis-fit. Fifth, the country sample for the
sub-2000 enrolment series is small and shifting; figures that span 1990–
2023 should be interpreted as a description of the available data, not
of the world. Sixth, the LOWESS-vs-linear comparison reported here uses
LOWESS R² as a non-linearity hint, not as a model-selection criterion;
the apparent "non-linearity" of `enrol_primary` could be an artefact of
the ceiling and the regional mixing rather than a true non-linear causal
form.
"""

CELL_22_HANDOFF = """\
## 10. Handoff to Phase 04

Phase 04's country clustering inherits from Phase 03 a clear short list
of cluster-defining features: `mean_years_schooling`, `gdp_per_capita_ppp`
(in log form), `enrol_secondary`, and the three sector-share variables.
The income-group view's Kuznets-type pattern motivates clustering on
*development-level* features rather than on regional dummies. Cluster
profiles should report mean Gini per cluster as a descriptive — not
explanatory — statistic.

Phase 05's econometric modelling inherits a clear specification template:
- baseline: `gini ~ mean_years_schooling + enrol_secondary + log(gdp_ppp) + log(population) + urban_population_pct`
- with sector shares: add `agri/manu/services_value_added_gdp`
- heterogeneity: replace `enrol_secondary` with `enrol_secondary_female`
  and `enrol_secondary_male`
- robustness: substitute `gdp_per_capita_usd` for `gdp_per_capita_ppp`,
  apply a winsorised or log-transformed `inflation_cpi`

Phase 07's synthesis inherits three named threats to identification
that are now grounded in concrete numbers: MNAR Gini missingness
(30% country-year completeness, geographic concentration), Gini
measurement heterogeneity, and the input-vs-attainment distinction
(`edu_expenditure_gdp` null result alongside the strong
`mean_years_schooling` signal).
"""

CELL_23_FOOTER = """\
---

*Notebook generated and executed by `scripts/phase03_s07_build_notebook.py`.
Source data: `data/processed/panel.csv`. Figures and tables under
`outputs/figures/` and `outputs/tables/` are produced by step scripts
`phase03_s02` through `phase03_s06`. Reproduction instructions:*
*`README.md` § Installation and Usage.*
"""


# -----------------------------------------------------------------------------
# Build + execute
# -----------------------------------------------------------------------------

def main() -> int:
    project_root = find_project_root(Path(__file__).resolve().parent)
    notebooks_dir = project_root / "notebooks"
    notebooks_dir.mkdir(exist_ok=True)
    nb_path = notebooks_dir / "03_eda.ipynb"

    # Pre-flight: confirm every referenced artefact exists.
    expected = [
        project_root / "data" / "processed" / "panel.csv",
        project_root / "data" / "raw" / "manifest.yaml",
        project_root / "outputs" / "figures" / "phase03_s02_univariate_distributions.png",
        project_root / "outputs" / "figures" / "phase03_s03_bivariate_gini.png",
        project_root / "outputs" / "figures" / "phase03_s04_correlation_matrix.png",
        project_root / "outputs" / "figures" / "phase03_s05_timeseries_by_region.png",
        project_root / "outputs" / "figures" / "phase03_s05_timeseries_by_income.png",
        project_root / "outputs" / "figures" / "phase03_s06_choropleth_gini.html",
        project_root / "outputs" / "figures" / "phase03_s06_choropleth_mys.html",
        project_root / "outputs" / "tables" / "phase03_s02_skewness.csv",
        project_root / "outputs" / "tables" / "phase03_s03_lowess_vs_linear.csv",
        project_root / "outputs" / "tables" / "phase03_s04_correlation_matrix.csv",
        project_root / "outputs" / "tables" / "phase03_s04_vif.csv",
    ]
    missing = [p for p in expected if not p.exists()]
    if missing:
        print("ERROR: missing required artefacts:", file=sys.stderr)
        for p in missing:
            print(f"  - {p}", file=sys.stderr)
        return 1

    nb = new_notebook()
    cells = [
        new_markdown_cell(CELL_01_TITLE),
        new_markdown_cell(CELL_02_FRAMING),
        new_markdown_cell(CELL_03_SETUP),
        new_code_cell(CELL_04_SETUP_CODE),
        new_markdown_cell(CELL_05_UNIVARIATE),
        new_code_cell(CELL_06_UNIVARIATE_CODE),
        new_markdown_cell(CELL_07_UNIVARIATE_INTERP),
        new_markdown_cell(CELL_08_BIVARIATE),
        new_code_cell(CELL_09_BIVARIATE_CODE),
        new_markdown_cell(CELL_10_BIVARIATE_INTERP),
        new_markdown_cell(CELL_11_CORR),
        new_code_cell(CELL_12_CORR_CODE),
        new_markdown_cell(CELL_13_CORR_INTERP),
        new_markdown_cell(CELL_14_TIMESERIES),
        new_code_cell(CELL_15_TIMESERIES_CODE),
        new_markdown_cell(CELL_16_TIMESERIES_INTERP),
        new_markdown_cell(CELL_17_GEOGRAPHY),
        new_code_cell(CELL_18_GEOGRAPHY_CODE),
        new_markdown_cell(CELL_19_GEOGRAPHY_INTERP),
        new_markdown_cell(CELL_20_SYNTHESIS),
        new_markdown_cell(CELL_21_LIMITATIONS),
        new_markdown_cell(CELL_22_HANDOFF),
        new_markdown_cell(CELL_23_FOOTER),
    ]
    nb.cells = cells
    nb.metadata["kernelspec"] = {
        "display_name": "Python 3",
        "language": "python",
        "name": KERNEL_NAME,
    }

    n_md = sum(1 for c in cells if c.cell_type == "markdown")
    n_code = sum(1 for c in cells if c.cell_type == "code")
    print(f"Built notebook with {len(cells)} cells "
          f"({n_md} markdown + {n_code} code)")

    # Execute against kernel.
    print(f"Executing notebook (timeout {EXECUTE_TIMEOUT}s, kernel={KERNEL_NAME})...")
    ep = ExecutePreprocessor(timeout=EXECUTE_TIMEOUT, kernel_name=KERNEL_NAME)
    try:
        ep.preprocess(nb, {"metadata": {"path": str(notebooks_dir)}})
    except Exception as exc:
        print(f"ERROR: notebook execution failed: {type(exc).__name__}: {exc}",
              file=sys.stderr)
        # Save the partially-executed notebook for debugging.
        with nb_path.open("w", encoding="utf-8") as f:
            nbformat.write(nb, f)
        print(f"  Partial notebook saved to {nb_path} for inspection.",
              file=sys.stderr)
        return 1

    # Persist.
    with nb_path.open("w", encoding="utf-8") as f:
        nbformat.write(nb, f)
    nb_size = nb_path.stat().st_size
    print(f"Saved executed notebook: {nb_path}")
    print(f"  Size: {nb_size:,} bytes ({nb_size/1024:.1f} KB)")

    return 0


if __name__ == "__main__":
    sys.exit(main())
