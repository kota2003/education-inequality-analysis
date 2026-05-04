"""
update_readme.py - regenerate README.md from a structured template.

Maintenance script per Workflow §8.1: never edit README.md by hand;
edit this script and rerun. Each phase regenerates this script from
scratch with the cumulative project state.

Phase 07 portfolio-polish update v3 (post-close):
- Section header emojis preserved.
- Table of Contents (12 entries) preserved between subtitle and
  Overview.
- Documentation section preserved with docs/findings.md and
  docs/methodology.md links.
- All Findings prose entries (Phases 01-07) preserved EXCEPT a
  single targeted correction in the Phase 06 entry: the trained
  models reference no longer points to outputs/models/ (which is
  gitignored due to phase06_s04_rf.joblib being ~55 MB). Models
  are described as reproducible via the corresponding step scripts,
  matching the gitignore policy and preserving portfolio integrity.
- All inline figure embeds (six raw GitHub URLs across Phase 04 / 05
  / 06 / 07 entries) preserved unchanged.
- Limitations and Future Work section preserved in its locked Phase
  07 Decision 5(b) form.
- "Last updated" stamp set to 2026-05-04.

Run from project root:
    python scripts/update_readme.py
"""

from __future__ import annotations

import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR.parent))

from src.paths import find_project_root  # noqa: E402


LAST_UPDATED = "2026-05-04"

# Raw GitHub URL prefix for embedded figures in the Findings section.
RAW_URL_BASE = (
    "https://raw.githubusercontent.com/kota2003/"
    "education-inequality-analysis/main/outputs/figures"
)

# Phase status table rows, in order. Status: complete | pending.
PHASE_STATUS = [
    ("00", "Scope & Setup", "complete"),
    ("01", "Data Collection", "complete"),
    ("02", "Data Cleaning & Integration", "complete"),
    ("03", "Exploratory Data Analysis", "complete"),
    ("04", "Country Clustering", "complete"),
    ("05", "Econometric Modelling", "complete"),
    ("06", "Predictive Modelling & Interpretability", "complete"),
    ("07", "Synthesis & Policy Discussion", "complete"),
]

STATUS_GLYPH = {"complete": "\u2705 Complete", "pending": "\u23f3 Pending"}


def render_status_table() -> str:
    rows = ["| Phase | Title | Status |", "|---|---|---|"]
    for n, title, status in PHASE_STATUS:
        rows.append(f"| {n} | {title} | {STATUS_GLYPH[status]} |")
    return "\n".join(rows)


HEADER = """\
# \U0001f4d8 Education and Income Inequality
### A Cross-Country Panel Analysis

> Quantifying the relationship between education and income inequality using panel econometrics and interpretable machine learning.

*Last updated: {last_updated}*
"""


TOC = """\
## \U0001f4cb Table of Contents

1. [Overview](#-overview)
2. [Research Questions](#-research-questions)
3. [Data](#%EF%B8%8F-data)
4. [Methods](#-methods)
5. [Tech Stack](#%EF%B8%8F-tech-stack)
6. [Project Structure](#-project-structure)
7. [Installation and Usage](#-installation-and-usage)
8. [Project Status](#-project-status)
9. [Findings](#-findings)
10. [Limitations and Future Work](#%EF%B8%8F-limitations-and-future-work)
11. [Documentation](#-documentation)
12. [Author](#-author)
"""


OVERVIEW = """\
## \U0001f4cc Overview

Education is widely considered a lever for reducing income inequality, but the
empirical relationship is complicated by confounders such as economic
development, demographics, and policy environment. This project quantifies the
relationship across countries and over time using a combination of panel
econometrics and interpretable machine learning.

The analysis is structured in three layers:

- **Descriptive** \u2014 How do education and inequality look across countries and
  over time? Do countries cluster into distinct education-inequality regimes?
- **Explanatory** \u2014 After controlling for economic and structural confounders,
  what is the association between different levels of education and the Gini
  coefficient?
- **Predictive** \u2014 Can a flexible machine-learning model predict inequality
  from education and controls? Which features drive its predictions, and do
  they agree with the econometric estimates?

Causal identification is discussed but not claimed; the project sets explicit
boundaries on what can and cannot be concluded from observational cross-country
panel data.
"""


RESEARCH_QUESTIONS = """\
## \U0001f50d Research Questions

1. Is higher educational attainment associated with lower income inequality
   across countries?
2. Which level of education (primary, secondary, tertiary) shows the strongest
   association with inequality?
3. Does the relationship survive controlling for GDP and other structural
   covariates?
4. Are the relationships heterogeneous across income groups or regions?
5. How do conclusions from linear panel models compare with those from
   flexible machine-learning models?
6. What causal claims can and cannot be made from this analysis?
"""


DATA = """\
## \U0001f5c4\ufe0f Data

| Source | Use |
|---|---|
| World Bank \u2014 World Development Indicators | Gini, GDP, unemployment, population, urbanisation, trade openness, government expenditure, sector shares, inflation |
| UNESCO Institute for Statistics (mirrored via WB) | Primary / secondary / tertiary enrolment, gender-disaggregated enrolment, education expenditure |
| UNDP Human Development Report | Mean years of schooling |
| World Bank country metadata | Region, income-group classification |

- **Unit of observation:** Country \u00d7 Year (unbalanced panel)
- **Coverage:** 1990 \u2013 2023, 217 sovereign and near-sovereign states
- **Integrated panel:** 7,378 rows \u00d7 24 columns (217 \u00d7 34, fully reindexed)
- **Country-level analytical typology:** 167 countries (Phase 04, 2010\u20132019 means)
- **Analytical sample:** 1,000\u20133,000 country-year rows depending on specification (Gini-binding)
"""


METHODS = """\
## \U0001f9ea Methods

**Descriptive layer**
- Distribution and time-trend analysis
- Correlation matrix with VIF diagnostics
- Geographic visualisation (choropleth)
- K-means + Ward hierarchical clustering of countries (Phase 04)

**Explanatory layer**
- Pooled OLS baseline
- Fixed Effects (country + year) with clustered standard errors
- Random Effects + Hausman test for specification choice
- Heterogeneity analysis by income group / region / cluster

**Predictive layer**
- Random Forest and Gradient Boosting (XGBoost)
- Time-aware cross-validation to avoid leakage
- SHAP for global and local feature attribution
- Comparison against linear baselines

Formal causal identification (IV, DiD, synthetic control) is out of scope but
discussed as future work.
"""


TECH_STACK = """\
## \U0001f6e0\ufe0f Tech Stack

- **Language:** Python 3.11
- **Data handling:** pandas, numpy, pycountry
- **Econometrics:** statsmodels, linearmodels
- **Machine learning:** scikit-learn, xgboost, shap
- **Visualisation:** matplotlib, seaborn, plotly
- **Notebook tooling:** jupyter, nbformat, nbconvert (notebooks built programmatically)
- **Environment:** conda + pinned `requirements.txt`
- **Version control:** git with per-phase branches
"""


PROJECT_STRUCTURE = """\
## \U0001f4c1 Project Structure

```
education-inequality-analysis/
\u251c\u2500\u2500 README.md                  # This file (generated by scripts/update_readme.py)
\u251c\u2500\u2500 PROJECT_LOG.md             # Append-only decision log
\u251c\u2500\u2500 requirements.txt           # Pinned dependencies
\u251c\u2500\u2500 .python-version            # Python version marker
\u251c\u2500\u2500 .gitignore
\u251c\u2500\u2500 data/
\u2502   \u251c\u2500\u2500 raw/                   # Original data + manifest.yaml (gitignored)
\u2502   \u2514\u2500\u2500 processed/             # panel.csv, country_features*.csv, panel_modelling.csv
\u251c\u2500\u2500 notebooks/                 # Phase-aligned portfolio notebooks (01..07)
\u251c\u2500\u2500 src/                       # Reusable functions and classes
\u2502   \u251c\u2500\u2500 paths.py               #   project-root locator
\u2502   \u251c\u2500\u2500 manifest.py            #   data source registry accessors
\u2502   \u251c\u2500\u2500 country_metadata.py    #   WB country metadata loader
\u2502   \u251c\u2500\u2500 io_utils.py            #   encoding-fallback CSV reader
\u2502   \u2514\u2500\u2500 log_utils.py           #   PROJECT_LOG idempotent-append helper
\u251c\u2500\u2500 scripts/                   # Step scripts (phaseXX_sYY_*.py) + maintenance utilities
\u251c\u2500\u2500 outputs/
\u2502   \u251c\u2500\u2500 figures/               #   phase-prefixed figures
\u2502   \u251c\u2500\u2500 tables/                #   phase-prefixed CSV reports
\u2502   \u2514\u2500\u2500 models/                #   trained models (gitignored, reproducible via scripts)
\u2514\u2500\u2500 docs/
    \u251c\u2500\u2500 project_scope.md       # Canonical project scope
    \u251c\u2500\u2500 findings.md            # Condensed standalone findings narrative
    \u251c\u2500\u2500 methodology.md         # Workflow and conventions reference
    \u2514\u2500\u2500 phase_summaries/       # Per-phase handoff files (gitignored)
```
"""


INSTALLATION = """\
## \U0001f680 Installation and Usage

### 1. Clone the repository

```bash
git clone https://github.com/kota2003/education-inequality-analysis.git
cd education-inequality-analysis
```

### 2. Create the conda environment

```bash
conda create -n p4_education python=3.11 -y
conda activate p4_education
pip install -r requirements.txt
```

### 3. Register the Jupyter kernel

```bash
python -m ipykernel install --user --name p4_education \\
    --display-name "Python (p4_education)"
```

### 4. Reproduce the data layer

`data/raw/` and `data/processed/` are gitignored. Regenerate them by
running the step scripts in order:

```bash
# Phase 01 - raw data acquisition
python scripts/phase01_s01_design_manifest.py
python scripts/phase01_s02_download_world_bank.py
python scripts/phase01_s04_download_undp_hdr.py
python scripts/phase01_s05_inspect_coverage.py

# Phase 02 - panel construction
python scripts/phase02_s02_build_intermediate_long.py
python scripts/phase02_s03_concat_master_long.py
python scripts/phase02_s04_pivot_to_wide_panel.py
python scripts/phase02_s05_missingness_report.py

# Phase 04 - country-level feature matrix (depends on panel.csv)
python scripts/phase04_s02_build_country_features.py

# Phase 05 - modelling-ready panel (depends on panel.csv + cluster_assignments.csv)
python scripts/phase05_s02_build_modelling_data.py

# Phase 06 - ML-ready panel (depends on panel_modelling.csv + cluster_assignments.csv)
python scripts/phase06_s02_build_ml_data.py
```

### 5. View the notebooks

Open `notebooks/` in Jupyter or VS Code, select the `p4_education`
kernel, and execute notebooks in numerical order (01 \u2192 07).
"""


def render_project_status() -> str:
    return f"""\
## \U0001f4ca Project Status

{render_status_table()}
"""


# Findings section. Embedded figures are placed at the end of each Phase
# 04 / 05 / 06 / 07 entry as visual punctuation, with raw GitHub URL
# substituted via `.format(raw_url_base=RAW_URL_BASE)` in render_readme().
# v3 update: Phase 06 entry's "trained models" reference no longer
# points to outputs/models/ (gitignored due to ~55 MB RF .joblib).
# Models are described as reproducible via the corresponding step
# scripts, matching the gitignore policy.
FINDINGS = """\
## \U0001f3af Findings

### Available now (Phases 01\u201307)

- **Phase 01** \u2014 [`01_data_collection.ipynb`](notebooks/01_data_collection.ipynb)
  documents the raw layer: a machine-readable manifest of 19 declared variables
  across three sources (WB WDI, WB country metadata, UNDP HDR), per-variable
  coverage characterisation, and source reconciliation. Gini emerges as the
  binding constraint at 30% country-year completeness.
- **Phase 02** \u2014 [`02_data_cleaning.ipynb`](notebooks/02_data_cleaning.ipynb)
  produces the integrated 7,378-row \u00d7 24-column panel covering 217 countries
  \u00d7 34 years (1990\u20132023). Missingness is profiled per-variable, jointly across
  candidate specifications, and visually as a country \u00d7 year heatmap.
  Removing Gini from the baseline specification doubles the listwise sample
  (1,423 \u2192 3,041 rows) \u2014 the quantitative anchor for the project's MNAR caveat.
- **Phase 03** \u2014 [`03_eda.ipynb`](notebooks/03_eda.ipynb) characterises the
  analytical panel along five axes: univariate distributions, bivariate Gini
  relationships, multicollinearity, time trends by region and income group,
  and country-level geography. Headline findings: mean years of schooling is
  the strongest single linear predictor of Gini (r = \u22120.52, OLS R\u00b2 = 0.27);
  the three secondary-enrolment variables are arithmetically nested with VIFs
  in the 9,000\u201340,000 range, forcing a one-of-three choice in Phase 05; and
  the income-group view of Gini is consistent with a Kuznets-type inverted-U
  in which upper-middle income countries \u2014 not low income \u2014 are the most
  unequal.
- **Phase 04** \u2014 [`04_country_clustering.ipynb`](notebooks/04_country_clustering.ipynb)
  builds a data-driven typology of 167 countries via K=3 K-means clustering
  on seven standardised development features (`mean_years_schooling`,
  log `gdp_per_capita_ppp`, `enrol_secondary`, three sector shares,
  `urban_population_pct`). The typology **independently re-discovers the
  Kuznets inverted-U** flagged in Phase 03 finding #4: Cluster 1
  (Middle-development / Kuznets transition, n=59) has the highest mean
  Gini at 39.05, surpassing both Cluster 0 (Low-development / Sub-Saharan-led,
  n=40, mean Gini 38.24) and Cluster 2 (High-development / mature economies,
  n=68, mean Gini 34.72). Cross-algorithm agreement (K-means K=3 vs Ward
  K=3 Adjusted Rand Index = 0.65) is substantial; PC1 alone captures 63.2%
  of variance across the seven features, indicating that development is
  approximately one-dimensional in this space. Cluster assignments are
  exposed in [`outputs/tables/phase04_s04_cluster_assignments.csv`](outputs/tables/phase04_s04_cluster_assignments.csv)
  and feed Phase 05 robustness specifications as cluster fixed effects.

  ![PCA scatter of country-level development features]({raw_url_base}/phase04_s06_pca_scatter.png)

  *Figure 1 \u2014 PCA scatter of 167 standardised country-level features, colour-coded by K=3 K-means cluster assignment. PC1 captures 63.2% of variance and PC1+PC2 reach 79.2%, indicating that development is approximately one-dimensional in this seven-feature space.*

- **Phase 05** \u2014 [`05_econometric_modelling.ipynb`](notebooks/05_econometric_modelling.ipynb)
  estimates the education\u2013Gini relationship across three identification
  strategies on the 1,642-country-year analytical sample (153 countries,
  Spec A listwise complete): Pooled OLS, two-way Fixed Effects (country +
  year), and Random Effects, all with country-clustered standard errors.
  The coefficient on `mean_years_schooling` attenuates from \u22121.33\\*\\*\\*
  (Pooled OLS, between-country identification) to a statistically null
  \u22120.38 (FE, within-country only) and partially recovers to \u22120.69\\*
  under RE (\u03b8 = 0.82 GLS combination). A mid-phase adaptive override
  (PROJECT_LOG Step 07b) replaced the pre-registered \"Hausman picks one
  estimator\" rule with tri-headline reporting after the Mundlak
  alternative-Hausman test returned conflicting answers under cluster-
  robust SE. Heterogeneity analysis surfaces the substantive
  finding: **Cluster 1 (middle-development / Kuznets transition) shows
  a within-country slope of \u22121.19 (p = 0.010), robust to BRA/ZAF/MEX/ARG
  boundary reassignment (\u22121.15\\*\\*, p = 0.008)**. This is the econometric
  corroboration, from a within-country identification strategy, of the
  Phase 04 Kuznets finding. The robustness suite confirms 2010\u20132019
  sub-period stability (RE \u22120.74\\*) and surfaces a non-monotonic MNAR
  pattern (high-income microstates over-represented in the excluded
  sample, \u03c7\u00b2 p = 0.0017 country-level). Coefficient tables and per-
  cluster slopes are exposed in [`outputs/tables/`](outputs/tables/);
  three figures (forest plot, per-cluster bar, MNAR contingency) are in
  [`outputs/figures/`](outputs/figures/).

  ![Three-estimator forest plot for the mys coefficient]({raw_url_base}/phase05_s08_forest_plot.png)

  *Figure 2 \u2014 Three-estimator forest plot for the `mean_years_schooling` coefficient on Spec A (1,642 country-years from 153 countries). Pooled OLS \u22121.33\\*\\*\\* (between-country identification) attenuates to FE \u22120.38 (within-country only, ns) and partially recovers to RE \u22120.69\\* under \u03b8 = 0.82 GLS combination.*

  ![Per-cluster within-country slopes]({raw_url_base}/phase05_s08_cluster_slopes.png)

  *Figure 3 \u2014 Per-cluster RE Spec C within-country slopes (delta-method SE). Only Cluster 1 (Kuznets transition) is statistically detectable at p < 0.05: slope = \u22121.19, p = 0.010, 95% CI [\u22122.09, \u22120.28]. The Cluster 1 finding is robust to BRA/ZAF/MEX/ARG boundary reassignment (\u22121.15\\*\\*).*

- **Phase 06** \u2014 [`06_predictive_modelling.ipynb`](notebooks/06_predictive_modelling.ipynb)
  trains tree-based machine-learning models (Random Forest, XGBoost) plus a
  Ridge baseline on the same 1,642-country-year Spec A sample, with a temporal
  holdout (year \u2264 2018 train, 2019\u20132023 test) and pre-registered
  hyperparameter grids (RandomizedSearchCV, n_iter=50, TimeSeriesSplit-5
  folds). Test R\u00b2 climbs from 0.426 (Ridge) to 0.706 (RF) to **0.733
  (XGBoost)**; the +0.28 R\u00b2 jump from Ridge to RF is the non-linear and
  interaction signal that linear panel models cannot capture. TreeSHAP
  attributions on the test set produce **identical top-5 global rankings
  in both tree models**: `mean_years_schooling` #1, `log_gdp_per_capita_ppp`
  #2, `enrol_secondary` #3, `trade_openness` #4, `gov_expenditure_gdp` #5.
  Mean signed SHAP for mys is \u22121.13 (RF) / \u22121.06 (XGB), close to Phase 05
  Pooled OLS (\u22121.33) and Phase 06 Ridge raw-scale (\u22121.42), well above Phase
  05 FE (\u22120.38) \u2014 ML performs mixed-identification estimation. **The
  headline cross-method comparison: Phase 06 corroborates the Phase 05
  Cluster 1 (Kuznets-transition) finding and strengthens it.** The Phase
  06 SHAP-on-mys regression slope within Cluster 1 is \u22121.92 (RF) and \u22122.00
  (XGB), roughly 1.7\u00d7 the Phase 05 RE Spec C linear panel estimate of
  \u22121.19\\*\\*. Spearman \u03c1 between Phase 05 |coef| ranking and Phase 06 mean
  |SHAP| ranking on the 5 common Spec A features = +0.30 for both models;
  the rank disagreement below mys reflects Phase 05 sampling noise (4 of 5
  coefficients have p \u2265 0.19) rather than a methodological clash. **Critical
  caveat from the boundary-case country holdout** (BRA / ZAF / MEX / ARG
  removed from training, n=59 evaluation cy): test R\u00b2 collapses to **\u22122.4**,
  RMSE rises to ~10, and mys mean signed SHAP **flips sign** for BRA / ZAF /
  MEX. This is the strongest internal evidence that Phase 06 in-sample R\u00b2
  reflects within-distribution interpolation and that SHAP attribution is
  correlation rather than causation \u2014 directly framing the Phase 07
  identification discussion. SHAP CSVs and 7 figures (summary beeswarms,
  dependence top-3, Brazil 2015 waterfall, ranking comparison, per-cluster
  slopes) are in [`outputs/tables/`](outputs/tables/) and
  [`outputs/figures/`](outputs/figures/); trained Random Forest and XGBoost
  models (`.joblib`, ~55 MB and ~2 MB respectively) are reproducible via
  [`scripts/phase06_s04_random_forest.py`](scripts/phase06_s04_random_forest.py)
  and [`scripts/phase06_s05_xgboost.py`](scripts/phase06_s05_xgboost.py).

  ![XGBoost TreeSHAP global summary]({raw_url_base}/phase06_s06_shap_summary_xgb.png)

  *Figure 4 \u2014 XGBoost TreeSHAP global summary on the test set. `mean_years_schooling` ranks #1 by mean |SHAP|; the top-5 ranking is identical between Random Forest and XGBoost (mys, log_gdp_per_capita_ppp, enrol_secondary, trade_openness, gov_expenditure_gdp).*

  ![Per-cluster SHAP-on-mys slopes vs Phase 05 RE estimate]({raw_url_base}/phase06_s07_per_cluster_slopes.png)

  *Figure 5 \u2014 Per-cluster SHAP-on-mys regression slope (Random Forest and XGBoost) compared with the Phase 05 RE Spec C linear panel estimate. Cluster 1 (Kuznets transition, n=105 test cy): RF \u22121.92, XGB \u22122.00 \u2014 roughly 1.7\u00d7 the Phase 05 estimate of \u22121.19. Cluster 2 (mature economies, n=188) shows a meaningful negative slope where Phase 05 found none.*

- **Phase 07** \u2014 [`07_synthesis_and_policy.ipynb`](notebooks/07_synthesis_and_policy.ipynb)
  integrates Phases 03\u201306 into a single portfolio narrative anchored on
  the project's central converged finding: across three independent
  estimation strategies, the within-country slope of Gini on mean years
  of schooling in the **Cluster 1 (middle-development / Kuznets-transition)
  regime** is \u22121.19 (Phase 05 RE Spec C, p = 0.010), \u22121.92 (Phase 06 RF
  SHAP-on-mys), and \u22122.00 (Phase 06 XGB SHAP-on-mys). The aggregate
  convergence across all seven Phase 03\u201306 estimators (Pearson r \u22120.52;
  Pooled OLS / FE / RE Spec A: \u22121.33\\*\\*\\* / \u22120.38 ns / \u22120.69\\*; Ridge /
  RF / XGB raw-scale or signed SHAP: \u22121.42 / \u22121.13 / \u22121.06) is visualised
  as a forest plot in
  [`outputs/figures/phase07_s03_convergence.png`](outputs/figures/phase07_s03_convergence.png),
  with the underlying long-format synthesis table at
  [`outputs/tables/phase07_s02_synthesis_table.csv`](outputs/tables/phase07_s02_synthesis_table.csv).
  **The chained finding is association, not causation.** The notebook's
  \u00a76 generalises this position into a single \"what would it take to
  claim causation?\" framework that threads four threats \u2014 omitted
  variable bias, reverse causality, measurement error, and selection bias
  from Gini-reporting \u2014 through one unified discussion, anchored on the
  Phase 06 boundary-case holdout (BRA / ZAF / MEX / ARG, test R\u00b2 = \u22122.4
  with mys SHAP sign-flip for three of four) as the strongest internal
  evidence that in-sample attribution is within-distribution interpolation
  rather than transportable causal structure. \u00a77 specifies the natural
  follow-on identification strategy in depth \u2014 **synthetic control on
  Brazil, Mexico, and South Africa** with concrete treatment events
  (BRA 1996 LDB / 1998 FUNDEF, MEX 1993 ANMEB, ZAF 1996 SASA) and a donor
  pool of \u224855 remaining Cluster 1 countries \u2014 alongside briefer outlines
  of IV / RD / DiD as alternative routes. \u00a78 provides **conditional**
  policy framing for Cluster 1 only (Clusters 0 and 2 excluded because
  their within-country slopes are statistically null); the project closes
  at git tag `v1.0`.

  ![Cross-method convergence forest plot]({raw_url_base}/phase07_s03_convergence.png)

  *Figure 6 \u2014 Cross-method aggregate convergence: seven Phase 03\u201306 estimators of the mys-Gini relationship on a single x-axis. All point estimates are negative; the six regression-style estimates from Phases 05\u201306 sit in the \u22120.38 to \u22121.42 Gini-points-per-mys-year range, with the Phase 03 univariate Pearson r at \u22120.52 (unitless correlation, shown alongside for direction). Phase 05 95% CIs make the within-vs-between identification structure visible: Pooled OLS and RE intervals are strictly below zero, while two-way FE crosses zero.*
"""


LIMITATIONS = """\
## \u26a0\ufe0f Limitations and Future Work

This project documents a robust negative association between mean years of
schooling and the Gini coefficient in cross-country panel data 1990\u20132023,
with the relationship strongest within the 59-country middle-development /
Kuznets-transition cluster identified by Phase 04. The findings characterise
the *Gini-reporting subpopulation* (153 countries, 1,642 country-years
under Spec A) rather than the global universe of 217 World Bank countries,
and the project does not claim causal identification. The four threats
discussed in notebook \u00a76 \u2014 omitted variable bias, reverse causality,
measurement error, and selection bias from Gini reporting \u2014 interact in
ways that observational panel methods cannot resolve, and the Phase 06
boundary-case holdout (test R\u00b2 = \u22122.4 on BRA / ZAF / MEX / ARG with mys
SHAP sign-flip for three of four) is the strongest internal evidence that
the in-sample attribution reflects within-distribution interpolation
rather than transportable causal structure. See
[`07_synthesis_and_policy.ipynb`](notebooks/07_synthesis_and_policy.ipynb)
\u00a76 for the full discussion.

### Future Work

- **Synthetic control on Brazil, Mexico, and South Africa** \u2014 the natural
  follow-on study, motivated directly by the Phase 06 boundary-case
  caveat. Treatment events: BRA 1996 LDB / 1998 FUNDEF, MEX 1993 ANMEB,
  ZAF 1996 SASA. Donor pool: \u224855 remaining Cluster 1 countries.
  Inference via in-place placebo permutation. Specified in detail in
  notebook \u00a77.
- **Instrumental variables on compulsory-schooling reform timing**,
  building from country-year reform-event datasets in the
  development-economics literature. Targets the omitted-variable threat
  that synthetic control does not directly address.
- **Regression discontinuity at policy thresholds** within single
  Cluster 1 countries with sharp rule breaks, using sub-national
  administrative microdata.
- **Difference-in-differences around education-spending changes**, using
  Phase 05 Spec B's `gov_expenditure_gdp` series and event studies
  around discrete spending-rule changes.

### Structural Limitations

- **MNAR selection on Gini reporting.** The headline coefficient describes
  countries with sustained Gini reporting; the 50 listwise-excluded
  countries (conflict-affected states such as HTI / SOM / SSD / SYR / VEN /
  YEM, small WB-only territories, and persistent statistical-capacity
  cases such as PRK / ZMB) are systematically under-represented.
- **Cross-country Gini measurement heterogeneity.** Consumption-based and
  income-based surveys are mixed across the panel; cross-cluster magnitude
  comparisons are not insulated against the cross-method differences.
- **Country-level aggregation.** All Phase 05 / 06 estimates average over
  sub-national heterogeneity that is meaningful for inequality (Brazil,
  India, China, Indonesia, the United States).
- **Temporal extrapolation cost.** The Phase 06 TimeSeriesSplit minus
  random-KFold RMSE gap (+0.41 to +0.60) reflects the structural
  difficulty of year-out-of-sample prediction on panel data;
  extrapolation to post-2023 country-years introduces a known degradation.
"""


DOCUMENTATION = """\
## \U0001f4c4 Documentation

| Document | Description |
|---|---|
| [`docs/project_scope.md`](docs/project_scope.md) | Canonical project specification (Phase 00 baseline) |
| [`docs/findings.md`](docs/findings.md) | Condensed standalone findings narrative \u2014 5 findings with decision anchors |
| [`docs/methodology.md`](docs/methodology.md) | Workflow, conventions, and `src/` promotion discipline |
| [`PROJECT_LOG.md`](PROJECT_LOG.md) | Append-only record of design decisions across all eight phases |

Phase summaries live in `docs/phase_summaries/` (gitignored, internal handoff documents).
"""


AUTHOR = """\
## \U0001f464 Author

**Kota** \u2014 [GitHub @kota2003](https://github.com/kota2003)

Part of a data science portfolio for roles in applied analytics and applied research.
"""


def render_readme() -> str:
    sections = [
        HEADER.format(last_updated=LAST_UPDATED),
        TOC,
        OVERVIEW,
        RESEARCH_QUESTIONS,
        DATA,
        METHODS,
        TECH_STACK,
        PROJECT_STRUCTURE,
        INSTALLATION,
        render_project_status(),
        FINDINGS.format(raw_url_base=RAW_URL_BASE),
        LIMITATIONS,
        DOCUMENTATION,
        AUTHOR,
    ]
    return "\n".join(s.rstrip() + "\n" for s in sections)


def main() -> int:
    project_root = find_project_root(SCRIPT_DIR)
    out_path = project_root / "README.md"

    content = render_readme()
    out_path.write_text(content, encoding="utf-8")

    n_lines = content.count("\n")
    print(f"Wrote: {out_path.relative_to(project_root)}")
    print(f"  size: {len(content):,} characters, {n_lines} lines")

    n_complete = sum(1 for _, _, s in PHASE_STATUS if s == "complete")
    n_pending = sum(1 for _, _, s in PHASE_STATUS if s == "pending")
    print(f"  phase status: {n_complete} complete, {n_pending} pending")
    print(f"  embedded figures: {content.count('![')} (raw GitHub URLs)")
    print(f"  TOC entries: {sum(1 for line in TOC.splitlines() if line and line[0].isdigit())}")
    print(f"  last updated: {LAST_UPDATED}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
