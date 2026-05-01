"""
update_readme.py - regenerate README.md from a structured template.

Maintenance script per Workflow §8.1: never edit README.md by hand;
edit this script and rerun. Each phase regenerates this script from
scratch with the cumulative project state.

Phase 05 update:
- Phase 05 row in the project status table flipped to complete.
- Findings section updated: "Available now" extended through Phase 05
  with the three-estimator reconciliation headline and the Cluster 1
  heterogeneity finding; "Coming soon" reduced to Phases 06-07.
- Reproduce-the-data-layer section adds phase05_s02 (the data-producing
  step that yields panel_modelling.csv).
- "Last updated" stamp set to 2026-05-02.

Run from project root:
    python scripts/update_readme.py
"""

from __future__ import annotations

import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR.parent))

from src.paths import find_project_root  # noqa: E402


LAST_UPDATED = "2026-05-02"

# Phase status table rows, in order. Status: complete | pending.
PHASE_STATUS = [
    ("00", "Scope & Setup", "complete"),
    ("01", "Data Collection", "complete"),
    ("02", "Data Cleaning & Integration", "complete"),
    ("03", "Exploratory Data Analysis", "complete"),
    ("04", "Country Clustering", "complete"),
    ("05", "Econometric Modelling", "complete"),
    ("06", "Predictive Modelling & Interpretability", "pending"),
    ("07", "Synthesis & Policy Discussion", "pending"),
]

STATUS_GLYPH = {"complete": "\u2705 Complete", "pending": "\u23f3 Pending"}


def render_status_table() -> str:
    rows = ["| Phase | Title | Status |", "|---|---|---|"]
    for n, title, status in PHASE_STATUS:
        rows.append(f"| {n} | {title} | {STATUS_GLYPH[status]} |")
    return "\n".join(rows)


HEADER = """\
# Education and Income Inequality
### A Cross-Country Panel Analysis

> Quantifying the relationship between education and income inequality using panel econometrics and interpretable machine learning.

*Last updated: {last_updated}*
"""


OVERVIEW = """\
## Overview

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
## Research Questions

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
## Data

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
## Methods

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
## Tech Stack

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
## Project Structure

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
\u2502   \u2514\u2500\u2500 models/                #   trained models (Phase 06+)
\u2514\u2500\u2500 docs/
    \u251c\u2500\u2500 project_scope.md       # Canonical project scope
    \u2514\u2500\u2500 phase_summaries/       # Per-phase handoff files (gitignored)
```
"""


INSTALLATION = """\
## Installation and Usage

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
```

### 5. View the notebooks

Open `notebooks/` in Jupyter or VS Code, select the `p4_education`
kernel, and execute notebooks in numerical order (01 \u2192 07).
"""


def render_project_status() -> str:
    return f"""\
## Project Status

{render_status_table()}
"""


FINDINGS = """\
## Findings

### Available now (descriptive layer & explanatory layer, Phases 01\u201305)

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

### Coming soon (predictive modelling & policy synthesis, Phases 06\u201307)

- *Top drivers of inequality identified by SHAP (Phase 06)*
- *Cross-method comparison and policy-relevant takeaways (Phase 07)*
"""


LIMITATIONS = """\
## Limitations and Future Work

*A detailed discussion will appear after Phase 07. Known constraints by design:*

- Observational data \u2014 causal claims are made only with explicit caveats
- Cross-country Gini measurement is heterogeneous (consumption- vs income-based surveys)
- Systematic missingness under-represents low-income countries (plausibly MNAR)
- IV, DiD, and dynamic panel estimators are out of scope; framed as next steps
"""


DOCUMENTATION = """\
## Documentation

- [Project Scope](docs/project_scope.md) \u2014 Canonical specification
- [Project Log](PROJECT_LOG.md) \u2014 Append-only record of decisions and progress
- Phase summaries live in `docs/phase_summaries/` (gitignored, internal use)
"""


AUTHOR = """\
## Author

**Kota** \u2014 [GitHub @kota2003](https://github.com/kota2003)

Part of a data science portfolio for roles in applied analytics and applied research.
"""


def render_readme() -> str:
    sections = [
        HEADER.format(last_updated=LAST_UPDATED),
        OVERVIEW,
        RESEARCH_QUESTIONS,
        DATA,
        METHODS,
        TECH_STACK,
        PROJECT_STRUCTURE,
        INSTALLATION,
        render_project_status(),
        FINDINGS,
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
    print(f"  last updated: {LAST_UPDATED}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
