"""
scripts/update_readme.py

Regenerate README.md from project state.

Run from the repository root:
    python scripts/update_readme.py

This script is the single source of truth for README content.
Do not edit README.md by hand; update the section functions below and
re-run the script.

Typical maintenance workflow:
    1. Update PHASE_STATUS when a phase completes.
    2. Update relevant section function(s) with new findings or changes.
    3. Run this script.
    4. Commit both the script change and the regenerated README.md.
"""

from datetime import date
from pathlib import Path

# ============================================================
# Project metadata — update these as the project evolves
# ============================================================

PROJECT_TITLE = "Education and Income Inequality"
PROJECT_SUBTITLE = "A Cross-Country Panel Analysis"
TAGLINE = (
    "Quantifying the relationship between education and income inequality "
    "using panel econometrics and interpretable machine learning."
)

AUTHOR_DISPLAY_NAME = "Kota"
GITHUB_HANDLE = "kota2003"
REPO_NAME = "education-inequality-analysis"

REPO_URL = f"https://github.com/{GITHUB_HANDLE}/{REPO_NAME}"
GITHUB_PROFILE_URL = f"https://github.com/{GITHUB_HANDLE}"

# Phase status: update as each phase completes.
# Status values: "complete", "in_progress", "pending"
PHASE_STATUS = {
    0: ("Scope & Setup", "complete"),
    1: ("Data Collection", "complete"),
    2: ("Data Cleaning & Integration", "complete"),
    3: ("Exploratory Data Analysis", "pending"),
    4: ("Country Clustering", "pending"),
    5: ("Econometric Modelling", "pending"),
    6: ("Predictive Modelling & Interpretability", "pending"),
    7: ("Synthesis & Policy Discussion", "pending"),
}

STATUS_ICON = {
    "complete": "✅",
    "in_progress": "🔄",
    "pending": "⏳",
}

LAST_UPDATED = date.today().isoformat()


# ============================================================
# Section builders
# ============================================================

def header() -> str:
    return (
        f"# {PROJECT_TITLE}\n"
        f"### {PROJECT_SUBTITLE}\n\n"
        f"> {TAGLINE}\n\n"
        f"*Last updated: {LAST_UPDATED}*"
    )


def overview() -> str:
    return """## Overview

Education is widely considered a lever for reducing income inequality, but the
empirical relationship is complicated by confounders such as economic
development, demographics, and policy environment. This project quantifies the
relationship across countries and over time using a combination of panel
econometrics and interpretable machine learning.

The analysis is structured in three layers:

- **Descriptive** — How do education and inequality look across countries and
  over time? Do countries cluster into distinct education-inequality regimes?
- **Explanatory** — After controlling for economic and structural confounders,
  what is the association between different levels of education and the Gini
  coefficient?
- **Predictive** — Can a flexible machine-learning model predict inequality
  from education and controls? Which features drive its predictions, and do
  they agree with the econometric estimates?

Causal identification is discussed but not claimed; the project sets explicit
boundaries on what can and cannot be concluded from observational cross-country
panel data."""


def research_questions() -> str:
    return """## Research Questions

1. Is higher educational attainment associated with lower income inequality
   across countries?
2. Which level of education (primary, secondary, tertiary) shows the strongest
   association with inequality?
3. Does the relationship survive controlling for GDP and other structural
   covariates?
4. Are the relationships heterogeneous across income groups or regions?
5. How do conclusions from linear panel models compare with those from
   flexible machine-learning models?
6. What causal claims can and cannot be made from this analysis?"""


def data_section() -> str:
    return """## Data

| Source | Use |
|---|---|
| World Bank — World Development Indicators | Gini, GDP, unemployment, population, urbanisation, trade openness, government expenditure, sector shares, inflation |
| UNESCO Institute for Statistics (mirrored via WB) | Primary / secondary / tertiary enrolment, gender-disaggregated enrolment, education expenditure |
| UNDP Human Development Report | Mean years of schooling |
| World Bank country metadata | Region, income-group classification |

- **Unit of observation:** Country × Year (unbalanced panel)
- **Coverage:** 1990 – 2023, 217 sovereign and near-sovereign states
- **Integrated panel:** 7,378 rows × 24 columns (217 × 34, fully reindexed)
- **Analytical sample:** 1,000–3,000 country-year rows depending on specification (Gini-binding)"""


def methods_section() -> str:
    return """## Methods

**Descriptive layer**
- Distribution and time-trend analysis
- Correlation matrix with VIF diagnostics
- Geographic visualisation (choropleth)
- K-means / hierarchical clustering of countries

**Explanatory layer**
- Pooled OLS baseline
- Fixed Effects (country + year) with clustered standard errors
- Random Effects + Hausman test for specification choice
- Heterogeneity analysis by income group / region

**Predictive layer**
- Random Forest and Gradient Boosting (XGBoost)
- Time-aware cross-validation to avoid leakage
- SHAP for global and local feature attribution
- Comparison against linear baselines

Formal causal identification (IV, DiD, synthetic control) is out of scope but
discussed as future work."""


def tech_stack() -> str:
    return """## Tech Stack

- **Language:** Python 3.11
- **Data handling:** pandas, numpy, pycountry
- **Econometrics:** statsmodels, linearmodels
- **Machine learning:** scikit-learn, xgboost, shap
- **Visualisation:** matplotlib, seaborn, plotly
- **Notebook tooling:** jupyter, nbformat, nbconvert (notebooks built programmatically)
- **Environment:** conda + pinned `requirements.txt`
- **Version control:** git with per-phase branches"""


def project_structure() -> str:
    return """## Project Structure

```
education-inequality-analysis/
├── README.md                  # This file (generated by scripts/update_readme.py)
├── PROJECT_LOG.md             # Append-only decision log
├── requirements.txt           # Pinned dependencies
├── .python-version            # Python version marker
├── .gitignore
├── data/
│   ├── raw/                   # Original data + manifest.yaml (gitignored)
│   └── processed/             # panel.csv (analytical artefact) + intermediates
├── notebooks/                 # Phase-aligned portfolio notebooks (01..07)
├── src/                       # Reusable functions and classes
│   ├── paths.py               #   project-root locator
│   ├── manifest.py            #   data source registry accessors
│   ├── country_metadata.py    #   WB country metadata loader
│   └── io_utils.py            #   encoding-fallback CSV reader
├── scripts/                   # Step scripts (phaseXX_sYY_*.py) + maintenance utilities
├── outputs/
│   ├── figures/               #   phase-prefixed figures
│   ├── tables/                #   phase-prefixed CSV reports
│   └── models/                #   trained models (Phase 06+)
└── docs/
    ├── project_scope.md       # Canonical project scope
    └── phase_summaries/       # Per-phase handoff files (gitignored)
```"""


def installation() -> str:
    return f"""## Installation and Usage

### 1. Clone the repository

```bash
git clone {REPO_URL}.git
cd {REPO_NAME}
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
running the Phase 01–02 step scripts in order:

```bash
python scripts/phase01_s01_design_manifest.py
python scripts/phase01_s02_download_world_bank.py
python scripts/phase01_s04_download_undp_hdr.py
python scripts/phase01_s05_inspect_coverage.py
python scripts/phase02_s02_build_intermediate_long.py
python scripts/phase02_s03_concat_master_long.py
python scripts/phase02_s04_pivot_to_wide_panel.py
python scripts/phase02_s05_missingness_report.py
```

### 5. View the notebooks

Open `notebooks/` in Jupyter or VS Code, select the `p4_education`
kernel, and execute notebooks in numerical order (01 → 07)."""


def phase_progress() -> str:
    rows = ["| Phase | Title | Status |", "|---|---|---|"]
    for num, (title, status) in sorted(PHASE_STATUS.items()):
        icon = STATUS_ICON[status]
        label = status.replace("_", " ").title()
        rows.append(f"| {num:02d} | {title} | {icon} {label} |")
    return "## Project Status\n\n" + "\n".join(rows)


def findings() -> str:
    return """## Findings

### Available now (data infrastructure, Phases 01–02)

- **Phase 01** — [`01_data_collection.ipynb`](notebooks/01_data_collection.ipynb)
  documents the raw layer: a machine-readable manifest of 19 declared variables
  across three sources (WB WDI, WB country metadata, UNDP HDR), per-variable
  coverage characterisation, and source reconciliation. Gini emerges as the
  binding constraint at 30% country-year completeness.
- **Phase 02** — [`02_data_cleaning.ipynb`](notebooks/02_data_cleaning.ipynb)
  produces the integrated 7,378-row × 24-column panel covering 217 countries
  × 34 years (1990–2023). Missingness is profiled per-variable, jointly across
  candidate specifications, and visually as a country × year heatmap.
  Removing Gini from the baseline specification doubles the listwise sample
  (1,423 → 3,041 rows) — the quantitative anchor for the project's MNAR caveat.

### Coming soon (analytical findings, Phases 05–07)

- *Headline results from econometric models (Phase 05)*
- *Top drivers of inequality identified by SHAP (Phase 06)*
- *Cross-method comparison and policy-relevant takeaways (Phase 07)*"""


def limitations_placeholder() -> str:
    return """## Limitations and Future Work

*A detailed discussion will appear after Phase 07. Known constraints by design:*

- Observational data — causal claims are made only with explicit caveats
- Cross-country Gini measurement is heterogeneous (consumption- vs income-based surveys)
- Systematic missingness under-represents low-income countries (plausibly MNAR)
- IV, DiD, and dynamic panel estimators are out of scope; framed as next steps"""


def documentation_section() -> str:
    return """## Documentation

- [Project Scope](docs/project_scope.md) — Canonical specification
- [Project Log](PROJECT_LOG.md) — Append-only record of decisions and progress
- Phase summaries live in `docs/phase_summaries/` (gitignored, internal use)"""


def author_section() -> str:
    return (
        "## Author\n\n"
        f"**{AUTHOR_DISPLAY_NAME}** — "
        f"[GitHub @{GITHUB_HANDLE}]({GITHUB_PROFILE_URL})\n\n"
        "Part of a data science portfolio for roles in applied analytics "
        "and applied research."
    )


# ============================================================
# Assembly
# ============================================================

SECTIONS = [
    header,
    overview,
    research_questions,
    data_section,
    methods_section,
    tech_stack,
    project_structure,
    installation,
    phase_progress,
    findings,
    limitations_placeholder,
    documentation_section,
    author_section,
]


def build_readme() -> str:
    """Assemble all sections into a single README string."""
    return "\n\n".join(section() for section in SECTIONS) + "\n"


def main() -> None:
    repo_root = Path(__file__).resolve().parent.parent
    readme_path = repo_root / "README.md"

    content = build_readme()
    readme_path.write_text(content, encoding="utf-8")

    print(f"README.md regenerated at: {readme_path}")
    print(f"Sections written:         {len(SECTIONS)}")
    print(f"Total length:             {len(content):,} characters")


if __name__ == "__main__":
    main()
