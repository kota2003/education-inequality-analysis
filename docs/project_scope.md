# Project Scope v2

**Project:** Impact of Education on Income Inequality — A Cross-Country Panel Analysis
**Version:** 2.0 (Data Scientist portfolio edition)
**Last updated:** 2026-04-23

---

## 0. Revision Note

This document supersedes Project Scope v1. The revision expands the project from a single-method regression study into a three-layer data science portfolio project (descriptive → explanatory → predictive) with panel data methods and interpretable machine learning. The scope, data coverage, and phase count have all been expanded to match the technical bar expected of a Data Scientist portfolio.

**Flexibility clause:** This scope is a working design, not a contract. Direction may shift as data characteristics become clear during Phase 01–02. Any material change will be reflected in a revised version of this document.

---

## 1. Project Title

**Impact of Education on Income Inequality: A Cross-Country Panel Analysis**

---

## 2. One-Sentence Purpose

Quantify the relationship between education and income inequality across countries and over time, using panel econometrics and interpretable machine learning, to produce policy-relevant insights that honestly acknowledge causal limitations.

---

## 3. Target Audience (Portfolio Positioning)

- **Primary:** Hiring managers and technical interviewers for Data Scientist positions
- **Secondary, content-wise:** Policy makers, think-tank analysts, development economists

The portfolio should demonstrate:

- Handling messy real-world panel data end-to-end
- Command of both classical statistical inference and modern ML
- Awareness of causal inference concepts and their limitations
- Clean engineering: reproducibility, modularity, version control
- Ability to translate numerical results into policy-relevant language

---

## 4. Research Questions

1. Is higher educational attainment associated with lower income inequality across countries?
2. Which level of education (primary, secondary, tertiary) shows the strongest association with inequality?
3. Does the relationship survive controlling for economic development (GDP per capita) and other confounders?
4. Are the relationships heterogeneous across income groups or regions?
5. How do conclusions from linear panel models compare with those from flexible machine learning models?
6. What causal claims can and cannot be made from this analysis?

---

## 5. Three-Layer Analytical Framework

The project is organised around three analytical layers. Each layer answers a different class of question and exercises a different methodological skill set.

### Layer A — Descriptive

**Question:** What does the landscape of education and inequality look like?
**Methods:** EDA, geographic visualisation, unsupervised clustering of countries into education-inequality regimes.
**Skill shown:** Data wrangling, visualisation, unsupervised learning.

### Layer B — Explanatory (causal-leaning)

**Question:** After controlling for confounders, what is the association between education and inequality?
**Methods:** Pooled OLS → Fixed Effects → (Random Effects + Hausman test) with clustered standard errors.
**Skill shown:** Panel econometrics, hypothesis testing, causal reasoning.

### Layer C — Predictive

**Question:** Can a flexible model predict Gini from education and controls, and which features matter most?
**Methods:** Random Forest / Gradient Boosting, SHAP-based interpretation, comparison to linear baselines.
**Skill shown:** Supervised ML, model interpretability, model comparison.

Each layer has its own phase. The final phase synthesises across all three.

---

## 6. Data

### 6.1 Data Sources

| Source | Data |
|---|---|
| World Bank (World Development Indicators) | Gini index, GDP per capita, unemployment, population, urbanisation, trade openness, government expenditure, inflation, sector shares |
| UNESCO Institute for Statistics | Primary / secondary / tertiary enrolment rates, gender-disaggregated enrolment, education expenditure as % GDP |
| UNDP Human Development Report | Mean years of schooling |
| World Bank (country metadata) | Region, income group classification |

### 6.2 Coverage

- **Countries:** All available (target ~190; final analytical sample determined by data availability)
- **Years:** 1990–2023
- **Unit of observation:** Country × Year (unbalanced panel)
- **Expected raw size:** ~6,000 country-year rows pre-cleaning; 2,000–3,500 rows post-cleaning (estimate)

### 6.3 Variables

**Target**
- `gini` — Gini index (World Bank)

**Main explanatory variables (education)**
- `enrol_primary`, `enrol_secondary`, `enrol_tertiary` — gross enrolment ratios
- `mean_years_schooling` — UNDP
- `edu_expenditure_gdp` — public education expenditure as % of GDP
- `enrol_secondary_female`, `enrol_secondary_male` — gender disaggregation (for heterogeneity analysis)

**Control variables (economic and structural)**
- `gdp_per_capita` (log-transformed)
- `unemployment_rate`
- `population` (log-transformed)
- `urban_population_pct`
- `trade_openness` (exports + imports / GDP)
- `gov_expenditure_gdp`
- `inflation_cpi`
- `agri_value_added_gdp`, `manu_value_added_gdp`, `services_value_added_gdp`

**Categorical / structural**
- `region` (World Bank 7-region classification)
- `income_group` (Low / Lower-middle / Upper-middle / High)
- `country_code` (ISO-3)
- `year`

Variable list is a superset; final model specifications will use a subset selected based on multicollinearity and theoretical relevance.

### 6.4 Data Integration Challenges (known in advance)

- Country name reconciliation across sources (handled via `pycountry` + manual mapping table)
- Unbalanced panel: countries have different observation years
- Systematic missingness: low-income countries under-represented in Gini data → selection bias to be made explicit, not hidden
- Measurement heterogeneity in Gini calculation across countries (to be discussed in limitations)

---

## 7. Methodology

### 7.1 Descriptive (Layer A)

- Distributions, time trends, regional patterns of Gini and education variables
- Correlation matrix with multicollinearity diagnostics (VIF)
- Geographic visualisation (choropleth)
- K-means or hierarchical clustering on standardised country-level features to identify education-inequality regimes

### 7.2 Explanatory (Layer B)

Sequential modelling, each step motivated by the limitations of the previous one:

1. **Pooled OLS** — baseline, ignores panel structure
2. **Fixed Effects (country + year)** — absorbs time-invariant country characteristics and common shocks
3. **Random Effects + Hausman test** — to justify the FE specification
4. **Clustered standard errors** — at the country level, to account for within-country serial correlation
5. **Heterogeneity analysis** — interaction terms or subsample regressions by income group

Inference tool: `linearmodels` (panel) + `statsmodels` (OLS baselines, diagnostics).

### 7.3 Predictive (Layer C)

- Baseline: linear regression (same specification as Layer B, but framed as a predictor)
- Tree ensembles: Random Forest and Gradient Boosting (XGBoost or LightGBM)
- Time-aware cross-validation (train on earlier years, validate on later) to avoid leakage
- Metrics: RMSE, MAE, R² on held-out data
- Interpretation: SHAP values for global and per-country feature attribution
- Explicit comparison to Layer B: do ML importance rankings agree with econometric coefficient magnitudes?

### 7.4 Causal Discussion (Phase 07)

The project does **not** claim causal identification. Phase 07 makes this explicit by discussing:

- Omitted variable bias candidates that FE cannot absorb
- Reverse causality (inequality → education investment)
- Measurement error in Gini and enrolment
- Selection bias from missing country-years
- What credible identification would require (IV, DiD, synthetic control) — framed as future work

This honest boundary-setting is itself a portfolio feature.

---

## 8. Phase Plan (7 phases)

| # | Phase | Primary Deliverable |
|---|---|---|
| 01 | Data Collection | Raw data files, ingestion scripts, data-source registry |
| 02 | Data Cleaning & Integration | Integrated panel dataset (`processed/panel.csv`), missingness report |
| 03 | Exploratory Data Analysis | Distributions, correlations, time trends, geographic maps |
| 04 | Descriptive Analytics (clustering) | Country typology, cluster profiles |
| 05 | Econometric Modelling | Pooled OLS / FE / RE results, Hausman test, clustered SE |
| 06 | Predictive Modelling & Interpretability | Trained ML models, SHAP outputs, baseline comparisons |
| 07 | Synthesis & Policy Discussion | Integrated narrative, policy implications, causal caveats, final report |

Each phase produces a numbered notebook in `notebooks/` as the portfolio-facing artefact and follows the step-execution protocol defined in `PROJECT_WORKFLOW.md`.

---

## 9. Outputs

### 9.1 Notebooks (portfolio core)

- `01_data_collection.ipynb`
- `02_data_cleaning.ipynb`
- `03_eda.ipynb`
- `04_country_clustering.ipynb`
- `05_econometric_modelling.ipynb`
- `06_predictive_modelling.ipynb`
- `07_synthesis_and_policy.ipynb`

### 9.2 Analytical outputs

- Regression coefficient tables (OLS / FE / RE) with clustered standard errors
- ML performance metrics and feature importance rankings
- SHAP summary and dependence plots
- Cluster profile tables

### 9.3 Visual outputs

- Distribution plots of Gini and education variables
- Scatter plots with regression lines
- Choropleth maps
- Time series of Gini by region / income group
- SHAP visualisations

### 9.4 Documentation

- `README.md` (portfolio-facing, maintained via `scripts/update_readme.py`)
- `PROJECT_LOG.md` (append-only decision log)
- `docs/phase_summaries/` (per-phase handoff files, gitignored)

---

## 10. Tech Stack

**Core**
- Python 3.11+
- pandas, numpy

**Econometrics**
- statsmodels
- linearmodels (panel FE / RE, clustered SE)

**Machine learning**
- scikit-learn
- xgboost or lightgbm
- shap

**Visualisation**
- matplotlib, seaborn
- plotly (interactive) and/or geopandas (choropleth)

**Data handling**
- pycountry (ISO code reconciliation)
- wbdata or direct World Bank API client
- requests (UNESCO bulk downloads)

**Engineering**
- pinned `requirements.txt`
- git with per-phase branches
- Jupyter for notebooks

---

## 11. Success Criteria

### 11.1 Technical

- Panel dataset integrated from 3+ sources with documented reconciliation logic
- Three panel specifications (Pooled OLS, FE, RE) estimated with clustered standard errors; Hausman test executed to justify model choice
- At least one ML model outperforms the linear baseline on held-out RMSE, or the reason it does not is explained
- SHAP-based feature attribution produced and compared to econometric coefficients
- At least one documented heterogeneity finding (by income group or region)

### 11.2 Analytical

- Clear quantitative answer to each research question, with uncertainty
- Explicit statement of what can and cannot be concluded causally, with at least three named threats to identification
- Policy-relevant framing of findings in Phase 07

### 11.3 Portfolio

- README readable by a recruiter in 5 minutes
- All notebooks execute top-to-bottom from a fresh kernel without error
- Git history is clean and tells a coherent story of the project
- Code in `src/` is importable, has docstrings, and contains no dead code

---

## 12. Risks and Limitations

| Risk | Mitigation |
|---|---|
| Country name mismatches across sources | `pycountry` + manual mapping, unit-tested |
| Systematic missingness biases sample | Explicit missingness report in Phase 02; bias discussed in Phase 07 |
| Gini measurement differences across countries | Acknowledge in limitations; use FE to absorb time-invariant measurement style |
| Multicollinearity among education variables | VIF diagnostics; alternative specifications |
| Reverse causality (inequality → education) | Not solvable without IV; flagged as limitation |
| Unbalanced panel | Use methods that handle unbalanced panels natively |
| Overfitting in ML layer | Time-aware CV, regularisation, held-out test set |

---

## 13. Out of Scope

To keep the project tractable:

- Formal causal identification (IV, DiD, RDD, synthetic control) — discussed but not estimated
- Within-country subnational analysis
- Dynamic panel models (Arellano-Bond and similar)
- Deep learning models (not justified by sample size)
- Real-time / production deployment (this is an analytical portfolio, not a product)

These are positioned as natural next steps in the final phase.

---

## 14. Immediate Next Steps

1. Confirm this v2 scope with the user
2. Set up repository structure per `PROJECT_WORKFLOW.md` §6
3. Begin Phase 01 — Data Collection, starting with World Bank Gini and GDP indicators
4. Generate Phase 01 step plan and await user approval

---

*End of Project Scope v2*
