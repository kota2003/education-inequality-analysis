# Project Log

Append-only decision and progress log for the Education–Inequality analysis project.
New entries go at the bottom. Do not edit prior entries; if correction is needed, append a new entry explaining the correction.

---

## 2026-04-23 — Phase 00, Project Kickoff

**Context:** Project 4 of the data science portfolio. Initial scope (v1) proposed a cross-country regression of Gini on education enrolment, with a five-phase plan. The scope was reviewed against the bar expected of a Data Scientist portfolio and found to be too narrow (single method, limited variables, no machine learning component, no explicit handling of panel structure).

**Decision:** Adopted Scope v2, which restructures the project around a three-layer analytical framework:

- Layer A — Descriptive (EDA, geographic visualisation, country clustering)
- Layer B — Explanatory (Pooled OLS → Fixed Effects → Random Effects + Hausman test, clustered standard errors)
- Layer C — Predictive (Random Forest / Gradient Boosting, SHAP interpretability)

Phase count expanded from 5 to 7 to accommodate the added clustering (Phase 04) and predictive modelling (Phase 06) layers. Data coverage expanded to all available countries, 1990–2023. Variable set expanded to include mean years of schooling, gender-disaggregated enrolment, education expenditure, and additional structural controls (urbanisation, trade openness, industry shares).

**Rationale:**

- A DS portfolio must demonstrate both statistical inference and machine learning; a single-method project cannot do this.
- Panel data demands panel methods; Pooled OLS alone would be a technical weakness visible to any reviewer with econometrics background.
- Wider data coverage enables fixed-effects estimation (which needs within-country variation over time) and supports ML models that require sufficient sample size.
- Explicit out-of-scope statements (IV, DiD, deep learning) prevent scope creep while demonstrating awareness of methodological alternatives.

**Impact:**

- Project duration extended; expect ~7 chat sessions (one per phase).
- Tech stack expanded: added `linearmodels`, `xgboost`, `shap`, `pycountry`, `wbdata`.
- Success criteria rewritten to be measurable across technical, analytical, and portfolio dimensions.
- Scope v2 placed at `docs/project_scope.md` as the canonical living specification.

---

## 2026-04-23 — Phase 00, Environment Setup

**Context:** Repository cloned from GitHub as an empty project. Required a reproducible Python environment with pinned dependencies, isolated from prior projects' environments.

**Decision:**

- Created a dedicated conda environment `p4_education` with Python 3.11.15.
- Installed the full Scope v2 dependency set (pandas, numpy, statsmodels, linearmodels, scikit-learn, xgboost, shap, matplotlib, seaborn, plotly, wbdata, pycountry, jupyter, ipykernel).
- Pinned all installed versions via `pip freeze > requirements.txt` to guarantee reproducibility.
- Registered `p4_education` as a Jupyter kernel for use in notebooks.
- Established the standard project directory layout per `PROJECT_WORKFLOW.md` §6.
- Configured `.gitignore` to exclude raw data, virtual environment folders, phase summaries, and OS/IDE artefacts, while preserving directory structure via `.gitkeep` placeholders.

**Rationale:**

- A dedicated environment per project is a baseline reproducibility requirement for a portfolio repository that reviewers may clone.
- Conda was chosen over `venv` for consistency with prior projects in this workspace and because Anaconda is already the user's primary Python distribution.
- Version pinning via `pip freeze` captures not just declared dependencies but transitive ones, eliminating "works on my machine" failures.

**Impact:**

- All subsequent phases run inside `p4_education`. Do not install additional packages globally.
- Any new library introduced in later phases must be followed by `pip freeze > requirements.txt` and a commit noting the addition.
- `data/raw/` contents are gitignored; raw data acquisition scripts (Phase 01) must be fully re-runnable from the scripts themselves so that a fresh clone can reproduce the raw layer.
