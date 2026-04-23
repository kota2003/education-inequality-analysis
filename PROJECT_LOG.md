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

## 2026-04-23 — Phase 01, Step 01

**Context:** Drafted `data/raw/manifest.yaml` declaring 3 data sources (World Bank WDI, World Bank country metadata, UNDP HDR) and 18 World Bank indicators. Built `scripts/phase01_s01_design_manifest.py` to structurally validate the manifest and probe each WB indicator code against the World Bank REST API (`/v2/indicator/{code}?format=json`).

**Decision:** Accept Step 01 as complete with 17/18 indicators API-verified and 1 (`SE.SEC.ENRR.MA`) externally verified via the official WB data portal (https://data.worldbank.org/indicator/SE.SEC.ENRR.MA — confirmed live, 1970–2025 coverage, sourced from UNESCO UIS, last published 2026-02). Retry logic (3 attempts, 20s timeout, exponential backoff) resolved 2 of the initial 3 transient timeouts; the remaining one is a route-specific network quirk rather than a code validity issue.

**Rationale:** The purpose of Step 01 is to catch code typos and renames before Step 02 bulk downloads. All 18 codes are confirmed current. Extending retry budgets further would only chase network variance without new information. Step 02's actual download attempt serves as the authoritative test.

**Impact:** Manifest is finalised for Step 02 consumption. Validation report saved at `outputs/tables/phase01_s01_manifest_validation.csv`. No changes to `requirements.txt`. If `SE.SEC.ENRR.MA` fails to download in Step 02, we will revisit with concrete failure information.

## 2026-04-23 — Phase 01, Step 02

**Context:** Bulk-downloaded all 18 World Bank WDI indicators (1990–2023, all entities) plus country metadata from the WB REST API. Output in long format to `data/raw/world_bank/wb_wdi.csv` (162,792 rows) and `wb_country_metadata.csv` (296 entities: 217 countries + 79 aggregates). Per-indicator summary saved to `outputs/tables/phase01_s02_download_report.csv`.

**Decision:** Accept Step 02 as complete. All 18 indicators resolved and returned data. `SE.SEC.ENRR.MA` — which failed all probe attempts in Step 01 — downloaded cleanly with 5,505 non-null observations (parity with the female counterpart's 5,509), confirming the Step 01 timeout was isolated network variance on the metadata endpoint and not a code-level issue. Per the Q3 decision (UNESCO via WB mirror), the original Step 03 (separate UNESCO download) is absorbed into Step 02; no Step 03 script will be produced.

**Rationale:** Non-null coverage is consistent with Scope v2 §6.2 priors. Gini (2,212 non-null) is the binding constraint for the joint analytical sample and lands at the lower end of the predicted 2,000–3,500 country-year range. Education variables (5,000–6,500 non-null) and core economic controls (7,000–9,000) are substantially denser. Raw layer preserves both ISO-2 and ISO-3 plus source country names; aggregates are kept and will be filtered in Phase 02 per the Q4 canonicalisation decision.

**Impact:** Downstream steps can assume wb_wdi.csv and wb_country_metadata.csv exist in their declared locations. Phase 01 is now a 5-script phase (Step 03 skipped). Next script to produce: `scripts/phase01_s04_download_undp_hdr.py`.

## 2026-04-23 — Phase 01, Step 04

**Context:** Downloaded the UNDP HDR composite indices time series CSV to `data/raw/undp_hdr/hdr_composite_indices.csv`. The first run revealed two issues: (a) the UTF-8 read in the inspection step failed with `UnicodeDecodeError: 'utf-8' codec can't decode byte 0xf4 in position 132167` — HDR CSVs are published in Windows-1252, not UTF-8; (b) the manifest URL (`2023-24_HDR/HDR23-24_...`) pointed at an older HDR vintage whose time series stopped at 2022, leaving mys_2023 missing relative to Scope coverage.

**Decision:** Fixed both. Added an encoding-fallback reader (utf-8 → utf-8-sig → cp1252 → latin-1) to the inspection step; the raw file itself is saved byte-for-byte unchanged. Updated the manifest URL to the HDR 2025 vintage (`2025_HDR/HDR25_Composite_indices_complete_time_series.csv`, released May 2025) and added a `metadata_url` field pointing at the accompanying metadata XLSX.

**Rationale:** Encoding normalisation belongs in Phase 02 (per the Q4 raw-layer policy), so the download script only decodes for inspection and never rewrites the file. The HDR 2025 vintage covers 1990–2023 exactly, matching Scope §6.2 with no truncation, and is what the current UNDP Documentation-and-Downloads page directs users to.

**Impact:** HDR layer now carries 206 entities × 34 years = 7,004 potential mys cells, of which 6,455 are non-null (~92%). All 34 mys_YYYY columns are present. Detected time-series families (hdi, le, eys, mys, gnipc, gdi, gii, ihdi, phdi) are available for downstream use if later phases need them, though Scope v2 commits only to `mys`.

## 2026-04-23 — Phase 01, Step 05

**Context:** Produced per-variable coverage summary and country × year availability matrix across WB and HDR raw data. First run exposed a filter bug: `pd.read_csv` silently converted the literal string "NA" (WB's code for aggregate-level rows in `region_id`) to NaN, so the `!= "NA"` comparison never matched and all 296 entities passed through. A parallel issue existed on the HDR side with ZZ*-prefixed aggregate iso3 codes (ZZA.VHHD, ZZE.AS, ZZK.WORLD, etc.).

**Decision:** Replaced the WB filter with `.notna()` and added a ZZ-prefix filter for HDR. Re-ran cleanly: 217 WB countries and 195 HDR real entities (intersection = 195, WB-only = 22 non-sovereign territories, HDR-only = 0).

**Rationale:** The reconciliation output is now diagnostic: all 22 WB-only entries are non-sovereign territories or SARs (Aruba, Bermuda, Channel Islands, Puerto Rico, Macao, etc.) which HDR does not cover by design. Phase 02 has a binary decision — keep these 22 with HDR-as-NaN or drop them — rather than a general country-name reconciliation problem.

**Impact:** Coverage matrix (`outputs/figures/phase01_s05_coverage_matrix.png`) reveals: (a) Gini is the binding constraint at 30% country-year completeness versus 50–100% for every other variable; (b) enrolment variables show a clear c.2000 reporting-regime shift; (c) mys and GDP are near-complete from 1990. These three facts are direct inputs to Phase 02 missingness strategy. Full coverage summary saved to `outputs/tables/phase01_s05_coverage_summary.csv`.

## 2026-04-23 — Phase 01, Step 06

**Context:** Consolidated Phase 01 into `notebooks/01_data_collection.ipynb` (21 cells, 8 code cells + 13 markdown) as the portfolio-facing artefact. Promoted one reusable helper — `read_csv_with_encoding_fallback` — from the step scripts to `src/io_utils.py` per PROJECT_WORKFLOW.md §6.2.

**Decision:** Notebook runs top-to-bottom from a fresh kernel. Raw data is read from disk, not re-downloaded, so the notebook is network-free and idempotent. Figures (coverage heatmaps) are regenerated inline rather than embedding the PNG from `outputs/figures/`, so the notebook stands alone as a narrative document.

**Rationale:** `src/` promotion limited to one function because the download / retry / manifest-loading logic is tightly scoped to Phase 01 scripts and is more legible inline than re-imported. Further promotions are deferred to Phase 02 when concrete re-use demand appears (likely a country-canonicalisation helper).

**Impact:** Phase 01 closes. All deliverables listed in `docs/phase_summaries/phase01_summary.md`. Raw data layer is reproducible from the step scripts; the notebook and `src/io_utils.py` are the only code artefacts that persist into Phase 02.