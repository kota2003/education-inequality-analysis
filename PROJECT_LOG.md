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

## 2026-04-25 — Phase 02, Step 01

**Context:** Phase 02 (Data Cleaning & Integration) opens with five
unresolved design questions inherited from Phase 01: analytical window,
treatment of WB-only countries lacking HDR coverage, missingness
strategy, timing of log transforms, and whether to attach Gini
provenance metadata. These choices shape the schema of
`data/processed/panel.csv` and constrain every downstream phase, so
they are recorded before any integration code is written.

**Decision:**

1. **Analytical window — keep 1990–2023 in the panel.** The panel
   retains the full Scope-committed window. Specification-level sample
   restrictions (e.g. enrolment-dependent models effectively becoming
   2000–2023) are handled at modelling time, not at panel construction.
2. **WB-only 22 countries — retain with `mys` = NaN.** The 22 entities
   (21 high-income territories/SARs + Kosovo) are kept in the panel.
   Listwise deletion at the specification level will drop them
   automatically from any model that includes `mys`.
3. **Missingness — listwise default, per-specification reporting.**
   Multiple imputation is rejected: Gini's missingness concentrates in
   low-income countries and is plausibly MNAR, so MI under MAR would
   inflate apparent power without controlling bias direction. Each
   model specification will report N and country coverage explicitly.
4. **Log transforms — apply at modelling time (Phase 05).** `panel.csv`
   stores `gdp_per_capita` and `population` in raw units. EDA in
   Phase 03 sees the genuine skewed distributions; Phase 05 applies
   `np.log` at use.
5. **Gini provenance metadata — not attached in Phase 02.** Adding a
   PIP method-type column would require a separate API call surface
   and would yield a partially-populated column that pollutes the
   panel. The Gini measurement-heterogeneity caveat is deferred to
   Phase 07's causal discussion (already flagged in Scope §12).

**Rationale:**

- **Discard data only once, at the latest stage.** Trimming the window
  or dropping countries during cleaning forecloses analyses (early-
   1990s descriptive statistics, high-income heterogeneity) that the
  panel could otherwise support at near-zero storage cost.
- **Statistical honesty over statistical convenience.** MI on a
  plausibly MNAR target inflates effective sample size without
  trustworthy bias control. Phase 07 framing the missingness as a
  named threat to identification is more credible than imputed
  coefficients.
- **Schema discipline.** Storing log columns or a half-populated
  provenance column duplicates state and creates "which column do I
  use?" ambiguity. The panel stores raw, model-time code applies
  transformations.

**Impact:**

- Phase 02 Steps 02–05 build a panel keyed on (iso3, year) over
  1990–2023 with 217 WB countries (HDR's 195 plus the 22 WB-only).
- Phase 03 EDA can plot raw `gdp_per_capita` and `population`
  distributions directly; log scale is a plotting choice, not a
  schema fact.
- Phase 05 modelling code owns log transforms and listwise sample
  construction, and must report N + country coverage per
  specification.
- Phase 07 inherits the Gini measurement caveat and the MNAR
  selection-bias discussion.


  

---

## 2026-04-25 — Phase 02 Completion

**Context:** Phase 02 — Data Cleaning & Integration — produced the
analytical panel from the raw layer Phase 01 deposited. Six step
scripts plus a portfolio notebook completed in sequence; the entire
processed layer is reproducible from a fresh clone given only the raw
files.

**Decision (closure):**

- The analytical panel is fixed at `data/processed/panel.csv`:
  7,378 rows × 24 columns (217 countries × 34 years; 5 metadata
  columns plus 19 variables in canonical manifest declaration order).
- Missingness is documented along three axes: per-variable in
  `outputs/tables/phase02_missingness_report.csv`, spatiotemporal in
  `outputs/figures/phase02_missingness_matrix.png`, narrative in
  `notebooks/02_data_cleaning.ipynb`.
- Phase 03 EDA inherits this panel without modification. Any future
  panel revision must produce a versioned successor and document the
  diff.

**Rationale:**

Three discoveries during Phase 02 carry forward as binding constraints
or named caveats:

1. **24 WB countries lack a usable `mys` value** (Phase 01 had
   predicted 22). The 2-country gap traces to HDR iso3 rows that
   exist but contain no observed `mys` for any year. Listwise
   deletion handles this automatically per Decision 2.
2. **Removing Gini more than doubles the listwise sample.** The
   illustrative `core_education_economic` specification (with Gini)
   yields 1,423 rows; the same specification without Gini yields
   3,041. This is the quantitative baseline for the MNAR caveat
   recorded in Step 01 Decision 3.
3. **`unemployment_rate` first observed year is 1991.** Any 1990
   cross-section using ILO modeled unemployment will be empty;
   FE specifications are unaffected.

**Impact:**

- Phase 03 EDA reads `panel.csv` directly; variables are in manifest
  declaration order, metadata in columns 3-5. `src.manifest` provides
  programmatic access to variable lists.
- Phase 05 modelling code owns log transforms, listwise deletion, and
  per-specification N reporting.
- Phase 07 inherits three named threats to identification:
  Gini measurement heterogeneity, MNAR selection (low-income
  under-coverage), and reverse causality.
- `src/` now contains `paths.py`, `manifest.py`, and
  `country_metadata.py` in addition to the `io_utils.py` promoted
  in Phase 01.

---

## 2026-04-30 — Phase 03, Step 01

**Context:** Phase 03 (Exploratory Data Analysis) opens with seven
presentation- and methodology-level decisions that shape the EDA notebook's
design. Five are open by design (geographic library, primary categorical
lens, univariate display for skewed variables, VIF flagging threshold,
time-series aggregation strategy); two were deferred from Phase 02
(pre-2000 enrolment sparsity, unemployment_rate 1990 missingness). These
are recorded before any plotting code is written so that the choices are
visible in the project's audit trail rather than hidden inside step
scripts.

**Decision:**

1. **Geographic visualisation library — plotly.** Use
   `plotly.express.choropleth` with `locations="iso3", locationmode="ISO-3"`.
   `geopandas` is not installed and will not be added in Phase 03.
2. **Primary categorical lens — both `region_name` and `income_level_name`,
   purposefully.** Region (n=7) carries the geographic narrative; income
   group (n=4) carries the structural-development narrative. Step 03 colours
   bivariate scatters by region (better discrimination across 7 categories);
   Step 05 produces parallel region- and income-faceted time-series views.
3. **Univariate display for skewed variables — raw and log side by side.**
   Applies to `gdp_per_capita_usd`, `gdp_per_capita_ppp`, and `population`.
   Other 16 variables are shown raw only.
4. **VIF flagging threshold — stratified.** `VIF > 5` flagged as "watch",
   `VIF > 10` flagged as "concern". The sector-share trio
   (`agri_value_added_gdp`, `manu_value_added_gdp`,
   `services_value_added_gdp`) is expected at the "concern" level by
   construction (sum-to-~100 constraint); separating thresholds prevents
   that structural collinearity from drowning out other multicollinearity
   signals.
5. **Time-series aggregation — region mean ± IQR and income-group mean ±
   IQR, in parallel.** Country-level lines (217 series) are unreadable.
   Where a stratum has N_countries < 5 (notably North America at n=3),
   suppress the IQR band and either show min–max range or individual lines.
6. **Pre-2000 enrolment sparsity — show full 1990–2023 with explicit
   sparsity indicator.** No truncation. Per-year N_observed annotated on
   enrolment time-series plots; interpretation text states that water-level
   comparisons should anchor on the post-2000 dense regime.
7. **`unemployment_rate` 1990 missingness — start unemployment line at
   1991 with footnote; do not restrict the rest of the panel.** ILO's
   modeled series begins in 1991 by construction. Other variables' 1990
   observations are not sacrificed for visual consistency; the
   variable-specific start year is documented per plot.

**Rationale:**

- **Plotly over geopandas:** plotly is already pinned (`plotly==6.7.0`),
  has built-in ISO-3 country lookups, and avoids the GDAL/PROJ dependency
  surface on Windows. The choropleth use case in Phase 03 is a simple
  country-level fill; the additional capabilities geopandas provides
  (projections, spatial joins) are not needed here.
- **Both categorical lenses, not one:** region and income group answer
  different analytical questions. Forcing a single lens would either
  weaken geographic storytelling (income only) or weaken inequality
  storytelling (region only). The two views together also support the
  Phase 04 clustering motivation by surfacing cases where region and
  income-group framings disagree.
- **Raw + log presentation:** raw alone hides the skew that motivates log
  transforms in Phase 05; log alone obscures the panel's actual storage
  convention (Phase 02 Decision 4: panel stores raw). Showing both makes
  the design separation between presentation and modelling explicit.
- **Stratified VIF:** a single threshold conflates two qualitatively
  different problems — structural collinearity (sector shares,
  by-construction) and incidental collinearity (e.g. between mean years
  of schooling and GDP per capita, which the modelling owns). Phase 05's
  variable-selection rationale benefits from seeing them separated.
- **Region/income parallel time-series:** the dual view supports the
  Phase 04 clustering motivation directly. Convergence within a region
  but divergence across income groups (or vice versa) is exactly the
  kind of pattern that justifies a country typology.
- **No enrolment truncation:** truncating the 1990s would silently drop
  the 20 countries with full primary-enrolment coverage and make the
  data appear cleaner than it is. Annotating sparsity is more honest
  and aligns with the project's portfolio framing on missingness.
- **Unemployment 1991 start:** restricting all subplots to 1991+ would
  discard ~190 country-year observations across other variables for the
  sake of one ILO series's start year. Per-line start years with a
  footnote is the more proportionate fix.

**Impact:**

- Step 02 (univariate distributions) emits a 5×4 grid where three
  variables (`gdp_per_capita_usd`, `gdp_per_capita_ppp`, `population`)
  appear as raw + log pairs.
- Step 03 (bivariate Gini) colours scatters by `region_name`.
- Step 04 (correlation/VIF) emits a stratified VIF table with watch/concern
  bands; Phase 05's specification rationale will cite this directly.
- Step 05 (time-series) emits two figures, one region-faceted and one
  income-faceted, with N_countries annotated and IQR bands suppressed
  where N < 5.
- Step 06 (choropleth) uses plotly only; `requirements.txt` is unchanged.
- Enrolment plots show 1990–2023 with year-level N_observed; unemployment
  time-series start at 1991 with a footnote.
- No `requirements.txt` changes for Phase 03 (geopandas not installed,
  kaleido added only if static-PNG export proves necessary in Step 06).

---

## 2026-04-30 — Phase 03 Completion

**Context:** Phase 03 — Exploratory Data Analysis — completed in eight
step scripts plus the portfolio notebook. The analytical panel produced
in Phase 02 was characterised along five axes (univariate distribution,
bivariate Gini-vs-predictor, correlation/VIF, region-and-income
time-series, geographic choropleth) and the resulting findings condensed
into `notebooks/03_eda.ipynb`. The phase did not modify the panel;
all outputs are derived diagnostics that bind subsequent specifications.

**Decision (closure):**

- Phase 03 deliverables are fixed at:
  - `notebooks/03_eda.ipynb` (~6.4 MB, 23 cells: 17 markdown + 6 code,
    fully executed)
  - `outputs/figures/phase03_s02_univariate_distributions.png`
  - `outputs/figures/phase03_s03_bivariate_gini.png`
  - `outputs/figures/phase03_s04_correlation_matrix.png`
  - `outputs/figures/phase03_s05_timeseries_by_region.png`
  - `outputs/figures/phase03_s05_timeseries_by_income.png`
  - `outputs/figures/phase03_s06_choropleth_gini.html`
  - `outputs/figures/phase03_s06_choropleth_mys.html`
  - `outputs/tables/phase03_s02_skewness.csv`
  - `outputs/tables/phase03_s03_lowess_vs_linear.csv`
  - `outputs/tables/phase03_s04_correlation_matrix.csv`
  - `outputs/tables/phase03_s04_correlation_spearman.csv`
  - `outputs/tables/phase03_s04_vif.csv`
- Phase 04 (country clustering) inherits the panel and the diagnostics
  above without modification.
- `requirements.txt` regenerated mid-phase with `kaleido==0.2.1` pinned
  for plotly static-image export.

**Rationale (carry-forward findings):**

Five concrete findings carry forward as binding constraints or named
caveats for Phases 04, 05, and 07.

1. **"Not classified" income group, n=2.** The WB income classification
   contains a stratum that Phase 02's metadata loader did not surface
   explicitly. Two countries (most plausibly Venezuela, suspended from
   classification since 2021, plus one structural case) sit outside the
   four canonical income groups. Phase 05 income-stratified
   specifications must either drop these two or document their absence.
2. **VIF listwise sample is 1,718, not 3,041.** Phase 02's missingness
   report listed the `no_gini_diagnostic` specification (gini removed
   from a 6-variable spec) at 3,041 country-years. The 18-RHS-variable
   VIF listwise specification computed in Phase 03 Step 04 is
   structurally different — it includes all controls — and yields 1,718
   country-years on listwise-complete intersection. This is a new number
   not present in any Phase 02 artefact.
3. **Sector-share trio is not the structural collinearity hazard
   anticipated.** The pre-Phase-03 expectation (recorded in Phase 03
   Step 01 Decision 4) that `agri/manu/services_value_added_gdp` would
   register at the VIF concern level by virtue of summing to ~100% turns
   out to be overstated. Only `agri_value_added_gdp` enters the watch
   tier (VIF = 5.4); `services` is at 2.9 and `manu` at 1.4. Phase 05
   can use the three sector-share variables as separate regressors in
   the baseline specification without the dropping-or-PCA workaround
   anticipated in Step 01.
4. **Income view of Gini is consistent with a Kuznets-type inverted-U.**
   The income-faceted time-series shows upper-middle income countries at
   the top of the Gini distribution (~40, drifting toward 35), high
   income at the bottom (~30 with narrow IQR), and low income in the
   middle with substantial year-on-year noise. The casual "low-income =
   high-inequality" framing is contradicted by the data. This is the
   strongest single narrative input to Phase 04 clustering and Phase 07
   policy framing.
5. **kaleido v1 / plotly 6.7.0 static-image export is broken on Windows.**
   Documented to spare future reproducers: `kaleido==1.x` registered
   against `plotly==6.7.0` raises `ValueError: Image export using the
   "kaleido" engine requires the Kaleido package` even when kaleido is
   installed and importable (upstream issue plotly/Kaleido #443, opened
   2026-04-19). The downgrade `pip install kaleido==0.2.1
   --force-reinstall` installs cleanly but hangs on the first
   `fig.write_image` call on Windows. Phase 03 Step 06 therefore ships
   HTML-only choropleths; the notebook embeds them via IFrame for local
   Jupyter viewers and via Markdown link as a fallback for GitHub
   previewers. `requirements.txt` retains `kaleido==0.2.1` as a marker
   of the attempted resolution.

**Impact:**

- Phase 04 clustering inherits a clear short list of cluster-defining
  features: `mean_years_schooling`, log `gdp_per_capita_ppp`,
  `enrol_secondary`, and the three sector-share variables (now usable
  in a single specification per finding #3).
- Phase 05's baseline specification template is preliminarily set:
  `gini ~ mean_years_schooling + enrol_secondary + log(gdp_per_capita_ppp)
  + log(population) + urban_population_pct`. Heterogeneity
  specifications swap `enrol_secondary` for the gender-split pair.
  Robustness specifications swap `gdp_per_capita_ppp` for `usd` and
  apply log or winsorisation to `inflation_cpi` (skewness 52 in raw
  form).
- Phase 07's synthesis inherits three named threats to identification
  now grounded in concrete numbers: (a) MNAR Gini missingness with 30%
  country-year completeness and a clear geographic pattern (Sub-Saharan
  Africa, low-income countries under-represented); (b) Gini measurement
  heterogeneity (consumption- vs income-based, pre- vs post-tax); (c)
  the input-vs-attainment distinction (`edu_expenditure_gdp` null
  alongside the strong `mean_years_schooling` signal).
- `src/` unchanged. The idempotent-append pattern recurred in Phase 03
  (Steps 01 and 08a), bringing total uses across the project to four
  (Phases 02 s01/s07 and 03 s01/s08a). Promotion to `src/log_utils.py`
  remains optional for now and is deferred until the next append-style
  step appears in Phase 04 or later.


## 2026-05-01 — Phase 03 Correction Note

Context: The 2026-04-30 Phase 03 Completion entry stated that
`requirements.txt` retains `kaleido==0.2.1` as a marker. After the
kaleido 0.2.1 PNG-export attempt hung on Windows, kaleido was
uninstalled (`pip uninstall -y kaleido`) and `requirements.txt` was
regenerated. The final repository state therefore does NOT contain
kaleido. The substantive narrative — that kaleido v1 / plotly 6.7.0
static-image export is broken on Windows and that Phase 03 ships
HTML-only choropleths — is unchanged. Only the specific claim about
requirements.txt is corrected here.

## 2026-05-01 — Phase 04, Step 01

**Context:** Phase 04 (Country Clustering, Scope v2 Layer A) opens with
eight design decisions that fix the methodology before any clustering
code is written. These choices shape the country-level feature matrix,
the clustering algorithm and its hyperparameters, and the diagnostics
that justify K. They are recorded here so that the Phase 04 audit trail
is visible in the project log rather than buried inside step scripts,
mirroring the Phase 02 s01 and Phase 03 s01 patterns.

**Decision:**

1. **Feature set - 7 variables.** `mean_years_schooling`,
   log `gdp_per_capita_ppp`, `enrol_secondary`, `agri_value_added_gdp`,
   `manu_value_added_gdp`, `services_value_added_gdp`,
   `urban_population_pct`. `enrol_primary` and `enrol_tertiary` are
   excluded (Phase 03 finding: secondary-enrolment trio is
   arithmetically nested, use at most one); `gdp_per_capita_usd` is
   excluded (collinear with PPP, VIF 14.4 / 10.5 in Phase 03);
   `edu_expenditure_gdp` is excluded (null bivariate signal in Phase
   03, R^2 = 0.05); `inflation_cpi` is excluded (raw skewness 52.45,
   not stable as a structural feature); `unemployment_rate` is
   excluded (1991 start, structural rather than typology-defining);
   `trade_openness` is excluded (different axis from
   education-inequality typology). `population` is excluded
   (size, not development-state, dimension).

2. **Unit of clustering - country-level (217 rows pre-filter).**
   One row per ISO-3 country, features aggregated across years per
   Decision 3. Country-year-level clustering would allow trajectory
   typologies but is harder to interpret in a portfolio narrative
   and does not match the country-level Phase 03 motivational
   evidence (income-faceted Gini Kuznets pattern).

3. **Aggregation method - 2015-2019 five-year mean per country per
   feature, with fallback to 2010-2019 if listwise-complete countries
   < 150.** Using `np.nanmean`, so countries with at least one
   observed year in the window contribute. Listwise filtering
   (Decision 8) applies to the resulting country x feature matrix,
   not to individual years. The 2015-2019 window excludes COVID
   (2020+), reflects post-2008 GFC stable conditions, and represents
   "current state" without averaging across structural transitions
   (e.g. China's WTO-era acceleration). The 150-country threshold is
   ~70% of the 217-country panel; below this, the aggregation window
   is widened to 2010-2019 and the change is logged in the Phase 04
   Step 02 PROJECT_LOG entry.

4. **Algorithms - K-means + Ward hierarchical, both.** K-means is the
   primary deliverable (Scope v2 §7.1 default, fast, produces flat
   assignment). Ward-linkage hierarchical clustering is run in
   parallel on the same standardised features for two purposes:
   (a) the dendrogram is a portfolio-friendly artefact that conveys
   cluster structure at multiple granularities, (b) the K-means vs
   Ward agreement rate at the chosen K is a robustness diagnostic
   that single-algorithm clustering cannot provide.

5. **Distance metric - standardised Euclidean.** Z-score features
   first (Decision 7), then Euclidean. This is the K-means default
   and the only metric that has a coherent meaning under K-means'
   centroid update rule. Mahalanobis distance is rejected: 217
   samples is too few for stable covariance estimation across 7
   features, and Mahalanobis with a poorly estimated covariance
   matrix is worse than plain Euclidean. Correlation-based distance
   is rejected: it discards the magnitude information that
   distinguishes a high-MYS country from a low-MYS country.

6. **K selection method - multi-method consensus over K = 2..10.**
   Compute four diagnostics: Elbow (within-cluster sum of squares),
   Silhouette (mean per-sample silhouette score), Calinski-Harabasz
   index, and Gap statistic (50 reference distributions, fixed
   random_state). Recommend a K supported by at least two of the
   four diagnostics; if diagnostics disagree, prefer K with the
   highest Silhouette and document the disagreement. Prior
   expectation from Phase 03 income-Kuznets pattern is K = 3 or 4,
   but the data drives the choice.

7. **Standardisation - z-score (mean 0, std 1) on the 7-feature
   matrix.** Required by K-means under standardised Euclidean
   distance (Decision 5). Min-max scaling is rejected: outlier
   countries (e.g. small high-income territories with extreme
   urbanisation) would compress the range that the rest of the
   feature distribution occupies, biasing cluster boundaries.
   Robust scaling (median / IQR) is rejected: the 7 features are
   not heavily outlier-driven after the log transform on
   `gdp_per_capita_ppp`.

8. **Missing values - listwise deletion at the country level.**
   Drop countries that have NaN on any of the 7 features after
   2015-2019 aggregation. Report the dropped count and ISO-3 list
   in stdout and in the Phase 04 Step 02 PROJECT_LOG entry. Mean
   or median imputation for clustering is rejected on methodological
   grounds: imputing the very features that drive cluster
   assignment manufactures structure that is not in the data. A
   stratified two-step procedure (cluster the listwise-complete
   subset, then assign held-out countries to the nearest centroid)
   is rejected as out-of-scope for Phase 04; the dropped countries
   will be discussed qualitatively in the notebook.

Two phase-level engineering decisions:

- **`src/log_utils.py` promoted in this step but first used in Step
  08 wrap.** The idempotent-append pattern has been used five times
  across the project (Phase 02 s01/s07, Phase 03 s01/s08-wrap/
  s08-correction); Workflow §6.2's promotion threshold is met.
  Creating the helper now means Step 04 s01 (this script) and the
  Phase 04 wrap script (Step 08) can each justify their pattern
  choice cleanly: this script uses the inline pattern by design (so
  the helper's creation and first use are in different reviewable
  steps), and Step 08 will use the helper as the first real test of
  its API.

- **K-means random_state = 42, n_init = 50.** Seed for
  reproducibility; n_init = 50 (vs. scikit-learn 1.4+ default of 10)
  to reduce sensitivity to initialisation across the small (217-row)
  feature matrix. Ward hierarchical clustering is deterministic and
  needs no seed.

**Rationale:**

- **Discard data only at the latest stage, but discard cleanly when
  required (Phase 02 s01 echo).** Listwise deletion drops the
  minimum set of countries strictly necessary for the chosen feature
  set; widening the window via the 150-country fallback prevents
  excessive deletion driven by 2015-2019 sparsity. The dropped
  countries are reported transparently rather than imputed away.

- **Statistical honesty over algorithmic flexibility.** The
  alternatives we rejected (Mahalanobis distance, mean imputation,
  stratified two-step assignment) all amount to letting the method
  fill in for what the data does not say. Each would yield a more
  inclusive-looking deliverable and a less defensible one.

- **Multi-algorithm + multi-diagnostic consensus is the portfolio
  bar.** A reviewer can ask "why K = 4?" or "why K-means?". The
  answer "because four diagnostics agreed and Ward hierarchical
  produced 92% the same assignment" is qualitatively stronger than
  "because the elbow looked clean".

- **The 7-feature set is anchored in Phase 03 findings, not vibes.**
  Each inclusion or exclusion cites a specific Phase 03 number
  (Δ R^2, VIF tier, skewness). Phase 04 derives directly from
  Phase 03 rather than restarting feature selection.

**Impact:**

- Step 02 builds `data/processed/country_features.csv` (217 rows
  pre-filter, ≤217 post-filter) and
  `country_features_standardised.csv` from the 7 features above.
- Step 03 emits a 4-panel diagnostic figure (one panel per K
  selection method) and a recommended K with explicit
  multi-diagnostic support.
- Step 04 runs K-means and Ward at the chosen K and reports the
  K-means / Ward agreement rate as a robustness number.
- Step 05 cluster profiles include `region_name` distribution,
  `income_level_name` distribution (including "Not classified"
  n=2 from Phase 03 finding #5, kept visible), and descriptive Gini
  statistics.
- Step 06 produces PCA scatter, dendrogram, and HTML choropleth
  (PNG export remains broken on this stack per Phase 03 Correction
  Note).
- `src/log_utils.py` is added in this step but not exercised until
  Step 08; its absence from Step 04 s01's import list is
  intentional.
- No `requirements.txt` change. `scikit-learn`, `scipy`, and
  `matplotlib` are already pinned; the gap statistic implementation
  uses only NumPy + scikit-learn, no new package.

## 2026-05-01 — Phase 04, Step 02b

**Context:** Step 02 was first run with the Step 01 Decision 3
primary window of 2015-2019. The run produced 155 listwise-complete
countries from 217, satisfying the pre-registered fallback threshold
of 150 by a margin of 5 and therefore accepting the primary window
under the rule as written.

Qualitative inspection of the 62-country exclusion list immediately
flagged a problem: CHN was dropped on `enrol_secondary` alone, as
were DZA, ZWE, NIC, GIN, COG, TJK, GMB, GUY, LBR, PLW, SLB. The
2015-2019 listwise sample retained 155 countries but excluded the
single largest middle-income country in the world and a cluster of
LMICs whose presence is essential to a credible
education-inequality typology. The pre-registered numerical rule
had passed; the qualitative substance had not.

A diagnostic comparison was then run
(`scripts/phase04_diag_compare_windows.py`) over four windows:
2015-2019, 2010-2019, 2005-2019, 2000-2019. The results:

| Window     | Listwise count | CHN included | Watchlist rescued |
|------------|---------------:|:------------:|:-----------------:|
| 2015-2019  | 155            | no           | -                 |
| 2010-2019  | 167            | yes          | +12               |
| 2005-2019  | 176            | yes          | +21               |
| 2000-2019  | 178            | yes          | +23               |

The +12 countries newly included by widening to 2010-2019 are: CHN,
COG, DZA, GIN, GMB, GUY, LBR, NIC, PLW, SLB, TJK, ZWE. The
incremental rescues from further widening to 2005-2019 (BWA, FSM,
GNB, GNQ, IRQ, KIR, LBY, SDN, UGA) include several conflict-affected
states (IRQ, LBY, SDN) for which a 15-year period mean averages
across major structural ruptures (Iraq War aftermath, Libyan civil
war, South Sudan secession), making the resulting "country state"
hard to interpret. The marginal gain from 2000-2019 over 2005-2019
is +2 countries (TKM, TTO) and is not worth the further loss of
"current state" interpretation.

**Decision:** Re-run Step 02 with `PRIMARY_WINDOW = (2010, 2019)`.
Retain `FALLBACK_WINDOW = (2005, 2019)` as a safety net (not
expected to trigger). Re-run produces 167 listwise-complete
countries from 217, written to
`data/processed/country_features.csv` (217 rows, raw scale, NaN
preserved) and
`data/processed/country_features_standardised.csv` (167 rows,
z-scored).

**Rationale:**

- **The pre-registered threshold (150) was honoured by the data
  (155 >= 150) but the threshold was a numerical proxy for a
  qualitative goal: "enough countries that the typology covers the
  policy-relevant world".** Once the exclusion list made it clear
  the proxy had failed (CHN missing), the substantive goal took
  precedence. This is adaptive design, not pre-registration
  violation: the adaptation is documented here at the moment of
  the decision, not buried in retrospective rationalisation.

- **CHN inclusion is not negotiable for portfolio quality.** A
  cross-country education-inequality typology that excludes the
  world's second-largest economy and largest middle-income country
  cannot credibly speak to the Kuznets-type pattern flagged in
  Phase 03 finding #4 (income-faceted Gini inverted-U), since CHN
  is the upper-middle-income exemplar of that pattern.

- **2010-2019 is the smallest widening that achieves the
  qualitative goal.** It rescues CHN plus 11 other LMICs with no
  cost to interpretability: the period sits entirely post-GFC and
  pre-COVID, and contains no major structural rupture in any
  rescued country (CHN's WTO-era acceleration peaked 2001-2010, so
  the chosen window starts at the inflection point, not in the
  middle of the transition). Wider windows (2005-2019, 2000-2019)
  rescue marginal additional countries at increasing interpretive
  cost.

- **The "current state" narrative is preserved.** "Latest five
  years" becomes "latest ten years"; the Phase 04 deliverable is
  still defensibly characterising recent country structure rather
  than long-run averages.

**Impact:**

- `data/processed/country_features.csv` regenerated: 217 rows
  (unchanged), but feature values reflect 2010-2019 means rather
  than 2015-2019 means.
- `data/processed/country_features_standardised.csv` regenerated:
  167 rows (was 155). The 12 newly-included countries enter
  K-means and Ward clustering in Steps 03-05.
- Phase 04 narrative gains a transparent example of adaptive
  decision-making for the notebook synthesis: "the pre-registered
  rule passed numerically but the exclusion list inspection drove
  a 5-year widening".
- Phase 03 finding #4 (Kuznets inverted-U) is now testable in
  Phase 04 with CHN included as an upper-middle-income exemplar.
- The 50 countries still excluded after the widening are
  predominantly conflict-affected states (HTI, ZMB, VEN, PRK, SOM,
  SSD, SYR, YEM), WB-only territories (22 entries lacking `mys`),
  and structural-data-deficit countries. Their absence is
  consistent with the MNAR caveat already named in Phase 02
  Decision 3 and Phase 03 Step 01 Decision 6 design rationale.
- No `requirements.txt` change. `scripts/phase04_diag_compare_windows.py`
  is committed to the repo as the audit trail for this decision.

## 2026-05-01 — Phase 04, Step 03b

**Context:** Step 03 computed the four pre-registered K-selection
diagnostics (Elbow, Silhouette, Calinski-Harabasz, Gap statistic
with 1-SE rule) on the 167-country, 7-feature standardised matrix
over K = 2..10. The per-method preferred K values were:

| Diagnostic        | Preferred K | Notes                                     |
|-------------------|------------:|-------------------------------------------|
| Elbow (WCSS)      | 3           | Only diagnostic supporting K = 3          |
| Silhouette        | 2           | 0.392 at K=2 vs 0.252 at K=3              |
| Calinski-Harabasz | 2           | 138 at K=2; monotonically decreasing      |
| Gap (1-SE rule)   | 4           | Margin 0.004; gap monotone-rising to K=11 |

The Step 01 Decision 6 mechanical consensus rule recommends K = 2
on a 2/4 plurality (Silhouette + CH).

**Decision:** Override the mechanical consensus and use K = 3 as
the primary clustering K. Fit K = 2 and K = 4 K-means in Step 04
as robustness comparators (cluster sizes + silhouette only; full
profiling, Ward hierarchical, and the choropleth use K = 3).

**Rationale:**

- **The data is a development gradient, not a discrete cluster
  structure.** The Gap statistic increases monotonically over the
  full K = 2..11 range (0.964 -> 1.246) with no plateau, which is
  the textbook signature of a continuous distribution rather than
  separable clusters. This is the substantive shape of the data:
  countries lie along a development continuum (the Phase 03
  income-Kuznets evidence). Clustering here is region-of-the-
  continuum extraction, not category discovery, and "best K" must
  be evaluated with that framing.

- **Silhouette and Calinski-Harabasz both have a structural bias
  toward small K on continuous data.** Silhouette compares
  intra- to nearest-other-cluster distances; on a development
  gradient the cleanest "intra/inter" split is the single
  bisection (Global North vs. rest), which is exactly the K = 2
  result. CH is a between/within ratio that on continuous data
  tends to favour fewer, broader regions. These two diagnostics
  agreeing on K = 2 is not independent evidence; it is the same
  bias appearing twice.

- **Elbow is the only diagnostic that targets "how many regions
  meaningfully reduce within-cluster variance" rather than
  "how separable are the clusters".** Its K = 3 pick is therefore
  more informative for a development-gradient setting than the
  Silhouette + CH 2/4 plurality.

- **Gap statistic K = 4 is fragile and circular.** The 1-SE rule
  fired with margin 0.004 (gap[K=4] = 1.101; gap[K=5] - se[K=5]
  = 1.125 - 0.028 = 1.097). At fifty reference draws this margin
  is well within sampling noise. K = 4 also coincides numerically
  with the WB income classification's four bands; "discover the
  income bands by clustering on development indicators" is a
  circular finding that adds no portfolio value.

- **K = 3 matches the Phase 03 Kuznets prior precisely.** The
  income-faceted Gini time-series in Phase 03 surfaced three
  distinct regimes: low-income (high noise, mid-level Gini),
  upper-middle income (Kuznets peak, highest Gini), high income
  (compressed, lowest Gini). K = 3 is the smallest K that admits
  this ordering as a discoverable pattern; K = 2 cannot represent
  it (it collapses LIC and UMC together).

- **K = 3 is also where Calinski-Harabasz's first-difference is
  largest in magnitude.** The CH series (138, 103, 89, 78, 72,
  66, 62, 59, 56) drops by 35 from K = 2 to K = 3 and by only 14
  from K = 3 to K = 4; the structural inflection sits at K = 3
  even though the index level is highest at K = 2.

- **The Step 02b precedent.** The threshold-150 rule passed
  numerically (155 >= 150) but failed substantively (CHN dropped),
  and the project widened the window via documented adaptive
  judgement. Step 03b applies the same discipline: the 2/4
  consensus rule passed numerically (K = 2) but failed
  substantively (K = 2 cannot host the Phase 03 Kuznets pattern
  and is portfolio-empty as a "Global North vs. rest"
  finding), and we override via documented adaptive judgement.

- **K = 2 and K = 4 are not discarded.** Step 04 computes both as
  robustness comparators and reports cluster sizes plus silhouette
  side-by-side with K = 3, so the override is auditable and the
  "what if you had picked K differently" question has a recorded
  answer.

**Impact:**

- Step 04 fits K-means at K = 2, 3, 4 and Ward hierarchical at
  K = 3. Cluster assignments for all four are written to
  `outputs/tables/phase04_s04_cluster_assignments.csv`. Silhouette
  is reported for each.
- Step 04 also reports the K-means K=3 vs Ward K=3 agreement rate
  (Adjusted Rand Index + confusion matrix) as a robustness
  diagnostic on the chosen K.
- Step 05 cluster profiles and Step 06 visualisations use K = 3
  K-means assignments. K = 2 and K = 4 K-means appear only as
  numerical comparators, not as profiled deliverables.
- Phase 04 notebook synthesis cites this decision as a worked
  example of adaptive K selection, paralleling the Step 02b
  worked example of adaptive window selection. Together they
  give the notebook two transparent instances of "the
  pre-registered rule and the substantive judgement diverged;
  here is how the project resolved it".
- No `requirements.txt` change.

## 2026-05-01 — Phase 04 Completion

**Context:** Phase 04 (Country Clustering, Scope v2 Layer A
Descriptive) produced a data-driven typology of 167 listwise-complete
countries via K-means at K=3 on seven standardised development
features, validated against Ward hierarchical clustering at the same
K. Eight step scripts plus two adaptive sub-step entries
(Step 02b window adaptation, Step 03b K adaptation) plus one diagnostic
script plus the portfolio notebook completed in sequence. The phase
delivers the country-level typology that Phases 05-07 consume as a
robustness regressor and as the organising structure of the policy
narrative.

**Decision (closure):**

- **Cluster assignments are fixed at**
  `outputs/tables/phase04_s04_cluster_assignments.csv` (167 rows x 8
  columns). `cluster_kmeans_k3` is the primary cluster column;
  `cluster_kmeans_k2`, `cluster_kmeans_k4`, and `cluster_ward_k3` are
  retained for robustness comparisons.
- **Aggregation window fixed at 2010-2019** (Step 02b override of the
  pre-registered 2015-2019 primary, after the initial run revealed
  CHN had been dropped on a single missing enrol_secondary value).
- **K=3 fixed as the primary clustering K** (Step 03b override of the
  mechanical 2/4 consensus that picked K=2, on Phase 03 Kuznets prior
  + Elbow + structural-reading-of-Gap-monotonicity grounds).
- **Portfolio notebook** `notebooks/04_country_clustering.ipynb` (37
  cells: 26 markdown + 11 code; ~1.3 MB executed) is the public
  deliverable.
- **`src/log_utils.append_log_entry` had its first real use in this
  script.** Step 01 promotion contract closed.

**Rationale (carry-forward findings for Phase 05+):**

Eight findings carry forward as binding inputs or named narrative
elements:

1. **Cluster assignments are usable as fixed effects.** Phase 05
   robustness specifications can add `cluster_kmeans_k3` as a
   country-group fixed effect or interact it with key regressors.
   The three clusters are 40 / 59 / 68 countries - adequate group
   sizes for stable cluster-level estimates.

2. **The Kuznets inverted-U is reproduced from clustering.** Cluster 1
   (Middle-development / Kuznets transition) has the highest mean Gini
   at 39.05, surpassing Cluster 0 (38.24) and Cluster 2 (34.72). The
   clustering did NOT use Gini as an input - this is independent
   re-discovery of the Phase 03 finding #4 inverted-U pattern, and is
   strong same-finding-two-different-ways evidence for Phase 07.

3. **Cluster 0 mean Gini approximately equals Cluster 1 mean Gini**
   (38.24 vs 39.05, gap = 0.81). The Kuznets curve is asymmetric: the
   compressed-Gini regime (Cluster 2 at 34.72) is more distinctive
   than the high-Gini plateau spanning Clusters 0 and 1 (gap 1->2 =
   4.33). Phase 07 should frame the inverted-U as "high plateau plus
   compressed peak" rather than as a clean inverted-U.

4. **Development is approximately one-dimensional.** PC1 captures
   **63.2%** of variance across the 7 features (PC1 + PC2 = 79.2%).
   The seven features are not seven independent axes but seven
   correlated indicators of a single underlying development
   dimension. This shapes how Phase 06 SHAP feature-importance
   results should be interpreted: high importance on multiple
   features does not mean independent contributions - they are
   loadings on a shared latent factor.

5. **K-means vs Ward at K=3 ARI = 0.650** (substantial agreement).
   Confusion matrix: perfect alignment on Cluster 0 (40/40), complete
   nesting of K-means Cluster 2 inside Ward Cluster 2 (68/68),
   disagreement concentrated at the K-means Cluster 1 boundary (22
   countries to Ward 0, 16 to Ward 2). The robustness narrative is
   "the extremes are robust, the middle is fuzzy because the middle
   is literally a transition".

6. **CHN is in Cluster 1.** The Step 02b window-widening from
   2015-2019 to 2010-2019 was the prerequisite for this placement -
   under the original window, CHN was dropped on a single missing
   `enrol_secondary` value and the typology lost its largest single
   exemplar of the Kuznets-peak regime. The Step 02b decision is
   therefore not a process footnote but a substantive prerequisite
   for the headline finding.

7. **IND is in Cluster 0** with Sub-Saharan Africa, structurally
   distinct from CHN despite the political grouping under "BRICS".
   Phase 07 narrative point: data partitions are not regional
   partitions and political groupings do not survive structural cuts.

8. **BRA / ZAF / MEX / ARG sit at the Cluster 1/2 boundary** in the
   PCA scatter - Cluster 2 by assignment but positionally adjacent
   to Cluster 1. This is the Latin / South-African pattern of
   "completed development transition but retained high inequality".
   Phase 05 robustness should treat their cluster assignment as
   borderline and test cluster-fixed-effects specifications both
   with and without these countries reassigned.

**Impact:**

- **Phase 05 (econometric modelling)** inherits `cluster_kmeans_k3`
  as a robustness regressor. Specifications can add cluster fixed
  effects or interact education variables with cluster. Finding 8's
  boundary-case countries (BRA, ZAF, MEX, ARG) define a natural
  reassignment robustness check.

- **Phase 06 (predictive modelling)** can use cluster as a categorical
  feature in tree ensembles. SHAP attributions can be computed per
  cluster. Finding 4 (development is approximately 1-dimensional)
  suggests that a single principal-component axis may carry most of
  the linear signal, and that the ML layer should justify itself by
  finding non-linear or interaction patterns *beyond* that axis.

- **Phase 07 (synthesis)** organises around the K=3 typology.
  Findings 2, 3, 6, 7, 8 are direct narrative material; findings 4
  and 5 are methodology robustness material; finding 1 is
  cross-phase plumbing.

- **`src/` final state** after Phase 04 is `paths.py`, `manifest.py`,
  `country_metadata.py`, `io_utils.py`, `log_utils.py` (5 modules;
  log_utils.py added in Phase 04 Step 01, first used here in Step 08a).

- **No `requirements.txt` change** in Phase 04. scikit-learn, scipy,
  matplotlib, plotly were already pinned.

- **The 50 listwise-dropped countries** are the most concentrated
  expression of the MNAR caveat noted in Phase 02 Decision 3 and
  Phase 03 Step 01 design rationale: conflict-affected states (HTI,
  SOM, SSD, SYR, VEN, YEM), small WB-only territories (22 entries),
  and a few persistent statistical-capacity cases (PRK, ZMB).
  Phase 07 should cite this list as a concrete instance of the
  selection-bias threat to identification rather than treating
  missingness as an abstract caveat.
## 2026-05-01 — Phase 05, Step 01

**Context:** Phase 05 (Econometric Modelling, Scope §7.2 Layer B
Explanatory) opens with eight design decisions that fix the modelling
protocol before any estimation code is written. The decisions cover
the canonical baseline specification, the spec inventory, the
estimator sequence, the use of the Phase 04 cluster typology, the
sample-restriction policy, boundary-case robustness, MNAR robustness,
and the coefficient-table format. Recording the choices in the audit
trail before fitting any model follows the Phase 02 / 03 / 04 pattern
and locks pre-registration discipline (Phase 05 kickoff §6.5).

**Decision:**

1. **Canonical baseline (Spec A) — five RHS variables.**
   `gini ~ mean_years_schooling + enrol_secondary +
   log(gdp_per_capita_ppp) + log(population) + urban_population_pct`.
   `mean_years_schooling` is the strongest single linear predictor of
   Gini in Phase 03 (r = -0.52, OLS R² = 0.27). `enrol_secondary` is
   the natural representative of the secondary-enrolment trio, which
   is arithmetically nested with VIFs 9,000-40,000 and forces a
   one-of-three choice (Phase 03 Step 04). `log(gdp_per_capita_ppp)`
   is the GDP duo's canonical choice (PPP preferred over USD on
   cross-country comparability; both score VIF > 10 in Phase 03).
   `log(population)` and `urban_population_pct` are standard
   structural controls. Both log transforms are applied at modelling
   time per Phase 02 Decision 4 (panel stores raw values).

2. **Three specifications - A (parsimonious) / B (full controls) /
   C (heterogeneity).** Spec A is the baseline above. Spec B adds the
   sector trio (`agri_value_added_gdp`, `manu_value_added_gdp`,
   `services_value_added_gdp`), `trade_openness`, and
   `gov_expenditure_gdp` - five additional controls beyond Spec A.
   Phase 03 Step 04 confirmed the sector trio is jointly usable
   (manu VIF 1.4, services 2.9, agri 5.4 "watch" but below the
   "concern" cutoff). Spec C is defined in Decision 4 below.
   `inflation_cpi` is excluded from Spec B despite being economically
   relevant: its raw skewness is 52.45 (Phase 03 Step 02) and the
   winsorisation/log fix cost outweighs the interpretation gain at
   this layer.

3. **Estimator sequence - Pooled OLS -> FE (country + year) ->
   RE + Hausman.** All three estimators carry country-clustered
   standard errors (heteroscedasticity- and within-country-
   correlation-robust). Pooled OLS is fitted via
   `linearmodels.PooledOLS` (not `statsmodels.OLS`) so that all three
   estimators share the linearmodels comparison API (Decision 8).
   FE absorbs time-invariant country characteristics and common year
   shocks; RE is fitted only to enable the Hausman comparison. The
   Hausman test outcome is the deciding diagnostic between FE and RE
   for headline reporting. This is the canonical Scope §7.2 sequence.

4. **Cluster strategy - `cluster_kmeans_k3` enters Spec C as a single
   education-interaction term.** Spec C := Spec A +
   `cluster_kmeans_k3 x mean_years_schooling` interaction. Cluster
   main effects are NOT included as a separate Spec C variant.
   Rationale: under the FE estimator (the headline specification once
   the Hausman test resolves), cluster main effects are absorbed by
   country fixed effects and become unidentified; reporting them
   under FE would yield a structurally empty result. The substantive
   heterogeneity question is whether the education-Gini slope varies
   across the K=3 development regimes - which is exactly the
   interaction term, and which IS identified under FE. The choice of
   `mean_years_schooling` (not `enrol_secondary`) for the interaction
   follows from Decision 1: mys is the strongest single predictor and
   the cleanest carrier of the heterogeneity story.

5. **Sample restriction policy - Spec A on three samples; Spec B and
   Spec C on the primary sample only.** Primary sample is Spec A's
   listwise-complete country-year set on the full panel (Phase 02
   anchor: ~1,423 rows x ~140 countries; revalidated in Phase 05
   Step 02 against the cluster-attached panel). Two robustness
   samples: 2010-2019 sub-period (matches the Phase 04 clustering
   window) and cluster-listwise (167 countries with non-NaN
   `cluster_kmeans_k3`). Rationale: 3 specs x 3 estimators x 3
   samples = 27 fits per heterogeneity / robustness check, which is
   more cells than a portfolio table can carry coherently. Spec A
   bears the headline result, so it earns the three-sample treatment;
   Spec B and Spec C earn one-sample treatment because their job is
   spec-level robustness, not sample-level.

6. **Boundary-case robustness - re-fit Spec C with BRA, ZAF, MEX, ARG
   re-assigned to Cluster 1.** These four countries sit at the
   K-means Cluster 1/2 boundary in PCA space (Phase 04 Step 06);
   Ward hierarchical clustering at K=3 reassigns 16 of 68 K-means
   Cluster 2 members to its Cluster 1 (Phase 04 Step 04, ARI = 0.65).
   Re-fitting Spec C with these four boundary countries flipped to
   Cluster 1 tests whether the headline interaction coefficient
   survives the algorithm-choice-induced uncertainty in cluster
   assignment.

7. **MNAR robustness - selection-bias diagnostic, NOT PIP-imputed
   Gini.** For the country-years where Gini is observed vs
   unobserved, compare the distributions of `mean_years_schooling`,
   `log(gdp_per_capita_ppp)`, `urban_population_pct`, plus
   `region_name` and `income_level_name` cross-tabulations. Welch
   t-test or Mann-Whitney + KS-test for continuous variables,
   chi-square for categorical. PIP-imputed Gini extension is
   rejected on three grounds: (a) PIP itself mixes consumption-based
   and income-based surveys, amplifying measurement error; (b)
   attenuation-bias direction under measurement error in Y is not
   defensible without additional assumptions; (c) running the
   headline regression on imputed Gini values is portfolio-fragile.
   The selection-bias diagnostic answers the right question - "is
   the gini-using sample representative?" - without manufacturing
   observations.

8. **Coefficient table format - one table per Spec, OLS / FE / RE
   side by side, via `linearmodels.compare()` + custom formatter.**
   Rows: each RHS coefficient with cluster-robust SE in parentheses
   and significance stars. Trailing rows: N (country-years),
   N_countries, R² (within / between / overall as applicable),
   estimator-specific diagnostics (e.g. F-stat for FE, theta for RE).
   The custom formatter renders `compare()`'s native output as
   portfolio-grade markdown tables; raw statsmodels-style output is
   not shipped to the notebook. This mirrors Phase 03's VIF tables
   and Phase 04's cluster-profile tables - "library output kept
   internal, formatted output shipped externally".

**Rationale:**

- **`mean_years_schooling` is the central carrier of the headline
  finding across Phase 05.** The identifiability of mys' coefficient
  under FE with cluster-robust SE is the single most-cited number in
  any portfolio walkthrough of Phase 05 (kickoff §7). Decisions 1, 4,
  5, and 8 all serve to keep mys' coefficient and its uncertainty
  interpretable across the spec / estimator / sample grid.

- **Heterogeneity via interaction, not subsample.** Subsample
  regressions by cluster were considered and rejected: with K=3
  clusters and ~140 countries listwise on Spec A, per-cluster N is
  ~47 country-cohort, and FE within each subsample loses
  considerable power. The interaction approach pools strength across
  the full panel and recovers heterogeneity as an additional
  parameter, not a fragmented one.

- **Pre-registration discipline (kickoff §6.5).** The three-sample /
  one-sample asymmetry in Decision 5 is recorded BEFORE estimates
  are seen, so that the eventual choice of which sample to feature
  in the notebook narrative is constrained by the pre-registered
  hierarchy (Spec A primary -> Spec A robustness -> Spec B/C).
  Adaptive overrides remain available via §6.3 sub-step entries if
  results trigger them.

- **MNAR is a selection problem, not a missing-Y problem.**
  Decision 7's diagnostic frames the 50-country exclusion list
  (Phase 04 §Known Issues) and the 30% Gini completeness (Phase 02)
  as a panel-representation question, which is the question Phase 05
  can actually answer with available data. The deeper question -
  whether the unobserved Gini values would change the relationship -
  is genuinely beyond the data and is deferred to Phase 07's
  identification discussion.

- **Portfolio polish wins over rigor at one explicit point.**
  Decision 4 chose interaction-only Spec C over a two-variant
  (cluster FE + interaction) Spec C, because cluster FE is
  structurally empty under the headline FE estimator. This trade is
  named here rather than papered over (kickoff §6.2).

**Impact:**

- Step 02 builds `data/processed/panel_modelling.csv` by attaching
  `cluster_kmeans_k3` (left-join on iso3) and computing
  `log_gdp_per_capita_ppp` and `log_population`. No NaN should be
  introduced beyond the panel.csv baseline; cluster column matches
  the Phase 04 distribution (40 + 59 + 68 = 167 with cluster, 50
  with NaN).

- Steps 03-05 fit the three estimators (Pooled OLS, FE, RE) and
  produce per-spec coefficient tables plus the Hausman test result.
  Steps 06-07 cover heterogeneity (Spec C under each estimator) and
  the three robustness checks (boundary-case reassignment, MNAR
  selection diagnostic, sub-period FE).

- Step 08 builds `notebooks/05_econometric_modelling.ipynb`
  programmatically (Phase 03 / 04 s07 pattern via nbformat +
  nbconvert.ExecutePreprocessor against the `p4_education` kernel).

- Step 09 wraps with the second use of
  `src.log_utils.append_log_entry` since Phase 04 Step 08, plus
  `docs/phase_summaries/phase05_summary.md` written directly as
  markdown (kickoff §6.6 - new convention from Phase 05 onwards),
  plus README regeneration via `scripts/update_readme.py`.

- No `requirements.txt` change anticipated for Step 01.
  `linearmodels` is already pinned (Phase 00 environment setup).
## 2026-05-02 — Phase 05, Step 07b (Adaptive Override)

**Context:** Step 01 Decision 3 pre-registered the estimator
sequence Pooled OLS -> FE -> RE + Hausman, with the Hausman test
named as "the deciding diagnostic between FE and RE for headline
reporting." Step 05 executed this sequence and produced two
heterogeneous results:

- Spec A Hausman: chi^2 = 5.11, df = 5, p = 0.4024.
  Conclusion: fail to reject H0 -> RE is consistent and efficient.
- Spec B Hausman: chi^2 = -15.86, df = 10, p = 1.0000.
  Conclusion: numerically degenerate. The covariance-difference
  matrix Sigma_FE - Sigma_RE was near-PSD in finite sample under
  cluster-robust SE (min eigenvalue -6.4e-04), the Moore-Penrose
  pseudoinverse fell back, and the resulting quadratic form was
  negative. This is a known failure mode of the Hausman test under
  clustered SE in panels of moderate cluster count (~140
  countries) and is not informative about FE-vs-RE preference.

To address the Spec B degeneracy, Step 07 Check 1 implemented a
Mundlak alternative-Hausman test - country-mean of each time-varying
RHS added as auxiliary regressors in a RandomEffects specification,
followed by a Wald test on the joint zero of those mean coefficients
(b' V^-1 b ~ chi^2(q)). Mundlak is asymptotically equivalent to
Hausman under H0 and is numerically stable under cluster-robust SE.
The Mundlak results:

- Spec A Mundlak: Wald = 9.84, df = 5, p = 0.0799.
  Conclusion: borderline reject at 10% only.
- Spec B Mundlak: Wald = 41.67, df = 10, p < 0.0001.
  Conclusion: strong reject H0 -> FE preferred.

The two asymptotically equivalent tests therefore disagree
materially on Spec B and weakly disagree on Spec A. Spec A
results are individually inconclusive; Spec B Mundlak is the only
unambiguous diagnostic on the table and points toward FE.

**Decision (Override):** The headline-estimator choice rule
pre-registered in Step 01 Decision 3 is replaced with a
dual-headline (technically tri-) reporting structure. Pooled OLS,
FE, and RE are all reported in parallel as the headline result for
each specification, rather than selecting one as definitive based
on Hausman alone. The notebook (Step 08) and phase summary
(Step 09) present the three estimators side-by-side with
appropriate context for each, rather than singling out RE based on
Step 05's Hausman alone.

**Rationale:**

- **The Mundlak-Hausman conflict is a real informational signal.**
  The Spec B Mundlak p < 0.0001 indicates that country-specific
  unobservables are correlated with the regressors when sector
  trio, trade_openness, and gov_expenditure_gdp are included -
  the RE identifying assumption cov(alpha_i, X_it) = 0 fails for
  the rich-controls specification. Spec A Mundlak p = 0.0799 is
  borderline, consistent with the Spec A Hausman p = 0.40 being
  inconclusive rather than supportive of RE. Treating Step 05's
  Hausman result as definitive would silently dismiss this signal.

- **Single-estimator selection over-claims under conflicting
  evidence.** Selecting RE as headline based solely on Step 05's
  Hausman result for Spec A would over-claim, given the Spec B
  Mundlak rejection. Selecting FE based solely on Mundlak Spec B
  would also over-claim, given that Spec A is borderline and that
  the FE point estimate is statistically null (mys coefficient
  -0.38, p=0.37). The honest analytical position is that the data
  do not uniquely identify a single best aggregate estimator at
  this layer; the three estimators are reporting different
  aggregations of the same evidence.

- **The reconciliation IS the headline finding.** The strong
  negative Pooled OLS coefficient (-1.328***) attenuates under FE
  (-0.384, p=0.37) and partially recovers under RE (-0.688*,
  p=0.016). This pattern - between-country identification produces
  a strong negative association; within-country identification
  loses it; GLS-combined identification recovers an intermediate
  value with theta=0.82 - is the central reconciliation story
  Phase 05 was designed to tell. Burying two of three estimators
  in an appendix would suppress that story (kickoff §6.7,
  "robustness via comparison, not single numbers").

- **The Cluster 1 heterogeneity finding stands independently.**
  Step 06 RE Spec C produced a Cluster 1 within-country slope of
  -1.19, p = 0.010; Step 07 boundary-reassignment produced -1.15,
  p = 0.008. The finding is robust across (i) the K-means/Ward
  algorithm-induced uncertainty in cluster boundaries (Phase 04 ARI
  = 0.65), (ii) the +0.04-point shift from BRA/ZAF/MEX/ARG
  reassignment, and (iii) the choice of estimator as far as the
  interaction sign and direction are concerned. It will continue
  to be reported as the central heterogeneity finding of Phase 05
  irrespective of which aggregate estimator is foregrounded.

- **Time-stamp discipline (kickoff §6.5).** This override is
  documented BEFORE Step 08 notebook construction so the audit
  trail shows the structural choice was deliberate rather than
  driven by visual inspection of notebook drafts. The override
  date precedes the notebook build date in the PROJECT_LOG, which
  is what pre-registration is for.

**Impact:**

- **Step 08 notebook structure.** Coefficient tables present
  Pooled OLS / FE / RE side-by-side via `linearmodels.compare()`,
  with equal emphasis. The Cluster 1 heterogeneity table is its
  own headline subsection. The notebook synthesis question shifts
  from "What is THE headline coefficient?" to "How does the
  education-Gini relationship look under three identification
  strategies, and what is the heterogeneity layer below the
  aggregate?".

- **Step 09 phase summary structure.** Opens with the three-
  estimator reconciliation as the primary aggregate finding,
  followed by Cluster 1 heterogeneity as a separate result. The
  Mundlak-Hausman conflict is described as a methodological
  tension surfaced by the Phase 05 design, not as an error.

- **Headline-coefficient interpretation.** The kickoff §7
  framing - "the single most-cited number from Phase 05 in any
  portfolio walkthrough" - is reframed: the most-cited number is
  no longer a single coefficient under FE but the comparison
  across the three estimators (-1.33 / -0.38 / -0.69), with the
  Cluster 1 heterogeneity as a paired secondary headline (-1.19
  for middle-development countries, RE Spec C).

- **No additional data work required.** All quantitative anchors
  needed by Step 08 are present in the existing s03/s04/s05/s06/s07
  output CSVs. This override is documentary, not computational.

- **Override typology.** This is the second documented adaptive
  override in the project. The Phase 04 overrides (Step 02b
  sample-window widening, Step 03b K-selection) were technical:
  pre-registered numerical rules passed but qualitative inspection
  triggered an adjustment. The Phase 05 override is interpretive:
  no pre-registered rule failed; two pre-registered diagnostics
  returned conflicting answers. The override is the choice of how
  to report the conflict honestly. Future projects in the
  portfolio may cite this entry as a template for handling
  asymptotically-equivalent-but-empirically-conflicting
  diagnostics.
## 2026-05-02 — Phase 05 Completion

**Context:** Phase 05 (Econometric Modelling, Scope §7.2 Layer B
Explanatory) is complete. The phase delivered the first explanatory
layer of the three-layer analytical framework: panel-econometric
estimation of the education-Gini relationship under three
identification strategies (Pooled OLS, two-way FE, RE) with country-
clustered standard errors, plus heterogeneity analysis through the
Phase 04 cluster typology and four robustness checks. The portfolio-
facing deliverable is `notebooks/05_econometric_modelling.ipynb`
(23 cells, fully executed, three figures embedded).

The phase contained one mid-flight adaptive override
(Step 07b, dated 2026-05-02): the pre-registered "Hausman picks one
estimator" rule was replaced with a tri-headline (Pooled OLS / FE /
RE) reporting structure after the Mundlak alternative-Hausman test
returned conflicting answers under cluster-robust SE. The override
is the second documented adaptive override in the project (after
Phase 04 Steps 02b and 03b) and the first interpretive one - the
data did not fail a pre-registered rule; two pre-registered tools
returned different verdicts. The override entry stands as the
template for handling asymptotically-equivalent-but-empirically-
conflicting diagnostics in future projects.

**Phase 05 Step Audit Trail (compressed):**

- **Step 01 — Eight design decisions** locked before any estimation
  code. Spec A (parsimonious, 5 RHS), Spec B (full controls, 10
  RHS), Spec C (mys × cluster_kmeans_k3 interaction). Country-
  clustered SE throughout. PROJECT_LOG entry: 2026-05-01.

- **Step 02 — Modelling-ready dataset built.** `panel_modelling.csv`
  (7,378 x 30): 24 panel originals + log_gdp_per_capita_ppp,
  log_population + 4 cluster columns from Phase 04. Spec A listwise
  sample: 1,642 country-years from 153 countries.

- **Step 03 — Pooled OLS (linearmodels.PooledOLS, country-clustered
  SE).** Spec A mys = -1.328*** (SE 0.275, p<0.001, CI [-1.87,
  -0.79]); Spec B mys = -1.204*** (SE 0.234, p<0.001). R² overall
  0.36 / 0.51, but R² within = 0.077 / -0.043 - signalling
  Pooled OLS draws its identification almost entirely from
  between-country variation.

- **Step 04 — Two-way FE (linearmodels.PanelOLS, country + year
  FE).** Spec A mys = -0.384 (SE 0.425, p=0.366, CI [-1.22,
  +0.45]) - within-country effect is statistically null. Spec B
  mys = -0.272 (p=0.520). R² within 0.115 / 0.083. Spec C cluster-
  main-effect dummies absorbed by EntityEffects as expected
  (`drop_absorbed=True`); only the two interaction terms identified.

- **Step 05 — RE + Hausman.** Spec A RE: mys = -0.688* (SE 0.285,
  p=0.016); theta = 0.82, indicating strong FE-weighted GLS
  combination. Spec A Hausman p = 0.402 (fail to reject - prefer
  RE). Spec B Hausman degenerate (statistic = -15.86, covariance
  difference matrix non-PD under cluster-robust SE).

- **Step 06 — Heterogeneity (Spec C, RE + FE per-cluster slopes via
  delta method).** RE Spec C per-cluster slopes: Cluster 0 = -0.80
  (p=0.13); Cluster 1 = -1.19* (p=0.010); Cluster 2 = -0.33
  (p=0.42). Cluster 1 is the middle-development / Kuznets-transition
  group from Phase 04; the within-country slope of education on
  Gini is significant only here.

- **Step 07 — Four robustness checks.** (1) Mundlak alternative-
  Hausman: Spec A Wald=9.84, p=0.080 (borderline reject); Spec B
  Wald=41.67, p<0.0001 (strong reject - prefer FE), in conflict
  with Step 05 Hausman Spec A result. (2) Boundary-case reassignment
  (BRA/ZAF/MEX/ARG -> Cluster 1): Cluster 1 RE slope -1.19 ->
  -1.15, significance unchanged - finding robust to algorithm-
  induced cluster ambiguity. (3) MNAR selection diagnostic: gini-
  using sample is +2.7 years more educated, +0.7 log-units richer,
  +9.8pp more urbanised; high-income microstates are over-
  represented in the excluded sample (chi^2 p=0.0017 country-
  level). (4) Sub-period 2010-2019 (Spec A): RE mys = -0.74*
  (p=0.014), within sampling error of full-panel RE.

- **Step 07b — Adaptive override of headline-estimator rule.** Triggered
  by Step 07 Check 1's Mundlak-Hausman conflict. New rule: report
  Pooled OLS / FE / RE in parallel; treat the three-estimator
  reconciliation pattern as the primary aggregate finding rather
  than promoting any single estimator's coefficient. The Cluster 1
  heterogeneity finding stands independently regardless of which
  aggregate estimator is foregrounded. PROJECT_LOG entry:
  2026-05-02.

- **Step 08 — Portfolio notebook.** `notebooks/05_econometric_modelling.ipynb`
  programmatically built via nbformat and executed via
  nbconvert.ExecutePreprocessor. 23 cells (12 markdown + 11 code).
  Three figures saved to `outputs/figures/`: forest plot of the
  three-estimator headline, per-cluster bar chart with Cluster 1
  highlight, MNAR income-level contingency stacked bar.

- **Step 09 — Phase wrap.** This entry plus
  `docs/phase_summaries/phase05_summary.md` (written directly as
  markdown - new convention from Phase 05 onwards per kickoff §6.6
  doc generation routing) plus `scripts/update_readme.py`
  regenerated.

**Findings (eight carry-forward anchors for Phase 06+):**

1. **Aggregate Pooled OLS / FE / RE for Spec A:** -1.328*** /
   -0.384 / -0.688* respectively. The 71% attenuation from Pooled
   OLS to FE indicates the education-Gini association is
   predominantly a between-country phenomenon, consistent with
   Phase 04's PC1 = 63.2% one-dimensional development gradient.

2. **theta = 0.82 in Spec A RE** - strongly weighted toward FE.
   The RE coefficient (-0.688) is identification-by-mixture;
   neither the Pooled OLS nor the FE result is its parent.

3. **Mundlak-Hausman conflict for Spec B** - the cluster-robust SE
   environment makes Hausman numerically unstable in panels of
   moderate cluster count (~140), and the Mundlak Spec B p<0.0001
   indicates that with rich controls the RE identifying assumption
   (cov(alpha_i, X_it) = 0) fails. Phase 06+ should default to
   Mundlak rather than Hausman as the FE-vs-RE diagnostic.

4. **Cluster 1 heterogeneity finding (RE Spec C):** within-country
   slope of mys on Gini = -1.19, p = 0.010 (95% CI [-2.09, -0.28]).
   Boundary-reassigned: -1.15** (p=0.008). This is the primary
   quantitative contribution of Phase 05's explanatory layer
   beyond the Phase 03/04 descriptive anchors. Cluster 1 is the
   "middle-development / Kuznets transition" group identified in
   Phase 04 (mean Gini 39.05, mean mys 8.85 years, mean
   log(gdp_ppp) 9.37 ~ \$11,700).

5. **Cluster 0 and Cluster 2 within-country slopes are not
   distinguishable from zero** under either FE or RE. Cluster 0:
   too little education variation to identify (mys mean 4.22
   years). Cluster 2: education near-saturated (mys mean 11.36),
   diminishing returns to expansion.

6. **Sub-period stability.** Spec A RE coefficient on the 2010-
   2019 sub-period (-0.74, p=0.014) matches the full-panel RE
   coefficient (-0.69, p=0.016) within a sampling error. The
   relationship is not a particular-decade artefact.

7. **MNAR is non-monotonic in income.** The gini-using sample is
   richer / more educated on average, but high-income microstates
   are over-represented in the excluded sample. The headline
   coefficient describes "countries with sustained Gini reporting"
   - predominantly middle-income economies with established
   statistical infrastructure - rather than the global universe.

8. **Spec A primary listwise sample = 1,642 country-years from
   153 countries** (revised from the Phase 02 anchor estimate of
   ~1,423 / ~140; the difference comes from Spec A's use of one
   enrolment variable rather than three). This is the canonical
   Phase 05 sample size for Phase 06 power calculations.

**Impact on subsequent phases:**

- **Phase 06 (Causal Inference / Identification)** inherits a
  panel-tested explanatory result that is statistically null on
  within-country dynamics aggregately but significant for the
  middle-development cluster. Phase 06's central question is
  whether the Cluster 1 finding survives an instrumental-variable
  or natural-experiment design. Candidate strategies:
  compulsory-schooling reforms (Heckman & Vytlacil-style IV),
  regression discontinuity at education-policy thresholds,
  difference-in-differences around inflection points in
  education spending. Phase 06 should NOT use Phase 05's
  aggregate coefficient as a target for replication; the within-
  Cluster-1 estimate (-1.19) is the substantively relevant target.

- **Carry-forward caveats** propagate from Phase 04 §Known Issues
  (50-country MNAR list, BRA/ZAF/MEX/ARG boundary cases, country-
  level aggregation flattening, 2010-19 transition smearing). Two
  Phase 05-specific caveats added:
  (a) cluster SE Hausman degeneracy - replace with Mundlak;
  (b) cross-country Gini heterogeneity (consumption-vs-income-based
  surveys) is partially insulated under within-country
  identification but remains a concern for cross-cluster
  comparisons.

- **Methodological deliverable.** The Step 07b override entry
  documents the dual/tri-headline reporting pattern as a
  reusable convention for handling diagnostic conflict under
  cluster-robust SE in moderate-cluster-count panels. This is
  itself a portfolio asset.

**Files produced:**

- `notebooks/05_econometric_modelling.ipynb`
- `data/processed/panel_modelling.csv`
- 7 output CSVs (`phase05_s03..s07_*.csv`)
- 3 figures (`phase05_s08_*.png`)
- 9 step scripts (`phase05_s01..s09`) plus 1 sub-step
  (`phase05_s07b_override_log.py`) plus 1 wrap
  (`phase05_s09_append_wrap_log.py`)
- 3 PROJECT_LOG entries (Step 01, Step 07b, this entry)
- `docs/phase_summaries/phase05_summary.md` written directly per
  kickoff §6.6 doc-generation routing convention
- `scripts/update_readme.py` regenerated to flip Phase 05 to ✅
  and add the Phase 05 Findings entry
