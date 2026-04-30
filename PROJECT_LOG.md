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