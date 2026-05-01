"""
Phase 04 - Step 08b: Write phase summary

Purpose:
    Write `docs/phase_summaries/phase04_summary.md` per Workflow §9.1.
    The summary is Claude's handoff artefact (gitignored), not a
    public deliverable. It captures the goal, completed steps,
    deliverables, key decisions, known issues, and handoff to
    Phase 05.

Inputs:
    (none beyond the script itself; content is embedded)

Outputs:
    docs/phase_summaries/phase04_summary.md (overwritten on each run)

Notes:
    Idempotent: rerunning produces the same file bytes. Output is
    gitignored per Workflow §6.4 / .gitignore baseline.
"""

from __future__ import annotations

import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR.parent))

from src.paths import find_project_root  # noqa: E402


SUMMARY_BODY = """\
# Phase 04 Summary

**Project:** Education and Income Inequality \u2014 A Cross-Country Panel Analysis
**Phase:** 04 \u2014 Country Clustering
**Completed:** 2026-05-01

## Goal

Construct a country-level data-driven typology from the analytical panel
(`data/processed/panel.csv`, 7,378 country-year rows) using clustering
methods, validate the typology with multiple diagnostics and a second
algorithm, and characterise each cluster on its constituent features and
on descriptive Gini statistics. Test whether the typology recovers the
income-Kuznets pattern surfaced in Phase 03 finding #4 from a clustering
that does not use Gini as an input. Deliver a portfolio notebook
(`notebooks/04_country_clustering.ipynb`) that an analytics reviewer
would call genuinely insightful.

## Completed Steps

- **Step 01 \u2014 Eight design decisions documented** before any clustering
  code. Idempotent append script wrote feature-set, unit-of-clustering,
  aggregation method, algorithm, distance metric, K-selection method,
  standardisation, and missing-value-policy decisions to PROJECT_LOG.md.
  `src/log_utils.py` was created in this step but, by deliberate design,
  not exercised until Step 08a.

- **Step 02 \u2014 Country-level feature matrix.** Aggregated panel to one
  row per country via 5-year nanmean over 2015-2019, applied log
  transform to `gdp_per_capita_ppp`, listwise-dropped countries with
  any NaN among the 7 features. Produced `country_features.csv` (217
  rows, raw scale, NaN preserved) and `country_features_standardised.csv`
  (155 rows initially, z-scored). The 155-row count satisfied the
  pre-registered threshold of 150 by 5 countries, but qualitative
  inspection of the 62-country exclusion list flagged CHN as dropped.

- **Step 02b \u2014 Window adaptation.** Diagnostic comparison
  (`scripts/phase04_diag_compare_windows.py`) over four candidate
  windows (2015-2019, 2010-2019, 2005-2019, 2000-2019) showed that
  widening to 2010-2019 rescues CHN plus 11 other LMICs (COG, DZA,
  GIN, GMB, GUY, LBR, NIC, PLW, SLB, TJK, ZWE) at a country count
  of 167. Step 02 was re-run with `PRIMARY_WINDOW = (2010, 2019)`;
  the adaptive override was logged to PROJECT_LOG.md as Phase 04
  Step 02b.

- **Step 03 \u2014 K-selection diagnostics.** Computed Elbow (WCSS),
  Silhouette, Calinski-Harabasz, and Gap statistic (50 reference
  draws) over K=2..10 (gap to K=11 for the 1-SE rule). Per-method
  picks: Elbow K=3, Silhouette K=2, CH K=2, Gap (1-SE) K=4.
  Mechanical 2/4 consensus identified K=2.

- **Step 03b \u2014 K adaptation.** The mechanical K=2 was overridden to
  K=3 on three substantive grounds: (a) the gap statistic's monotone
  rise from 0.96 to 1.25 is the textbook signature of a continuous
  distribution (development gradient), making Silhouette and CH's
  small-K bias structural rather than independent evidence; (b)
  Elbow's K=3 pick targets variance reduction rather than
  separability, which is the more relevant criterion for a
  gradient-with-regimes setting; (c) Phase 03 finding #4 identifies
  three distinct Gini regimes, which K=3 is the smallest K to host.
  K=2 and K=4 retained as robustness comparators. Logged to
  PROJECT_LOG.md as Phase 04 Step 03b.

- **Step 04 \u2014 Cluster fitting and robustness.** K-means at K=2, 3, 4
  with `n_init=50, random_state=42`. Ward hierarchical at K=3.
  Labels canonicalised by ascending mean of `mean_years_schooling`
  (cluster 0 = lowest education, cluster K-1 = highest). K-means K=3
  cluster sizes: 40 / 59 / 68. Ward K=3 sizes: 46 / 37 / 84. K-means
  K=3 vs Ward K=3 Adjusted Rand Index = **0.650** (substantial
  agreement). Confusion matrix: K-means Cluster 0 100% aligned with
  Ward Cluster 0; K-means Cluster 2 100% nested in Ward Cluster 2;
  disagreement concentrated at K-means Cluster 1 boundary. Cross-K
  ARIs: K=2 vs K=3 = 0.406 (K=3 captures structure beyond
  bisection); K=3 vs K=4 = 0.706 (K=4 is mainly a refinement, not
  reorganisation).

- **Step 05 \u2014 Cluster profiles.** Per-cluster computation of income
  distribution, region distribution, feature mean/IQR statistics,
  and descriptive Gini statistics (country-level Gini means over
  2010-2019, then aggregated per cluster). Cluster sizes: 40, 59,
  68. Income composition: Cluster 0 has zero HIC/UMC; Cluster 1 is
  64% UMC dominance; Cluster 2 is 85% HIC. Sector composition
  follows the textbook structural transformation: agri 23.65% \u2192
  9.87% \u2192 2.37%, manu 9.90% \u2192 13.01% \u2192 11.76% (peak in Cluster 1),
  services 46.45% \u2192 53.78% \u2192 63.38%. **Mean Gini: Cluster 0 = 38.24,
  Cluster 1 = 39.05, Cluster 2 = 34.72** \u2014 Cluster 1 is highest,
  reproducing the Phase 03 income-Kuznets pattern.

- **Step 06 \u2014 Cluster visualisations.** PCA scatter (PC1 = 63.2%,
  PC2 = 16.0%, cumulative 79.2%) with cluster centroids and 25/25
  watchlist countries annotated. Ward dendrogram with K=3 cut at
  h=12.90; sub-tree colours auto-assigned by scipy and represent
  Ward sub-trees (not K-means cluster IDs - documented in caption).
  Plotly choropleth as self-contained HTML (~4.9 MB; embedded
  plotly.js; works offline and in GitHub preview); kaleido PNG
  export remains broken on this stack per Phase 03 Correction Note.

- **Step 07 \u2014 Portfolio notebook.** 37-cell `04_country_clustering.ipynb`
  (26 markdown + 11 code) built programmatically via nbformat and
  executed via nbconvert's `ExecutePreprocessor` against the
  `p4_education` kernel. The notebook follows the Phase 03 narrative
  pattern: prose-led with three-layer interpretation
  (Observation / Explanation / Implication) for each figure and
  table. Closes with nine synthesis takeaways and a limitations
  section. Choropleth embedded via IFrame with a Markdown-link
  fallback for GitHub previewers.

- **Step 08 \u2014 Phase wrap.** Three sub-actions:
  - `phase04_s08_append_wrap_log.py` \u2014 Phase 04 Completion entry to
    PROJECT_LOG.md, eight carry-forward findings. **First real use
    of `src.log_utils.append_log_entry`** per the Step 01 promotion
    contract.
  - `phase04_s08_write_phase_summary.py` \u2014 this file.
  - `scripts/update_readme.py` regenerated from scratch with
    Phase 04 status flipped to \u2705 and a Phase 04 Findings entry
    added covering the headline Kuznets confirmation and PC1=63.2%
    finding.

## Deliverables

- `notebooks/04_country_clustering.ipynb` \u2014 portfolio narrative
  (~1.3 MB, 37 cells: 26 markdown + 11 code, fully executed)
- `data/processed/country_features.csv` \u2014 217 rows x 11 cols
  (raw scale, NaN preserved)
- `data/processed/country_features_standardised.csv` \u2014 167 rows x
  11 cols (z-scored, listwise-complete)
- `outputs/tables/phase04_s03_k_diagnostics.csv` \u2014 K=2..10 x 6 cols
- `outputs/tables/phase04_s04_cluster_assignments.csv` \u2014 167 rows x
  8 cols (4 metadata + 4 cluster columns)
- `outputs/tables/phase04_s05_cluster_profiles.csv` \u2014 3 rows x 49 cols
- `outputs/figures/phase04_s03_k_selection.png` \u2014 4-panel diagnostic
- `outputs/figures/phase04_s06_pca_scatter.png` \u2014 PCA scatter with
  watchlist and centroids
- `outputs/figures/phase04_s06_dendrogram.png` \u2014 Ward dendrogram with
  K=3 cut line
- `outputs/figures/phase04_s06_choropleth_clusters.html` \u2014 self-
  contained interactive map
- `src/log_utils.py` \u2014 promoted helper (`append_log_entry`); first
  used in Step 08a
- 8 step scripts (`phase04_s01..s07`) plus 2 sub-step scripts
  (`s02b`, `s03b`) plus 1 diagnostic (`phase04_diag_compare_windows.py`)
  plus 3 wrap scripts (`s08_append_wrap_log`, `s08_write_phase_summary`,
  updated `update_readme.py`)
- 4 PROJECT_LOG entries: Step 01 design decisions, Step 02b window
  adaptation, Step 03b K adaptation, Phase 04 Completion

## Key Decisions and Rationale

The 8 Step 01 decisions (compressed):

1. **Feature set: 7 variables** \u2014 `mean_years_schooling`,
   log `gdp_per_capita_ppp`, `enrol_secondary`,
   `agri_value_added_gdp`, `manu_value_added_gdp`,
   `services_value_added_gdp`, `urban_population_pct`. Each
   inclusion / exclusion cited a specific Phase 03 number.
2. **Country-level clustering** (217 rows pre-filter) over country-
   year-level for portfolio interpretability and continuity with
   Phase 03 motivational evidence.
3. **2015-2019 nanmean primary, 2010-2019 fallback** if listwise <
   150. Adapted in Step 02b to **2010-2019 primary** after CHN
   exclusion was discovered.
4. **K-means + Ward hierarchical, both** \u2014 K-means as primary
   (Scope §7.1 default), Ward for the dendrogram artefact and as a
   cross-algorithm ARI robustness number.
5. **Standardised Euclidean distance** \u2014 the K-means standard.
   Mahalanobis rejected on stability grounds at N=167; correlation-
   distance rejected because it discards magnitude.
6. **Multi-method consensus K-selection** with K=2..10 over Elbow +
   Silhouette + CH + Gap (1-SE rule). Adapted in Step 03b: K=3
   override of the mechanical 2/4 consensus that picked K=2, on
   substantive (gradient-data) grounds.
7. **Z-score standardisation** \u2014 required for K-means; min-max and
   robust scaling rejected.
8. **Listwise deletion** at the country level. Mean / median
   imputation rejected (manufactures structure). Stratified two-
   step procedure rejected as out-of-scope.

Two engineering decisions:

- **`src/log_utils.py` promoted in Step 01 but first used in
  Step 08a.** Five prior project-wide uses of the inline idempotent-
  append pattern justified the promotion under Workflow §6.2;
  deferring first use ensured creation and validation could be
  reviewed in separate steps.
- **K-means `random_state = 42, n_init = 50`** \u2014 seed for
  reproducibility, n_init = 50 (vs sklearn 1.4+ default of 10) to
  reduce sensitivity to initialisation across the small (167-row)
  feature matrix. Ward is deterministic; no seed needed.

## Known Issues / Open Questions

- **The 50 listwise-dropped countries are not random.** They
  concentrate among conflict-affected states (HTI, SOM, SSD, SYR,
  VEN, YEM), small WB-only territories (22 entries), and a few
  persistent statistical-capacity cases (PRK, ZMB). Phase 07 should
  cite this list as a concrete instance of the selection-bias
  threat to identification.

- **Cluster-1 / Cluster-2 boundary cases.** BRA, ZAF, MEX, ARG sit
  at the Cluster 1/2 boundary in the PCA scatter \u2014 Cluster 2 by
  K-means assignment, but positionally adjacent to Cluster 1, and
  Ward hierarchical reassigns 16/68 K-means Cluster 2 members to
  its Cluster 1. Phase 05 robustness should test cluster-fixed-
  effects specifications both with and without these countries
  reassigned.

- **Country-level aggregation flattens within-country
  heterogeneity.** A country like India, where states span Cluster-0
  to Cluster-1 levels of development, is summarised as a single
  Cluster 0 row. Necessary at the country-panel level of analysis
  but should be flagged in any policy-implication discussion in
  Phase 07.

- **2010-2019 means smear within-window transitions.** Countries
  that have transitioned during the window (e.g. China during
  2010-2019, where mys, GDP, urbanisation all grew rapidly) are
  represented by an average. A trajectory-aware analysis is out of
  scope for Phase 04.

- **Cluster labels are interpretive, not data-derived.** "Low-dev
  / SSA-led" / "Kuznets transition" / "Mature economies" labels
  summarise the Step 05 composition tables. A reviewer who prefers
  different labels should look at Step 05 directly.

- **The kaleido / plotly PNG-export issue persists.** Choropleth
  ships as HTML only, per Phase 03 Correction Note. Not actionable
  in Phase 04.

## Handoff to Next Phase

**Phase 05 \u2014 Econometric Modelling will consume:**

- `data/processed/panel.csv` \u2014 primary panel input (unchanged)
- `outputs/tables/phase04_s04_cluster_assignments.csv` \u2014 K=3
  K-means cluster column for fixed-effects / interaction
  robustness specifications
- `outputs/tables/phase04_s05_cluster_profiles.csv` \u2014 reference
  for cluster-level interpretation in result tables
- `outputs/figures/phase04_s06_pca_scatter.png` and
  `phase04_s06_choropleth_clusters.html` \u2014 reference visualisations
  for boundary-case countries (BRA, ZAF, MEX, ARG)
- `src.paths.find_project_root()`,
  `src.manifest.manifest_variable_order()`,
  `src.country_metadata.load_country_metadata()`,
  `src.io_utils.read_csv_with_encoding_fallback()`,
  `src.log_utils.append_log_entry()` \u2014 same accessor surface

**Assumptions inherited:**

- The 7 Phase 04 features have been validated as a coherent
  development-axis basis (PC1 = 63.2% of variance). Phase 05
  baseline specifications are not constrained by this set but
  should be aware that strong multi-feature loadings reflect a
  shared latent factor.
- `cluster_kmeans_k3` is canonicalised: cluster 0 = lowest
  `mean_years_schooling`, cluster 2 = highest. Cluster IDs are
  stable across reruns of Step 04 (fixed `random_state`).
- BRA, ZAF, MEX, ARG are at the Cluster 1/2 boundary and should
  be treated as such in any cluster-stratified analysis.
- The MNAR concern is now grounded in a concrete 50-country
  list, not just a count.

**First actions of Phase 05:**

1. **Decide the canonical specifications.** Phase 03 §Handoff
   pre-suggested a baseline:
   `gini ~ mean_years_schooling + enrol_secondary + log(gdp_per_capita_ppp) + log(population) + urban_population_pct`.
   Phase 05 should review and lock the canonical set in a
   Step 01 design-decisions entry (Phase 02 / 03 / 04 pattern).
2. **Decide how to use clusters.** Three options to compare:
   (a) ignore cluster (baseline), (b) cluster fixed effects,
   (c) cluster x education interactions. Recommend running all
   three as nested specifications and reporting how clusters
   change the headline coefficient.
3. **Hausman test for FE vs RE.** Per Scope §7.2.
4. **Clustered standard errors at country level.** Per Scope
   §7.2.
5. **Boundary-case robustness check.** Re-fit with BRA, ZAF,
   MEX, ARG reassigned to Cluster 1; report sensitivity of
   headline coefficients.
6. **MNAR robustness check.** Compare gini-using specifications
   against a no-gini diagnostic specification (Phase 02
   `no_gini_diagnostic` was 3,041 rows; gini-using was 1,423
   rows). Report whether headline coefficients differ.

**Specification-design intel for Phase 05:**

- **Avoid in baseline:** `enrol_primary` (non-monotonic 100%-ceiling
  artefact, Phase 03), `edu_expenditure_gdp` (null bivariate,
  Phase 03), `gdp_per_capita_usd` (collinear with PPP, VIF 14.4),
  the gender-disaggregated enrolment pair (collinear with
  `enrol_secondary` by construction).
- **Sector shares** are usable jointly per Phase 03 finding #3 and
  Phase 04 Step 04 (no convergence issues encountered with all
  three).
- **`inflation_cpi`** has skewness 52.45 raw; needs winsorisation
  or log if used.
- **`unemployment_rate`** starts in 1991 (ILO modeled series); FE
  specifications are unaffected.
"""


def main() -> int:
    project_root = find_project_root(SCRIPT_DIR)
    out_path = project_root / "docs" / "phase_summaries" / "phase04_summary.md"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    out_path.write_text(SUMMARY_BODY, encoding="utf-8")

    n_lines = SUMMARY_BODY.count("\n")
    print(f"Wrote: {out_path.relative_to(project_root)}")
    print(f"  size: {len(SUMMARY_BODY):,} characters, {n_lines} lines")
    print(f"  destination: docs/phase_summaries/ (gitignored per Workflow §6.4)")

    return 0


if __name__ == "__main__":
    sys.exit(main())
