"""
Phase 04 - Step 01: Document design decisions

Purpose:
    Idempotently append the eight Phase 04 (Country Clustering) design
    decisions to PROJECT_LOG.md before any clustering code is written.
    Following the Phase 03 s01 pattern, the script detects an existing
    append by searching for the entry header marker and is safe to run
    multiple times.

Inputs:
    PROJECT_LOG.md at the project root

Outputs:
    PROJECT_LOG.md (one new entry appended on first run; no-op on reruns)

Notes:
    Phase 04 Step 01 deliberately uses the inline idempotent-append
    pattern rather than the new src/log_utils.py helper. Rationale:
    src/log_utils.py is created alongside this script (its first use
    is deferred to Step 08 wrap) so that the helper's "creation" and
    "first real use" are in different steps and reviewable separately.
"""

from __future__ import annotations

import sys
from pathlib import Path

# Make src/ importable when running from scripts/
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT_GUESS = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT_GUESS))

from src.paths import find_project_root  # noqa: E402


ENTRY_MARKER = "## 2026-05-01 \u2014 Phase 04, Step 01"

ENTRY_BODY = """
## 2026-05-01 \u2014 Phase 04, Step 01

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
"""


def main() -> int:
    """Append the Phase 04 Step 01 entry to PROJECT_LOG.md if not already present."""
    project_root = find_project_root(SCRIPT_DIR)
    log_path = project_root / "PROJECT_LOG.md"

    if not log_path.exists():
        print(f"ERROR: PROJECT_LOG.md not found at {log_path}")
        return 1

    existing = log_path.read_text(encoding="utf-8")

    if ENTRY_MARKER in existing:
        print("Phase 04 Step 01 entry already present in PROJECT_LOG.md.")
        print(f"  Marker found: {ENTRY_MARKER}")
        print("  No changes made.")
        return 0

    # Ensure exactly one trailing newline before appending
    if not existing.endswith("\n"):
        existing = existing + "\n"

    new_content = existing + ENTRY_BODY

    log_path.write_text(new_content, encoding="utf-8")

    appended_lines = ENTRY_BODY.count("\n")
    print(f"Appended Phase 04 Step 01 entry to {log_path.name}")
    print(f"  Marker: {ENTRY_MARKER}")
    print(f"  Body length: {len(ENTRY_BODY)} characters, {appended_lines} newlines")
    print(f"  File size: {len(existing)} -> {len(new_content)} characters")

    return 0


if __name__ == "__main__":
    sys.exit(main())
