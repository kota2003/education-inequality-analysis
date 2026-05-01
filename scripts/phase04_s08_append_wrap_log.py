"""
Phase 04 - Step 08a: Phase wrap PROJECT_LOG completion entry

Purpose:
    Append the Phase 04 Completion entry to PROJECT_LOG.md summarising
    the final state, eight carry-forward findings for Phase 05+, and
    the impact on downstream phases. This script is also the first
    real use of src/log_utils.append_log_entry per the Step 01 promotion
    contract: the helper was created in Step 01, deliberately not used
    by any Phase 04 sub-step, and exercised here for the first time.

Inputs:
    PROJECT_LOG.md at the project root

Outputs:
    PROJECT_LOG.md (one new entry appended on first run; no-op on reruns)

Notes:
    The body of this entry is long because it is the closure document
    for an analytically rich phase. Phase 05+ should use
    `from src.log_utils import append_log_entry` rather than the inline
    idempotent-append pattern - the inline pattern is no longer the
    standard.
"""

from __future__ import annotations

import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR.parent))

from src.log_utils import append_log_entry  # noqa: E402  - first real use!
from src.paths import find_project_root  # noqa: E402


ENTRY_MARKER = "## 2026-05-01 \u2014 Phase 04 Completion"

ENTRY_BODY = """
## 2026-05-01 \u2014 Phase 04 Completion

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
"""


def main() -> int:
    project_root = find_project_root(SCRIPT_DIR)
    log_path = project_root / "PROJECT_LOG.md"

    appended = append_log_entry(log_path, ENTRY_BODY, ENTRY_MARKER)

    if appended:
        print(f"Appended Phase 04 Completion entry to {log_path.name}")
        print(f"  Marker: {ENTRY_MARKER}")
        print(f"  Body length: {len(ENTRY_BODY)} characters")
    else:
        print(f"Phase 04 Completion entry already present in {log_path.name}.")
        print(f"  Marker found: {ENTRY_MARKER}")
        print("  No changes made.")

    print("")
    print("  This is the first real use of src/log_utils.append_log_entry,")
    print("  closing the Step 01 promotion contract.  Phase 05+ should use")
    print("  the helper for any append-style PROJECT_LOG entries.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
