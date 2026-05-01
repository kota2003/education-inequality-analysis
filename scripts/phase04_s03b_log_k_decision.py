"""
Phase 04 - Step 03b: Document K=3 override of mechanical consensus

Purpose:
    Append an entry to PROJECT_LOG.md recording that the four-way
    K-selection diagnostic (Step 03) returned K=2 by mechanical
    consensus (2 of 4 diagnostics) but is being overridden to K=3
    based on (a) the structural reading of the diagnostics
    themselves and (b) the Phase 03 Kuznets prior. K=2 and K=4 are
    retained as robustness comparators in Step 04.

    This is the same audit-trail discipline applied at Step 02b for
    the window decision: pre-registered numerical rule honoured,
    qualitative override documented at the moment of the decision.

Inputs:
    PROJECT_LOG.md at the project root

Outputs:
    PROJECT_LOG.md (one new entry appended on first run; no-op on reruns)

Notes:
    Inline idempotent-append pattern, per the Step 01 contract that
    src/log_utils.py is not exercised until Step 08 wrap.
"""

from __future__ import annotations

import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR.parent))

from src.paths import find_project_root  # noqa: E402


ENTRY_MARKER = "## 2026-05-01 \u2014 Phase 04, Step 03b"

ENTRY_BODY = """
## 2026-05-01 \u2014 Phase 04, Step 03b

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
"""


def main() -> int:
    project_root = find_project_root(SCRIPT_DIR)
    log_path = project_root / "PROJECT_LOG.md"

    if not log_path.exists():
        print(f"ERROR: PROJECT_LOG.md not found at {log_path}")
        return 1

    existing = log_path.read_text(encoding="utf-8")

    if ENTRY_MARKER in existing:
        print("Phase 04 Step 03b entry already present in PROJECT_LOG.md.")
        print(f"  Marker found: {ENTRY_MARKER}")
        print("  No changes made.")
        return 0

    if not existing.endswith("\n"):
        existing = existing + "\n"

    new_content = existing + ENTRY_BODY

    log_path.write_text(new_content, encoding="utf-8")

    appended_lines = ENTRY_BODY.count("\n")
    print(f"Appended Phase 04 Step 03b entry to {log_path.name}")
    print(f"  Marker: {ENTRY_MARKER}")
    print(f"  Body length: {len(ENTRY_BODY)} characters, {appended_lines} newlines")
    print(f"  File size: {len(existing)} -> {len(new_content)} characters")

    return 0


if __name__ == "__main__":
    sys.exit(main())
