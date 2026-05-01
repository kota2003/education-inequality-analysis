"""
Phase 05 - Step 09a: Append Phase 05 Completion entry to PROJECT_LOG.md.

Purpose:
    Append a single PROJECT_LOG.md entry recording Phase 05
    completion. The entry captures (i) the quantitative anchors
    that Phase 06 should treat as inherited facts, (ii) the
    Phase 05 step-by-step audit trail in compressed form, (iii)
    the canonical references to Step 01 (design decisions) and
    Step 07b (adaptive override), and (iv) the carry-forward
    findings to be cited rather than recomputed in Phase 06+.

    Phase 05 is the first explanatory layer in the project's three-
    layer analytical framework. The entry's primary purpose is to
    set up Phase 06 (causal inference) with a clean inventory of
    what is settled and what is open.

Inputs:
    PROJECT_LOG.md (read for idempotency, then appended to).

Outputs:
    PROJECT_LOG.md (Phase 05 Completion entry appended; idempotent
    on rerun via src.log_utils.append_log_entry).
"""

from __future__ import annotations

import random
import sys
from pathlib import Path

import numpy as np

SEED = 42
random.seed(SEED)
np.random.seed(SEED)

_SCRIPT_DIR = Path(__file__).resolve().parent
_PROJECT_ROOT_GUESS = _SCRIPT_DIR.parent
if str(_PROJECT_ROOT_GUESS) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT_GUESS))

from src.log_utils import append_log_entry  # noqa: E402
from src.paths import find_project_root  # noqa: E402


MARKER = "## 2026-05-02 — Phase 05 Completion"

ENTRY_TEXT = """## 2026-05-02 — Phase 05 Completion

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
   log(gdp_ppp) 9.37 ~ \\$11,700).

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
"""


def main() -> None:
    project_root = find_project_root()
    log_path = project_root / "PROJECT_LOG.md"

    print(f"[INFO] Project root: {project_root}")
    print(f"[INFO] Log path:     {log_path}")
    print(f"[INFO] Marker:       {MARKER!r}")
    print(f"[INFO] Entry length: {len(ENTRY_TEXT):,} characters")
    print()

    appended = append_log_entry(
        log_path=log_path,
        entry_text=ENTRY_TEXT,
        marker=MARKER,
    )

    if appended:
        print("[OK] Phase 05 Completion entry appended to PROJECT_LOG.md.")
    else:
        print("[NO-OP] Marker already present in PROJECT_LOG.md - file unchanged.")


if __name__ == "__main__":
    main()
