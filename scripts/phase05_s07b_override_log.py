"""
Phase 05 - Step 07b: Adaptive override of headline-estimator rule.

Purpose:
    Append a single PROJECT_LOG.md entry documenting an adaptive
    override (kickoff §6.3) of the headline-estimator rule pre-
    registered in Step 01 Decision 3. The override is triggered by
    the Mundlak-Hausman conflict surfaced in Step 07 Check 1: two
    asymptotically equivalent tests returned contradictory results
    (Hausman p=0.402 supports RE for Spec A, Hausman is degenerate
    for Spec B; Mundlak p=0.080 borderline rejects for Spec A,
    Mundlak p<0.0001 strongly rejects for Spec B).

    The override replaces "select FE or RE based on Hausman" with
    "report Pooled OLS / FE / RE in parallel as dual-headline".
    Recorded BEFORE Step 08 notebook construction so the audit trail
    shows the structural choice was deliberate and time-stamped, not
    post-hoc rationalised against notebook drafts (kickoff §6.5).

Inputs:
    PROJECT_LOG.md (read for idempotency check, then appended to).

Outputs:
    PROJECT_LOG.md (Phase 05 Step 07b entry appended; idempotent on
    rerun via src.log_utils.append_log_entry).
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


MARKER = "## 2026-05-02 — Phase 05, Step 07b (Adaptive Override)"

ENTRY_TEXT = """## 2026-05-02 — Phase 05, Step 07b (Adaptive Override)

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
        print("[OK] Phase 05 Step 07b entry appended to PROJECT_LOG.md.")
    else:
        print("[NO-OP] Marker already present in PROJECT_LOG.md - file unchanged.")


if __name__ == "__main__":
    main()
