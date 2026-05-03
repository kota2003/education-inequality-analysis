"""
Phase 07 - Step 01: Document Phase 07 design decisions in PROJECT_LOG.md

Purpose:
    Idempotently append the Phase 07 Step 01 design-decisions entry to
    PROJECT_LOG.md. Records the six pre-registration decisions locked at
    Phase 07 kickoff before any synthesis code or notebook prose is
    written. Mirrors the Phase 02 / 03 / 04 / 05 / 06 Step 01 pattern.

Inputs:
    PROJECT_LOG.md (must exist at the project root)

Outputs:
    PROJECT_LOG.md with one new entry appended (idempotent on rerun)
"""

from __future__ import annotations

import sys
from pathlib import Path

# Make src/ importable when the script is run from any working directory.
# Assumes this script lives at <project_root>/scripts/.
_HERE = Path(__file__).resolve().parent
_PROJECT_ROOT_GUESS = _HERE.parent
if str(_PROJECT_ROOT_GUESS) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT_GUESS))

from src.log_utils import append_log_entry  # noqa: E402
from src.paths import find_project_root  # noqa: E402


# -----------------------------------------------------------------------------
# Marker
# -----------------------------------------------------------------------------

ENTRY_DATE = "2026-05-04"
MARKER = f"## {ENTRY_DATE} \u2014 Phase 07, Step 01"


# -----------------------------------------------------------------------------
# Entry body
# -----------------------------------------------------------------------------

ENTRY_TEXT = f"""\
{MARKER}

**Context:** Phase 07 (Synthesis & Policy Discussion, Scope \u00a77.4) opens
with six design decisions to lock before any code or notebook prose is
written. Phase 07 produces no new modelling and no new data acquisition;
its deliverable is `notebooks/07_synthesis_and_policy.ipynb` plus a
recruiter-final README that integrate Phases 03-06 into a single portfolio
narrative, name causal limits explicitly, and outline credible
identification strategies as future work. The decisions below cover
notebook narrative arc, depth of causal discussion, identification-strategy
framing, policy framing scope, README final form, and step plan / wrap
routing. Recording the choices in the audit trail before notebook
construction follows the Phase 02 / 03 / 04 / 05 / 06 pre-registration
pattern (Phase 07 kickoff \u00a76.5).

**Decision:**

1. **Notebook structure - Cluster 1 chained finding leads.** The synthesis
   notebook opens with an executive summary cell stating the Cluster 1
   headline (Phase 04 Kuznets-transition cluster: n=59 countries, mean Gini
   39.05, mean mys 8.85), followed immediately by the Step 03 convergence
   figure showing seven mys estimates on a single x-axis. The methodology
   arc is then narrated chronologically (Phase 03 Pearson r = -0.52 ->
   Phase 04 cluster isolation -> Phase 05 RE Spec C -1.19** -> Phase 06
   RF/XGB SHAP-on-mys -1.92 / -2.00) so that "what we found" and "how we
   got there" are unified. Caveats, identification strategies, policy
   framing, and structural limitations occupy the closing third.

2. **Causal section depth - single "what would it take to claim causation"
   framework.** The four Scope \u00a77.4 caveats (omitted variable bias, reverse
   causality, measurement error, selection bias from missing
   country-years) are threaded through one unified discussion rather than
   separated into four sub-sections. Framing question: "under what design
   assumptions could the Cluster 1 association be read as causal?" - each
   caveat is then presented as a violation of one of those assumptions.
   Treats the four caveats as interacting (e.g. Gini-reporting selection
   amplifies omitted-variable bias) rather than independent, and bridges
   naturally into the identification-strategies section.

3. **Identification strategies - synthetic control on BRA / MEX / ZAF
   spec'd in depth, IV / RD / DiD outlined briefly.** Phase 06's
   boundary-case holdout finding (BRA / ZAF / MEX / ARG, test R^2 = -2.4,
   mys SHAP sign flip for BRA / ZAF / MEX) is the project's most
   distinctive internal caveat and the natural bridge to a synthetic-
   control follow-up: same four countries treated, donor pool drawn from
   other Cluster 1 members, treatment defined by concrete policy events
   (e.g. BRA 1996 LDB, MEX 1992 ANMEB, ZAF 1996 SASA), inference via
   permutation tests. IV (compulsory-schooling reforms,
   Heckman-Vytlacil), RD (policy thresholds), and DiD (education-spending
   changes) are each mentioned in one or two sentences as alternative
   routes - breadth preserved, depth concentrated on the headline
   candidate.

4. **Policy framing - Cluster 1 only, conditionally framed.** Policy
   implications are written for Cluster 1 only and explicitly conditional
   on the association being causal: "if the Cluster 1 within-country slope
   of -1.19 to -2.00 reflected a causal effect, the implied theory of
   change would be ..., and the design required to test it would be ..."
   Cluster 0 and Cluster 2 are excluded from policy framing because their
   within-country slopes are statistically null in Phase 05 (-0.80 ns,
   -0.33 ns) and have low absolute SHAP magnitude in Phase 06 (-0.08 / +0.16
   for c0; -0.84 / -0.85 for c2); writing policy implications for them
   would overclaim. Convention 6.13 (no causal claims) is preserved through
   the conditional framing.

5. **README final form - one-paragraph limitations + bulleted future
   work.** The Limitations and Future Work section is one prose paragraph
   stating the project's epistemic stance (observational data, no causal
   claims, results characterise the Gini-reporting sub-population),
   followed by two bulleted lists: "Future Work" (synthetic control
   headline plus IV / RD / DiD secondary) and "Structural Limitations"
   (MNAR, Gini measurement heterogeneity, country-level aggregation,
   temporal extrapolation cost). Optimised for five-minute recruiter scan;
   full discussion lives in the notebook.

6. **Step plan - six steps, \u00a76.6 routing as proposed in kickoff \u00a75.**
   Step 01 documents these decisions (this script). Step 02 builds
   `outputs/tables/phase07_s02_synthesis_table.csv` aggregating
   cross-method anchors from Phase 03-06 CSVs. Step 03 builds
   `outputs/figures/phase07_s03_convergence.png` (matplotlib forest plot
   of seven mys estimates). Step 04 builds the synthesis notebook
   programmatically via nbformat / nbconvert (Phase 06 Step 09 pattern).
   Step 05 edits `scripts/update_readme.py` to flip Phase 07 to OK and
   lock the Limitations / Future Work section. Step 06 is the phase wrap
   with three sub-actions: `phase07_s06_append_wrap_log.py` (log_utils),
   `docs/phase_summaries/phase07_summary.md` (direct markdown per \u00a76.6),
   and git operations (commit, merge --no-ff to main,
   `git tag -a v1.0`).

**Rationale:**

- **Headline-first portfolio convention.** Decision 1 puts the project's
  single substantively converged finding in the first cell so a recruiter
  who reads only the top of the notebook still leaves with the answer.
  Burying it inside a chronological walk would cost portfolio readability
  for marginal narrative payoff. The unified "lead with finding, then walk
  methodology" structure preserves both. Independent readability of
  notebook 07 is preserved by defining Cluster 1 in the headline cell
  itself, so the synthesis notebook does not require the reader to have
  read notebooks 01-06.

- **Epistemic depth over checklist.** Decision 2's single framework
  treats the four causal caveats as interacting, which they are: Gini-
  reporting selection biases the population on which omitted-variable
  bias is measured; measurement error in Gini attenuates point estimates
  that omitted variables would otherwise inflate. A four-sub-section
  treatment loses this interaction structure. The single-framework choice
  also makes the \u00a76.13 mandate ("delineate where causal claims would
  require additional design") operational rather than rhetorical.

- **Depth signals technical seriousness.** Decision 3 picks one
  identification strategy and specs it concretely because for a portfolio
  audience, "I know how to do this" is a higher signal than "I know that
  this exists." The synthetic-control choice is forced by the
  boundary-case holdout: that finding directly identifies four treated
  units (BRA / ZAF / MEX / ARG) and a donor pool (other Cluster 1
  members), so the spec is grounded in Phase 06 evidence rather than
  selected from a menu. IV / RD / DiD remain in the picture as breadth.

- **Conditional framing is the only line that preserves \u00a76.13.**
  Decision 4's explicit conditional ("if the association were causal, the
  implied theory of change would be") allows policy reasoning to appear
  in the notebook without claiming causation. Abstaining from policy
  entirely (Decision 4 option c) reads as evasive for an applied-
  analytics portfolio audience. Framing all three clusters (option b)
  overclaims because Clusters 0 and 2 have statistically null
  within-country slopes (Phase 05 RE Spec C: -0.80 ns and -0.33 ns).
  The phase06 hand-off explicitly recommends the Cluster 1 focus.

- **README is for scanning, notebook is for reading.** Decision 5's
  one-paragraph + bulleted form respects the README's role as a
  five-minute scan artefact distinct from the notebook's twenty-minute
  read artefact. Mirroring the full Phase 07 discussion in the README
  would duplicate content and bloat the recruiter entry point.

- **Step granularity preserves audit trail.** Decision 6's six-step plan
  keeps the synthesis CSV (Step 02) and convergence figure (Step 03) as
  separate artefacts because they are semantically independent: one is a
  data table, the other is a visualisation. Collapsing them would lose
  the audit-trail clarity that a reviewer benefits from when tracing how
  the figure was derived from the table. \u00a76.6 routing is unchanged
  from Phase 05 / 06: data-dependent artefacts are Python scripts; the
  phase summary is direct markdown.

**Impact:**

- **Step 02 builds `phase07_s02_synthesis_table.csv` consolidating cross-
  method estimates.** Long-format schema with columns
  `phase, estimator, scope, mys_effect, se, ci_lower, ci_upper, p, n,
  source_artefact`. Source artefacts trace explicitly to Phase 03-06 CSVs
  / PROJECT_LOG entries (Convention \u00a76.4: cite, do not recompute).

- **Step 03 builds `phase07_s03_convergence.png` (matplotlib forest plot).**
  Seven mys estimates on one x-axis: Phase 03 univariate Pearson r
  (rescaled note), Phase 05 Pooled OLS / RE / FE Spec A, Phase 06 Ridge
  raw-scale / RF mean signed SHAP / XGB mean signed SHAP. 95% CIs where
  available (Phase 05); point markers with annotation for Phase 06 SHAP
  point estimates. matplotlib backend per Phase 03 Correction Note (no
  plotly / kaleido).

- **Step 04 builds `notebooks/07_synthesis_and_policy.ipynb` per the
  Decision 1 structure.** Approximately 7-9 sections following the
  three-part arc: (i) executive summary + Cluster 1 finding + convergence
  figure, (ii) chronological methodology walk through Phase 03-06,
  (iii) causal caveats framework + identification strategies (synthetic
  control depth) + Cluster 1 conditional policy framing + structural
  limitations + research-agenda close. Same prose-led three-layer
  interpretation pattern (Observation / Explanation / Implication) as
  Phases 03-06.

- **Step 05 edits `scripts/update_readme.py` to lock final form.**
  Phase 07 status flips to OK; the placeholder Limitations and Future
  Work section is replaced with the locked form per Decision 5; the
  Coming-soon section is removed.

- **Step 06 closes the project at v1.0.** Phase wrap log entry appended
  via log_utils; phase summary written directly as markdown; git tag
  `v1.0` placed on the main merge commit.

- **Conventions held.** \u00a76.13 (no causal claims) enforced via Decision 4's
  conditional framing. \u00a76.4 (cite, do not recompute) enforced by Step 02's
  source_artefact column. \u00a76.7 (robustness via comparison) enforced by
  Decision 1's lead-with-convergence structure. \u00a76.15 (notebook is the
  deliverable) enforced by Decision 6's step plan: every step contributes
  to notebook readability. \u00a76.12 (don't fabricate references) enforced by
  Step 02's traceability column.
"""


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

def main() -> None:
    project_root = find_project_root()
    log_path = project_root / "PROJECT_LOG.md"

    print(f"Project root : {project_root}")
    print(f"Log path     : {log_path}")
    print(f"Marker       : {MARKER}")
    print(f"Entry length : {len(ENTRY_TEXT)} characters")
    print()

    appended = append_log_entry(
        log_path=log_path,
        entry_text=ENTRY_TEXT,
        marker=MARKER,
    )

    if appended:
        print(f"[OK] Appended new entry to {log_path.name}.")
    else:
        print(
            f"[OK] Marker already present in {log_path.name}; "
            f"no change made (idempotent)."
        )


if __name__ == "__main__":
    main()
