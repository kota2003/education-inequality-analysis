"""
Phase 07 - Step 06: Append Phase 07 Completion entry to PROJECT_LOG.md.

Purpose:
    Idempotently append the Phase 07 phase-wrap entry to PROJECT_LOG.md.
    Records the full Phase 07 close-out: the chained Cluster 1 finding
    that converged across three independent estimation strategies, the
    aggregate convergence of seven Phase 03-06 estimators, the Phase 06
    boundary-case holdout caveat, the synthetic-control follow-on
    framing, and the conditional policy positioning. The entry serves
    as the v1.0 carry-forward record for the project.

    Mirrors the Phase 04 / 05 / 06 wrap-entry pattern. Module-level
    triple-quoted string ENTRY_TEXT is passed to
    src.log_utils.append_log_entry with a marker that does not collide
    with the Step 01 marker on the same date (Step 01 marker is
    'Phase 07, Step 01'; this marker is 'Phase 07, Step 06').

Inputs:
    PROJECT_LOG.md (must exist at the project root)

Outputs:
    PROJECT_LOG.md with one new entry appended (idempotent on rerun).
"""

from __future__ import annotations

import sys
from pathlib import Path

# Make src/ importable when run from any working directory.
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
MARKER = f"## {ENTRY_DATE} \u2014 Phase 07, Step 06"


# -----------------------------------------------------------------------------
# Entry body
# -----------------------------------------------------------------------------

ENTRY_TEXT = f"""\
{MARKER}

**Context:** Phase 07 (Synthesis & Policy Discussion, Scope \u00a77.4) closes
the project by integrating the descriptive (Phase 03), typological
(Phase 04), econometric (Phase 05), and predictive / SHAP (Phase 06)
layers into a single portfolio narrative. Phase 07 produced no new
modelling and no new data acquisition; its deliverable is
`notebooks/07_synthesis_and_policy.ipynb` plus a recruiter-final README
with six inline figure embeds, and the project tags at v1.0. Six steps
were executed against the six pre-registered design decisions from the
Step 01 entry (notebook narrative arc, causal section depth,
identification strategy depth, policy framing scope, README final
form, step plan). No adaptive override (Convention 6.3) was triggered
during Phase 07; every step landed on its pre-registered structure
with one within-scope extension to Step 05 (raw GitHub URL figure
embeds in the README's Findings section, applied as a portfolio-polish
addition rather than a Step 05b override since Decision 5(b) governed
only the Limitations section structure and the embed extension does
not alter any analytical claim).

**Decision:** Phase 07 close-out. Project tags at v1.0. README
regenerated with six inline figures and locked Limitations / Future
Work section, phase summary written directly as markdown
(`docs/phase_summaries/phase07_summary.md`, Convention 6.6), Phase 07
status flipped to complete in the PHASE_STATUS table.

**Headline findings (the v1.0 carry-forward):**

1. **Cluster 1 chained finding (the project's substantively converged
   anchor).** Across three independent estimation strategies, the
   within-country slope of Gini on mean years of schooling in the
   middle-development / Kuznets-transition cluster is:
   - Phase 05 RE Spec C: \u22121.19 (p = 0.010, 95% CI [\u22122.09, \u22120.28])
   - Phase 06 RF SHAP-on-mys regression on test set (n=105 cy): \u22121.92
   - Phase 06 XGB SHAP-on-mys regression on test set (n=105 cy): \u22122.00

   The chain is robust to BRA/ZAF/MEX/ARG boundary reassignment
   (Phase 05 RE Spec C \u22121.15\\*\\*, p = 0.008) and is described as a
   robust negative association rather than a causal effect.

2. **Aggregate convergence (the methodology-comparison headline).**
   Seven Phase 03\u201306 estimators all return negative point estimates:
   Phase 03 univariate Pearson r = \u22120.52; Phase 05 Spec A Pooled OLS
   \u22121.328\\*\\*\\* / FE \u22120.384 ns / RE \u22120.688\\*; Phase 06 Ridge
   raw-scale \u22121.42, RF mean signed SHAP \u22121.13, XGB mean signed SHAP
   \u22121.06. Six of seven sit in the \u22120.4 to \u22121.4
   Gini-points-per-mys-year range; the seventh (Phase 03 Pearson r)
   is unitless. The convergence figure
   (`outputs/figures/phase07_s03_convergence.png`) is the project's
   primary cross-method visualisation.

3. **The single substantive caveat (Phase 06 boundary-case holdout).**
   When Brazil, South Africa, Mexico, and Argentina are removed from
   training and evaluated as a 59-country-year out-of-sample set, both
   Random Forest and XGBoost produce test R\u00b2 = \u22122.4 with mys mean
   signed SHAP flipping sign for Brazil, South Africa, and Mexico
   (range +1.62 to +2.46). This is the strongest internal evidence
   that the in-sample SHAP attribution reflects within-distribution
   interpolation rather than transportable causal structure, and it
   directly motivates the synthetic-control follow-on study specified
   in notebook \u00a77.

4. **Synthetic control on BRA / MEX / ZAF as the natural follow-up.**
   Treatment events: BRA 1996 LDB / 1998 FUNDEF, MEX 1993 ANMEB,
   ZAF 1996 SASA. Donor pool: \u224855 remaining Cluster 1 countries
   (excluding ARG as the fourth boundary-case unit). Inference via
   in-place placebo permutation. IV (compulsory-schooling reforms),
   RD (sub-national policy thresholds), and DiD (education-spending
   changes) are outlined briefly as alternative routes.

5. **Conditional policy framing.** Notebook \u00a78 provides policy
   implications for Cluster 1 only, framed in explicit
   "if ... were causal, then ..." conditional form. Clusters 0 and 2
   are deliberately excluded because their within-country slopes are
   statistically null in Phase 05 (\u22120.80 ns, \u22120.33 ns) and
   substantively small in Phase 06 (\u22120.08 / +0.16 for Cluster 0,
   \u22120.84 / \u22120.85 for Cluster 2).

**Step record:**

- **Step 01** \u2014 `phase07_s01_document_decisions.py`. Documented six
  pre-registration decisions in PROJECT_LOG.md via
  `src.log_utils.append_log_entry`. Marker
  `## 2026-05-04 \u2014 Phase 07, Step 01`. ~10,800-character entry
  covering notebook structure (Cluster 1 chained finding leads),
  causal section depth (single "what would it take to claim
  causation?" framework), identification strategy depth (synthetic
  control specified, IV / RD / DiD outlined), policy framing scope
  (Cluster 1 only, conditional), README final form (Decision 5(b):
  one-paragraph epistemic stance + bulleted Future Work + bulleted
  Structural Limitations), and step plan (6 steps, Convention 6.6
  routing).

- **Step 02** \u2014 `phase07_s02_build_synthesis_table.py`. Built
  `outputs/tables/phase07_s02_synthesis_table.csv` (16 rows \u00d7 10
  columns). Long-format consolidation of cross-method mys estimates
  from Phase 03\u201306 anchors with `source_artefact` traceability column
  (Convention 6.4: cite, do not recompute). Schema: `phase, estimator,
  scope, mys_effect, se, ci_lower, ci_upper, p, n, source_artefact`.
  Phase coverage: 03 (1 row) / 05 (6 rows) / 06 (9 rows). Scope
  coverage: aggregate (7 rows) / cluster_0..2 (3 rows each). Validated
  with embedded sanity-check assertions on the Cluster 1 chained
  finding.

- **Step 03** \u2014 `phase07_s03_build_convergence_figure.py`. Built
  `outputs/figures/phase07_s03_convergence.png` (matplotlib forest
  plot, ~10 \u00d7 6 inches at 150 dpi). Seven aggregate-scope estimators
  on a single x-axis with phase-keyed colour / marker encoding (Phase
  03 grey square / Phase 05 blue circle / Phase 06 orange triangle),
  Phase 05 95% CI horizontal error bars, significance stars on Phase
  05 labels (\\*\\*\\* / \\* / ns), and inline value annotations.
  Required CSV-roundtrip type-discipline fix
  (`dtype=\u007b"phase": str\u007d`) to preserve zero-padded phase IDs after
  initial KeyError on first execution.

- **Step 04** \u2014 `phase07_s04_build_notebook.py`. Built
  `notebooks/07_synthesis_and_policy.ipynb` programmatically via
  nbformat / nbconvert (Phase 06 Step 09 pattern). 41 cells (34
  markdown + 7 code) across 10 sections embedding 6 figures: Phase 04
  PCA scatter; Phase 05 forest plot + cluster slopes; Phase 06
  per-cluster slopes + SHAP summary XGB; Phase 07 convergence.
  Notebook reads only Step 02 CSV and pre-existing PNG artefacts (no
  new estimation, Convention 6.4). Pre-flight verifies all 7 required
  inputs before kernel start. Output: 1903.4 KB (1.86 MB). HTML-export
  verification confirmed Convention 6.13 (no causal claims): all 9
  instances of "causal effect" appear in conditional, negation, or
  framing contexts; no claim of causation made anywhere in the
  notebook.

- **Step 05** \u2014 `scripts/update_readme.py` regenerated. Phase 07
  status flipped to complete in the PHASE_STATUS table; "Coming soon
  (synthesis & policy, Phase 07)" subsection removed; Phase 07
  Findings entry added (chained Cluster 1 finding, convergence figure
  link, boundary-case caveat, synthetic-control depth, conditional
  policy framing); Limitations / Future Work section replaced with
  the locked Decision 5(b) form. Within-scope extension applied at
  user request: six raw-GitHub-URL figure embeds with italic captions
  added at the Phase 04 / 05 / 06 / 07 entry tails, matching the
  cumulative portfolio convention used in earlier projects (e.g.
  food-security-risk-analysis). Final README state: 23,043 characters
  / 414 lines / 6 figures / 8 phases complete.

- **Step 06** \u2014 this entry plus
  `docs/phase_summaries/phase07_summary.md` (written directly as
  markdown per Convention 6.6, no Python wrapper) plus git operations:
  commit, merge `--no-ff` to main, `git tag -a v1.0`.

**Outputs produced (Phase 07):**

- 5 step scripts: `phase07_s01_document_decisions.py`,
  `phase07_s02_build_synthesis_table.py`,
  `phase07_s03_build_convergence_figure.py`,
  `phase07_s04_build_notebook.py`, and
  `phase07_s06_append_wrap_log.py` (this script). No `s05` script
  \u2014 Step 05 was an in-place edit of the existing
  `scripts/update_readme.py`.
- 1 CSV: `outputs/tables/phase07_s02_synthesis_table.csv` (16 \u00d7 10).
- 1 PNG: `outputs/figures/phase07_s03_convergence.png` (forest plot).
- 1 notebook: `notebooks/07_synthesis_and_policy.ipynb` (41 cells,
  1.86 MB).
- 1 markdown: `docs/phase_summaries/phase07_summary.md` (gitignored).
- 2 PROJECT_LOG entries: Step 01 design decisions and Step 06 phase
  wrap (this entry).
- README.md regenerated to its locked v1.0 final form.
- `scripts/update_readme.py` updated with figure-embed extension.
- Git: commit + `--no-ff` merge to `main` + tag `v1.0`.

**No `requirements.txt` change.** Phase 07 used pandas, matplotlib,
nbformat, nbconvert, and IPython.display \u2014 all pinned in earlier
phases.

**Project closing statement.** The project documents a robust
negative association between mean years of schooling and the Gini
coefficient in cross-country panel data 1990\u20132023, strongest within
the 59-country middle-development / Kuznets-transition cluster. The
association is not a causal effect: four threats (omitted variable
bias, reverse causality, measurement error, selection bias from
Gini reporting) interact in observational panel data in ways that
this project's methods cannot resolve, and Phase 06's boundary-case
holdout provides direct internal evidence that the in-sample SHAP
attribution is within-distribution interpolation rather than
transportable causal structure. The project's contribution is a
clean characterisation of the within-Gini-reporting-population
association, a country-level typology in which the association is
heterogeneous and concentrated in the Kuznets-transition middle, and
a research agenda built around the specific identification strategies
most directly motivated by the project's own internal evidence \u2014
synthetic control on Brazil / Mexico / South Africa as the headline
follow-up, sub-national replication and a compulsory-schooling-reform
IV as complementary routes. **Project closes at v1.0 \u2014 2026-05-04.**
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
        print(f"[OK] Appended Phase 07 Completion entry to {log_path.name}.")
    else:
        print(
            f"[OK] Marker already present in {log_path.name}; "
            f"no change made (idempotent)."
        )


if __name__ == "__main__":
    main()
