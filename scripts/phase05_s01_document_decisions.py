"""
Phase 05 - Step 01: Document Phase 05 design decisions.

Purpose:
    Append a single PROJECT_LOG.md entry capturing the eight design
    decisions locked in at the start of Phase 05 (Econometric Modelling,
    Scope §7.2 Layer B Explanatory). Decisions cover canonical baseline
    specification, spec inventory, estimator sequence, cluster strategy,
    sample-restriction policy, boundary-case robustness, MNAR robustness,
    and coefficient-table format. Recording the choices before any
    modelling code is written follows the Phase 02 / 03 / 04 pattern and
    locks pre-registration discipline (Phase 05 kickoff §6.5).

Inputs:
    PROJECT_LOG.md (read for idempotency check, then appended to).

Outputs:
    PROJECT_LOG.md (Phase 05 Step 01 entry appended; idempotent on rerun
    via src.log_utils.append_log_entry).
"""

from __future__ import annotations

import random
import sys
from pathlib import Path

import numpy as np

# Reproducibility - log writing is deterministic but follow Workflow §4.2.
SEED = 42
random.seed(SEED)
np.random.seed(SEED)

# Make `src` importable regardless of where the script is invoked from.
_SCRIPT_DIR = Path(__file__).resolve().parent
_PROJECT_ROOT_GUESS = _SCRIPT_DIR.parent
if str(_PROJECT_ROOT_GUESS) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT_GUESS))

from src.log_utils import append_log_entry  # noqa: E402
from src.paths import find_project_root  # noqa: E402


MARKER = "## 2026-05-01 — Phase 05, Step 01"

ENTRY_TEXT = """## 2026-05-01 — Phase 05, Step 01

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
        print("[OK] Phase 05 Step 01 entry appended to PROJECT_LOG.md.")
    else:
        print("[NO-OP] Marker already present in PROJECT_LOG.md - file unchanged.")


if __name__ == "__main__":
    main()
