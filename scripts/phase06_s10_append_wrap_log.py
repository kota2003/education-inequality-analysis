"""
Phase 06 - Step 10a: Append wrap entry to PROJECT_LOG.md.

Purpose:
    Append the Phase 06 close-out entry to PROJECT_LOG.md via
    src.log_utils.append_log_entry, mirroring the Phase 02-05 wrap
    pattern.

Inputs:
    PROJECT_LOG.md (read for idempotency check; appended on first run).

Outputs:
    PROJECT_LOG.md with one new entry under the marker
    "## 2026-05-04 - Phase 06, Step 10".
"""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT_HINT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT_HINT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT_HINT))

from src.paths import find_project_root
from src.log_utils import append_log_entry

MARKER = "## 2026-05-04 \u2014 Phase 06, Step 10"

ENTRY_TEXT = """
## 2026-05-04 \u2014 Phase 06, Step 10

**Context:** Phase 06 (Predictive Modelling & Interpretability)
closes the explanatory layer of the project by training tree-based
ML models on the Phase 05 Spec A listwise sample (1,642 cy / 153
countries), computing TreeSHAP attributions, and comparing ML
feature rankings against Phase 05 panel-econometric coefficient
rankings. Ten steps were executed end-to-end with
pre-registration discipline (Convention 6.5) and adaptive override
discipline (Convention 6.3). No override was triggered: every
boundary-warning event was recorded and held to the pre-registered
grid (Convention 6.13: predictive performance is a means).

**Decision:** Phase 06 close-out. README regenerated, phase summary
written directly as markdown (docs/phase_summaries/phase06_summary.md,
Convention 6.6). Phase 06 status flips to complete.

**Headline numbers (anchors for Phase 07):**

- Test R^2 ladder: Ridge 0.426, RF 0.706, XGBoost 0.733. The
  Ridge -> RF jump (+0.28 R^2) is the non-linear and
  interaction-driven signal that linear panel models cannot capture.
- TimeSeriesSplit minus random KFold RMSE gap rises with model
  flexibility (-0.11 / +0.41 / +0.60). The gap is the structural
  temporal-extrapolation cost on panel data, not an overfitting bug.
- SHAP global ranking: top-5 perfect agreement between RF and
  XGBoost. mean_years_schooling is rank #1 in both. Mean signed SHAP
  for mys ~ -1.13 (RF) and -1.06 (XGB), close to Phase 05 Pooled OLS
  (-1.328) and Phase 06 Ridge (-1.42), well above Phase 05 FE
  (-0.384) - ML performs mixed identification.
- Spearman rho between Phase 05 RE Spec A |coef| ranking and Phase
  06 mean |SHAP| ranking on the 5 common features = +0.30 for both
  models. The mys-#1 agreement is robust; the rest of the Phase 05
  ranking is statistically non-significant (4 of 5 coefficients have
  p > 0.19), so the rank disagreement is dominated by Phase 05
  sampling noise.
- Per-cluster mys SHAP-on-mys regression slope:
  Cluster 0 (low-dev): RF -0.08, XGB +0.16 (n=17 small).
  Cluster 1 (Kuznets transition): RF -1.92, XGB -2.00.
  Cluster 2 (mature): RF -0.84, XGB -0.85.
  Compared to Phase 05 RE Spec C: c0 -0.80 (ns), c1 -1.19 (p=0.010),
  c2 -0.33 (ns). Phase 06 corroborates the Phase 05 Cluster 1
  Kuznets-transition headline and **strengthens** it: ML estimates
  the local marginal effect at roughly 1.7 times the linear panel
  RE estimate. Phase 06 also detects a meaningful negative slope in
  Cluster 2 (mature economies) where Phase 05 found none, suggesting
  Phase 05's linear specification under-fits the mature regime.
- Robustness check (a) - Spec-A-only refit: mys is rank #1 in both
  models, mean |SHAP| 2.99 (RF) / 3.10 (XGB) vs 2.48 / 2.48 in the
  13-feature baseline. Test R^2 falls to 0.628 / 0.664 (delta R^2
  = -0.078 / -0.069). Headline rankings survive feature-set
  contraction.
- Robustness check (b) - Boundary-case 4-country holdout
  (BRA / ZAF / MEX / ARG, n=59 cy aggregate): test R^2 collapses to
  -2.4 for both RF and XGBoost. RMSE rises from ~3.5 (in-sample
  test) to ~10.2 (out-of-sample 4 countries). For BRA / ZAF / MEX,
  the mys mean signed SHAP **flips to positive** (+1.62 to +2.46
  range), opposite to the in-sample attribution. This is the
  strongest internal evidence that SHAP is correlation-not-causation
  (Convention 6.15) and that the in-sample R^2 reflects within-
  distribution interpolation.

**Step boundary-warning record (Convention 6.5 / 6.13):**

- Step 04 (RF): 3 of 5 best hyperparameters at grid boundary
  (min_samples_split=2, min_samples_leaf=1, max_depth=20).
  Grid not expanded; no Step 04b override invoked.
- Step 05 (XGBoost): 4 of 7 best hyperparameters at grid boundary
  (n_estimators=1000, subsample=1.0, reg_alpha=1.0,
  colsample_bytree=0.6). Grid not expanded; no Step 05b override
  invoked.
- Both held to pre-registration; the alternative would have been
  predictive-performance optimisation, which is not the deliverable.

**Outputs produced (this phase):**

- data/processed/panel_ml.csv (1,642 x 20)
- 8 CSVs under outputs/tables/: phase06_s03_linear_baseline,
  phase06_s03_predictions, phase06_s04_rf_results,
  phase06_s04_predictions, phase06_s05_xgb_results,
  phase06_s05_predictions, phase06_s06_shap_global,
  phase06_s06_shap_per_cluster, phase06_s06_shap_values_test_rf,
  phase06_s06_shap_values_test_xgb, phase06_s07_comparison,
  phase06_s08_robustness.
- 7 figures under outputs/figures/: shap_summary_rf,
  shap_summary_xgb, dependence_top3_rf, dependence_top3_xgb,
  brazil2015_waterfall, ranking_comparison, per_cluster_slopes.
- 2 models under outputs/models/: phase06_s04_rf.joblib,
  phase06_s05_xgb.joblib.
- notebooks/06_predictive_modelling.ipynb (40 cells, executed,
  ~3.7 MB).
- docs/phase_summaries/phase06_summary.md (direct markdown).
- README.md regenerated.

**Phase 07 hand-off:**

- Core finding to carry forward: within-cluster mys-Gini relationship
  in Cluster 1 (Kuznets transition), confirmed by three independent
  estimation strategies (Phase 05 RE Spec C, Phase 06 RF
  SHAP-on-mys, Phase 06 XGB SHAP-on-mys), with magnitudes -1.19 /
  -1.92 / -2.00.
- Phase 07 should discuss (a) why association is not causation
  (omitted variables, reverse causality, selection on Gini
  reporting); (b) what credible identification would require (IV on
  compulsory-schooling reforms, RD on policy thresholds, DiD on
  spending changes, synthetic control for boundary cases); (c)
  policy framing centred on middle-development / Kuznets-transition
  countries.
- The boundary-case holdout R^2 = -2.4 finding is the bridge to
  Phase 07's identification discussion: the same model produces
  opposite mys SHAP signs on the same country depending on whether
  similar countries were in training, which is direct evidence that
  ML predictions and SHAP attributions in this panel are not causal.

**Impact:**
- Phase 06 close-out completes the explanatory layer (descriptive
  Phases 01-04 + explanatory Phases 05-06). The final phase, Phase
  07, integrates the four substantive phases into a synthesis
  notebook plus policy / limitations discussion.
- README.md regenerated with Phase 06 ✅, Findings extended to
  Phases 01-06, "Coming soon" reduced to Phase 07.
- Phase 06 summary at docs/phase_summaries/phase06_summary.md
  (gitignored per project convention) preserves the operational
  state for the Phase 07 kickoff prompt.
"""


def main() -> None:
    project_root = find_project_root()
    log_path = project_root / "PROJECT_LOG.md"

    print(f"[phase06_s10a] project root: {project_root}")
    print(f"[phase06_s10a] log path:     {log_path}")
    print(f"[phase06_s10a] log exists:   {log_path.exists()}")
    print(f"[phase06_s10a] marker:       {MARKER}")

    appended = append_log_entry(
        log_path=log_path,
        entry_text=ENTRY_TEXT,
        marker=MARKER,
    )
    if appended:
        print("[phase06_s10a] PROJECT_LOG.md: entry appended.")
    else:
        print("[phase06_s10a] PROJECT_LOG.md: marker already present, no-op.")

    contents = log_path.read_text(encoding="utf-8")
    occurrences = contents.count(MARKER)
    print(f"[phase06_s10a] marker occurrences in file: {occurrences}")
    if occurrences != 1:
        raise RuntimeError(
            f"Expected exactly 1 marker occurrence, got {occurrences}."
        )

    print("[phase06_s10a] Step 10a complete.")


if __name__ == "__main__":
    main()
