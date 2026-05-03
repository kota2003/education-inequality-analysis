"""
Phase 06 - Step 01: Document design decisions.

Purpose:
    Append the eight Phase 06 design decisions to PROJECT_LOG.md
    before any modelling code is written, following the Phase 02 /
    03 / 04 / 05 pre-registration pattern.

Inputs:
    PROJECT_LOG.md (read for idempotency check; appended to on first run).

Outputs:
    PROJECT_LOG.md with one new entry under the marker
    "## 2026-05-02 - Phase 06, Step 01".
"""

from __future__ import annotations

import sys
from pathlib import Path

# Make src importable when run from project root or scripts/.
PROJECT_ROOT_HINT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT_HINT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT_HINT))

from src.paths import find_project_root
from src.log_utils import append_log_entry

MARKER = "## 2026-05-02 \u2014 Phase 06, Step 01"

ENTRY_TEXT = """
## 2026-05-02 \u2014 Phase 06, Step 01

**Context:** Phase 06 (Predictive Modelling & Interpretability,
Scope v2 Layer C) opens with eight design decisions to lock before
any modelling code runs. Phase 06 trains tree-based ML models on
the same panel that Phase 05 used for econometric estimation,
computes SHAP attributions, and performs a quantitative comparison
of ML feature rankings against Phase 05 coefficient rankings. The
portfolio question Phase 06 answers is "do flexible non-linear
models tell the same story as the panel-econometric estimates,
and where do they diverge?". Pre-registration discipline matches
the Phase 02 / 03 / 04 / 05 pattern.

**Decision:**

1. **Feature set - Phase 06 extended specification, 13 features
   on the Spec A listwise row set (1,642 country-years, 153
   countries).** The row set is locked at Phase 05 Spec A's
   listwise sample to keep Phase 06 directly comparable to the
   canonical RE Spec A coefficients. The feature set extends Spec
   A's five RHS variables to 13 by adding back four covariates
   from Spec B (sector trio, trade_openness, gov_expenditure_gdp),
   two covariates that Phase 05 excluded only on transformation /
   start-date grounds (inflation_cpi raw skewness 52.45 -
   tree models scale-invariant; unemployment_rate 1991 ILO start
   - tree models tolerate; both add cheaply), and the Phase 04
   cluster typology (cluster_kmeans_k3) as a categorical feature.
   edu_expenditure_gdp is excluded (null bivariate R^2 approx 0.05
   per Phase 03; high missingness offers no compensating signal).
   Feature list: mean_years_schooling, enrol_secondary,
   log_gdp_per_capita_ppp, log_population, urban_population_pct,
   agri_value_added_gdp, manu_value_added_gdp,
   services_value_added_gdp, trade_openness,
   gov_expenditure_gdp, inflation_cpi, unemployment_rate,
   cluster_kmeans_k3.

2. **Models - Random Forest, XGBoost, Ridge baseline.** RF
   (sklearn.ensemble.RandomForestRegressor) and XGBoost
   (xgboost.XGBRegressor) as primary tree ensembles. Ridge
   (sklearn.linear_model.Ridge with alpha tuned by CV) as the
   linear baseline rather than LinearRegression because the
   13-feature extended set retains light multicollinearity that
   Ridge handles defensibly. LightGBM is not included - it is
   functionally redundant with XGBoost for the comparison
   narrative and would crowd the result tables.

3. **Train / test / CV - temporal holdout, year <= 2018 train,
   2019-2023 test.** Approximately 80 / 20 split on the 1,642
   country-year sample. Within-train hyperparameter search uses
   sklearn TimeSeriesSplit(n_splits=5). Cross-country leakage is
   not enforced because the natural panel-prediction question is
   "given a country's history, can we predict its future Gini",
   and country-grouped CV would change the question. As a
   diagnostic, random KFold metrics are also reported alongside
   the temporal-split metrics; a large gap indicates leakage or
   overfitting.

4. **Hyperparameter tuning -
   sklearn.model_selection.RandomizedSearchCV, n_iter=50,
   random_state=42.** Optuna is overkill at this sample size.
   scoring is neg_root_mean_squared_error. Pre-registered search
   spaces:
   - RF: n_estimators in {200, 500, 1000}; max_depth in
     {None, 5, 10, 20}; min_samples_split in {2, 5, 10};
     min_samples_leaf in {1, 2, 5}; max_features in
     {"sqrt", "log2", 0.5, 1.0}.
   - XGBoost: n_estimators in {100, 300, 500, 1000};
     max_depth in {3, 5, 7, 10}; learning_rate in
     {0.01, 0.05, 0.1}; subsample in {0.6, 0.8, 1.0};
     colsample_bytree in {0.6, 0.8, 1.0}; reg_lambda in
     {0, 1, 5, 10}; reg_alpha in {0, 0.1, 1.0}.
   - Ridge: alpha in np.logspace(-3, 3, 13).

5. **Cluster handling - label-encoded integer with
   model-specific NaN policy.** cluster_kmeans_k3 enters as an
   integer 0 / 1 / 2 (Phase 04 IDs are canonicalised by
   ascending mean_years_schooling, so the ordinal scale is
   informative). The ~50 Phase-04-excluded countries that lack
   a cluster assignment are handled per model: XGBoost retains
   NaN natively; RF substitutes -1 sentinel (a fourth
   "unclustered" category that the tree can split on); Ridge
   uses one-hot encoding to avoid an ordinal-scale assumption.

6. **SHAP design - TreeSHAP global + per-cluster + one local +
   top-3 dependence plots; SHAP interaction values skipped.**
   shap.TreeExplainer on RF and XGBoost. Global mean |SHAP|
   ranking with summary beeswarm. Per-cluster mean |SHAP| and
   mean signed SHAP for cluster 0 / 1 / 2 on the test set. One
   local explanation: Brazil 2015 waterfall (Phase 04
   boundary-case Cluster 1/2; Phase 05 boundary-reassignment
   robustness anchor). SHAP dependence plots for the top-3
   features by global mean |SHAP|, with auto-coloured
   interacting feature, to surface non-linearity / threshold
   effects beyond the PC1 = 63.2% one-dimensional development
   gradient. SHAP interaction values (N x F^2 ~ 1,642 x 169
   ~ 280k cells) are computationally feasible but the
   portfolio-narrative gain over dependence plots does not
   justify the interpretation cost.

7. **Phase 05 vs Phase 06 comparison - two quantitative axes
   plus one qualitative.** (i) Spearman rho between Phase 05 RE
   Spec A absolute coefficient ranking and Phase 06 mean |SHAP|
   ranking on the five common features, computed separately for
   RF and XGBoost. (ii) Per-cluster mys SHAP (mean signed) vs
   Phase 05 RE Spec C per-cluster slopes (Cluster 0 = -0.80,
   Cluster 1 = -1.19, Cluster 2 = -0.33), evaluated on
   direction agreement and magnitude ordering. (iii)
   Qualitative: shape of the mys SHAP dependence plot (linear
   vs threshold vs non-monotonic) - the demonstration of where
   ML adds information beyond Phase 05's linear story.
   Convention 6.15: SHAP attribution is a feature-importance
   decomposition over predictions, not a causal estimate. The
   comparison is between two non-causal characterisations of
   the same data; Phase 07 owns any causal framing.

8. **Output schema - long-format CSVs with explicit model and
   cluster columns.** Tables under outputs/tables/:
   phase06_s03_linear_baseline.csv,
   phase06_s04_rf_results.csv,
   phase06_s04_predictions.csv,
   phase06_s05_xgb_results.csv,
   phase06_s05_predictions.csv,
   phase06_s06_shap_global.csv,
   phase06_s06_shap_per_cluster.csv,
   phase06_s06_shap_values_test_rf.csv,
   phase06_s06_shap_values_test_xgb.csv,
   phase06_s07_comparison.csv,
   phase06_s08_robustness.csv. Models under outputs/models/:
   phase06_s04_rf.joblib, phase06_s05_xgb.joblib. Figures:
   phase06_s06_shap_summary_rf.png, phase06_s06_shap_summary_xgb.png,
   phase06_s06_shap_per_cluster.png,
   phase06_s06_dependence_top3_{rf,xgb}.png,
   phase06_s06_brazil2015_waterfall.png,
   phase06_s07_ranking_comparison.png. Predictions CSVs carry
   iso3, year, split, y_true, y_pred, residual, cluster.

**Rationale (anchors for Phase 06+):**

- **Spec A row set, Spec-B-plus feature set.** Phase 06 inherits
  Phase 05's 1,642-country-year canonical sample so the SHAP-vs-
  coefficient comparison is on the same identification window.
  The feature-set expansion is the Phase 06-specific design
  lever: tree models are scale-invariant and tolerate
  multicollinearity, so variables Phase 05 had to drop on
  econometric grounds re-enter cheaply.

- **Temporal holdout matches Convention 6.14.** Random K-fold
  on panel data leaks future into past; year-based holdout
  isolates train years from test years. The country-grouped CV
  alternative would test "given other countries' histories, can
  we predict a never-seen country's Gini" - a different, harder,
  and less Phase-05-comparable question.

- **RandomizedSearchCV over Optuna.** 50 random draws from the
  pre-registered grids cover the search space adequately at
  N = 1,642 and produce a portfolio-defensible audit trail
  (best_params_, cv_results_) without a Bayesian-optimisation
  framework that reviewers may need to relearn.

- **Cluster as label-encoded integer.** Phase 04 IDs are
  canonicalised by ascending mean_years_schooling, so 0 < 1 < 2
  is meaningful for tree splits. One-hot encoding for trees
  fragments the same information across three columns and tends
  to produce shallower splits on the cluster axis.

- **TreeSHAP, no interaction values.** Convention 6.13:
  predictive performance is a means, not a deliverable; the
  comparison story is. Dependence plots with auto-coloured
  interacting features deliver the non-linearity / threshold
  evidence in a form a reviewer can read in 30 seconds; SHAP
  interaction values would deliver the same evidence in a
  format that requires several minutes of orientation.

- **Two-axis quantitative + one-axis qualitative comparison.**
  Spearman rho on global rank gives a single defensible number
  for the executive summary. Per-cluster signed-SHAP-vs-slope
  mapping gives the Phase 05 Cluster 1 finding's ML
  counterpart. The dependence-plot shape is where ML earns its
  keep beyond linear estimation; without at least one
  qualitative finding here, Phase 06 risks being a confirmation
  of Phase 04 PC1 rather than an independent contribution
  (Convention 6.13).

- **No requirements.txt change.** xgboost, shap, scikit-learn
  pinned at Phase 00. joblib is sklearn's documented dependency.

**Impact:**

- Step 02 builds panel_ml.csv on the Spec A row set with the
  13-feature feature set, log transforms applied at modelling
  time per Phase 02 Decision 4, train (year <= 2018) / test
  (2019-2023) masks materialised as columns.
- Steps 03-05 fit Ridge / RF / XGBoost on the locked
  train mask with the pre-registered hyperparameter grids and
  write predictions plus best-params to outputs/tables/.
- Step 06 computes TreeSHAP attributions; Step 07 runs the
  Phase 05 vs Phase 06 ranking and per-cluster comparisons;
  Step 08 robustness checks: feature-set sensitivity (Spec-A-
  only re-fit) plus BRA / ZAF / MEX / ARG holdout.
- Step 09 builds the portfolio notebook
  notebooks/06_predictive_modelling.ipynb via
  nbformat / nbconvert, Step 10 wraps the phase.
- Adaptive override discipline (6.3) remains available: any
  pre-registered numerical rule that fails substantively
  (e.g. RandomizedSearchCV's best params land at the search-
  space boundary, requiring grid expansion) gets a Step XXb
  PROJECT_LOG entry rather than a silent grid edit.
"""


def main() -> None:
    project_root = find_project_root()
    log_path = project_root / "PROJECT_LOG.md"

    print(f"[phase06_s01] project root: {project_root}")
    print(f"[phase06_s01] log path:     {log_path}")
    print(f"[phase06_s01] log exists:   {log_path.exists()}")
    print(f"[phase06_s01] marker:       {MARKER}")

    appended = append_log_entry(
        log_path=log_path,
        entry_text=ENTRY_TEXT,
        marker=MARKER,
    )

    if appended:
        print("[phase06_s01] PROJECT_LOG.md: entry appended.")
    else:
        print("[phase06_s01] PROJECT_LOG.md: marker already present, no-op.")

    # Sanity check: re-read and confirm marker presence.
    contents = log_path.read_text(encoding="utf-8")
    occurrences = contents.count(MARKER)
    print(f"[phase06_s01] marker occurrences in file: {occurrences}")
    if occurrences != 1:
        raise RuntimeError(
            f"Expected exactly 1 marker occurrence, got {occurrences}. "
            f"Investigate before proceeding."
        )

    print("[phase06_s01] Step 01 complete.")


if __name__ == "__main__":
    main()
