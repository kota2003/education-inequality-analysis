# Findings

**Project:** Education and Income Inequality — A Cross-Country Panel Analysis
**Status:** v1.0 closed 2026-05-04
**Source notebooks:** `notebooks/01_data_collection.ipynb` through `notebooks/07_synthesis_and_policy.ipynb`

This document is a condensed standalone reference to the project's substantive findings, intended for readers who want the empirical content without traversing all seven notebooks. Each finding is anchored to its source phase, source notebook section, and source PROJECT_LOG entry for traceability.

---

## Finding 1 — The Cluster 1 chained finding (the v1.0 substantive anchor)

**Across three independent estimation strategies, the within-country slope of Gini on mean years of schooling in the middle-development / Kuznets-transition regime converges between −1.19 and −2.00.**

| Strategy | mys slope | p-value | n | Source artefact |
|---|---:|---:|---:|---|
| Phase 05 RE Spec C linear panel (delta-method SE) | **−1.19** | **0.010** | 1,605 cy | `outputs/tables/phase05_s07_per_cluster_slopes.csv` |
| Phase 06 RF SHAP-on-mys regression on test set | **−1.92** | — | 105 cy | `outputs/tables/phase06_s07_per_cluster_slopes.csv` |
| Phase 06 XGB SHAP-on-mys regression on test set | **−2.00** | — | 105 cy | `outputs/tables/phase06_s07_per_cluster_slopes.csv` |

The chain is robust to BRA / ZAF / MEX / ARG boundary reassignment (Phase 05 RE Spec C **−1.15\*\*, p = 0.008**), confirming that the algorithm-induced cluster boundaries pass an explicit perturbation test rather than being artefacts of the K-means partition.

**The finding is a robust negative association, not a causal effect.** Causal interpretation is constrained by four interacting threats discussed in Finding 3.

**Anchor decisions:**
- Phase 04 cluster construction: PROJECT_LOG entry "2026-04-XX — Phase 04, Step 03" (typology design)
- Phase 05 heterogeneity analysis: PROJECT_LOG entry "Phase 05, Step 07a" (per-cluster RE Spec C)
- Phase 05 boundary perturbation: PROJECT_LOG entry "Phase 05, Step 08" (robustness suite)
- Phase 06 SHAP-on-mys per-cluster: PROJECT_LOG entry "Phase 06, Step 07" (per-cluster slopes)

**Notebook section:** `notebooks/07_synthesis_and_policy.ipynb` §1, §3.

---

## Finding 2 — Aggregate convergence across seven estimators (the methodology-comparison headline)

**Seven Phase 03–06 estimators all return negative point estimates; six of seven sit in the −0.4 to −1.4 Gini-points-per-mys-year range.**

| Phase | Estimator | mys point estimate | Identification |
|---:|---|---:|---|
| 03 | Univariate Pearson r | −0.52 | Bivariate correlation (unitless) |
| 05 | Pooled OLS Spec A | −1.328 \*\*\* | Cross-country (between) |
| 05 | FE Spec A two-way | −0.384 ns | Within-country only |
| 05 | RE Spec A | −0.688 \* | GLS-combined (θ = 0.82) |
| 06 | Ridge raw-scale coefficient | −1.42 | Mixed identification |
| 06 | RF mean signed SHAP | −1.13 | Mixed identification |
| 06 | XGB mean signed SHAP | −1.06 | Mixed identification |

The visual is `outputs/figures/phase07_s03_convergence.png` (forest plot, seven estimators on a single x-axis with phase-keyed colour and 95% CI error bars where available).

**The within-vs-between identification structure is visible in the spread.** Pooled OLS (between-country, −1.328) and the three Phase 06 mixed-identification estimators (−1.06 to −1.42) cluster together; the two-way FE estimator (within-country only, −0.384, ns) is the single statistically null point and the most demanding identification strategy. RE (−0.688\*) is the GLS combination that recovers partial significance.

**This is a methodological convergence, not an N=7 sample size.** The seven estimators are not independent: they use overlapping samples (Phase 05 / 06 share Spec A, n=1,642 country-years from 153 countries) and overlapping features. The convergence is an internal-coherence check across estimators, not a meta-analytic effect size.

**Anchor decisions:**
- Phase 05 tri-headline reporting: PROJECT_LOG entry "Phase 05, Step 07b" (adaptive override of Hausman pre-registration after Mundlak alternative-test conflict)
- Phase 06 cross-method synthesis: PROJECT_LOG entry "Phase 06, Step 07"
- Phase 07 synthesis table: PROJECT_LOG entry "Phase 07, Step 01" decision 6

**Notebook section:** `notebooks/07_synthesis_and_policy.ipynb` §1.2, §5.4.

---

## Finding 3 — The single substantive caveat (Phase 06 boundary-case holdout)

**When BRA / ZAF / MEX / ARG are removed from training and used as a 59-country-year out-of-sample evaluation set, both Random Forest and XGBoost produce test R² = −2.4, with mys mean signed SHAP flipping sign for three of four countries.**

| Country | mys SHAP (in-sample, RF) | mys SHAP (boundary-case holdout, RF) | Sign flip? |
|---|---:|---:|:---:|
| BRA | −1.78 | **+1.62** | ✓ |
| ZAF | −1.94 | **+2.46** | ✓ |
| MEX | −1.51 | **+1.89** | ✓ |
| ARG | −1.33 | −0.21 | (attenuation, not flip) |

**Interpretation.** The same trained model produces predictions for held-out boundary-case countries that are not just imprecise but systematically wrong, attributing the wrongness to mean years of schooling — the very feature the in-sample SHAP attribution had ranked first by mean |SHAP|.

This is the strongest internal evidence in the project that the in-sample Phase 06 SHAP attribution reflects **within-distribution interpolation** rather than transportable causal structure. Test R² = −2.4 (worse than predicting the training mean) under a holdout set drawn from the same panel directly demonstrates non-transportability.

**This caveat motivates the synthetic-control follow-on study** specified in `notebooks/07_synthesis_and_policy.ipynb` §7. Synthetic control is the natural identification strategy because:

1. The same four countries are the boundary cases, so the donor pool is unambiguous (~55 remaining Cluster 1 countries, excluding ARG as the fourth boundary unit)
2. Concrete treatment events exist in the mid-1990s reform window (BRA 1996 LDB / 1998 FUNDEF, MEX 1993 ANMEB, ZAF 1996 SASA)
3. Inference via in-place placebo permutation is design-internal and does not require a parametric extrapolation step
4. The Phase 06 boundary-case failure is itself the empirical justification for the design choice

**Anchor decisions:**
- Phase 06 boundary-case holdout design: PROJECT_LOG entry "Phase 06, Step 06"
- Phase 07 synthetic control specification: PROJECT_LOG entry "Phase 07, Step 01" decision 3

**Notebook section:** `notebooks/07_synthesis_and_policy.ipynb` §6.3, §7.1, §7.2.

---

## Finding 4 — Conditional policy framing, Cluster 1 only

**Policy implications are written in explicit "if … were causal, then …" conditional form, and only for Cluster 1.** Clusters 0 and 2 are deliberately excluded.

| Cluster | Phase 05 RE Spec C slope | Phase 06 RF SHAP-on-mys | Policy framing |
|---|---:|---:|---|
| 0 (Low-development / Sub-Saharan-led) | −0.80 ns | −0.08 | Excluded — no detectable association |
| **1 (Kuznets transition)** | **−1.19 \*\*** | **−1.92** | **Conditional implications written** |
| 2 (Mature economies) | −0.33 ns | −0.84 | Excluded — null in Phase 05 |

The conditional language is the only formulation consistent with Convention 6.13 (no causal claims). The implications themselves trace through education-spending allocation, secondary-tertiary balance, and the timing of compulsory-schooling reforms in middle-development countries — but every claim is hedged behind the conditional and behind the four threats articulated in Finding 3.

**Why this matters as a portfolio signal.** The standard data-science failure mode in policy-adjacent inequality work is overclaiming: a negative correlation becomes "education reduces inequality," which becomes "spend more on education." The Cluster 1 chain is one of the largest converged effects this project produced, and the policy section deliberately stops short of recommending action — because the boundary-case holdout (Finding 3) is direct internal evidence that the in-sample attribution would not transport to a counterfactual policy intervention. **Discipline at the policy step is the project's most explicit demonstration of Convention 6.13 in action.**

**Anchor decision:**
- Phase 07 policy framing scope: PROJECT_LOG entry "Phase 07, Step 01" decision 4

**Notebook section:** `notebooks/07_synthesis_and_policy.ipynb` §8.

---

## Finding 5 — Phase 04 independently re-discovers the Phase 03 Kuznets pattern

**The K=3 K-means typology of 167 countries, built on seven standardised development features without any inequality input, surfaces a Cluster 1 (middle-development) with the highest mean Gini at 39.05 — surpassing both Cluster 0 (Sub-Saharan-led, 38.24) and Cluster 2 (mature economies, 34.72).**

This is methodologically meaningful because the clustering inputs deliberately excluded Gini. The fact that the unsupervised typology surfaces an inverted-U pattern in inequality across the development gradient — without being told inequality matters — is independent corroboration of the Kuznets-curve descriptive pattern that Phase 03 finding #4 had identified by income group.

The two findings are not the same observation reported twice: Phase 03 partitions countries by World Bank income group (an external classification), while Phase 04 partitions by data-driven K-means on seven standardised development features (an internal classification). The agreement between the two partitionings on the inverted-U shape is what makes the cluster-1-as-Kuznets-transition interpretation in Findings 1 and 4 substantive rather than circular.

**Algorithm-cross-validation.** K=3 K-means versus Ward K=3 hierarchical Adjusted Rand Index = 0.65 (substantial agreement); PC1 alone captures 63.2% of variance across the seven features, indicating that development is approximately one-dimensional in this space. The clustering is not an artefact of K-means initialisation noise.

**Anchor decisions:**
- Phase 04 typology design: PROJECT_LOG entry "Phase 04, Step 03"
- Phase 04 algorithm comparison: PROJECT_LOG entry "Phase 04, Step 05"

**Notebook section:** `notebooks/04_country_clustering.ipynb` §4, `notebooks/07_synthesis_and_policy.ipynb` §3.

---

## Cross-finding decision register

The five findings above are connected through a small number of explicitly logged design decisions. The full record is in [`PROJECT_LOG.md`](../PROJECT_LOG.md); this is the entry-point map.

| Decision | Where logged | What it governs |
|---|---|---|
| Phase 05 tri-headline (override) | "Phase 05, Step 07b" | Findings 1, 2 (replaced "Hausman picks one" pre-registration after Mundlak conflict) |
| Phase 05 boundary perturbation | "Phase 05, Step 08" | Finding 1 robustness |
| Phase 06 boundary-case holdout | "Phase 06, Step 06" | Finding 3 (the central caveat) |
| Phase 06 per-cluster SHAP-on-mys | "Phase 06, Step 07" | Finding 1 cross-method corroboration |
| Phase 07 chained finding lead | "Phase 07, Step 01" decision 1 | Finding 1 narrative position |
| Phase 07 unified causal framework | "Phase 07, Step 01" decision 2 | Finding 3 framing |
| Phase 07 synthetic control depth | "Phase 07, Step 01" decision 3 | Finding 3 follow-on path |
| Phase 07 conditional policy scope | "Phase 07, Step 01" decision 4 | Finding 4 |

Each row is a place in the workflow where a substantive call was made, written into PROJECT_LOG before execution, and then either executed as-pre-registered or revised in place via the adaptive-override convention (Convention 6.3). The Phase 05 Step 07b override is the only documented adaptive override in the project — the rest of the chain executed as pre-registered.

---

## What this project does not claim

- **No causal effect of education on inequality.** Finding 3 makes this explicit. The within-country FE point estimate (−0.38, ns) is consistent with a wide range of underlying causal structures, including no causal effect at all at the country-year level of aggregation.
- **No claim about countries outside the Gini-reporting subpopulation.** The 50 listwise-excluded countries (conflict-affected states, small WB-only territories, persistent statistical-capacity cases) are systematically under-represented; the headline coefficient describes the 153-country / 1,642-country-year Gini-reporting panel.
- **No claim about within-country sub-national heterogeneity.** All Phase 05 / 06 estimates average over sub-national variation that is meaningful for inequality (Brazil, India, China, Indonesia, the United States especially).
- **No causal claim about the policy implications in Finding 4.** They are written in conditional form precisely because the project's own evidence (Finding 3) argues against a causal reading.

The synthetic-control follow-on study would address the boundary-case fragility directly. Sub-national replication and a compulsory-schooling-reform IV are the two complementary paths discussed in `notebooks/07_synthesis_and_policy.ipynb` §9.2.
