"""
Phase 05 - Step 08: Build & execute portfolio notebook.

Purpose:
    Programmatically construct `notebooks/05_econometric_modelling.ipynb`
    using nbformat, then execute it against the project's Python kernel
    via nbconvert.ExecutePreprocessor. The notebook is the single
    portfolio-facing artefact of Phase 05; the underlying analysis was
    completed in Steps 03-07 and is loaded here from CSV outputs
    rather than re-fitted (kickoff §6.4 - inherited fact discipline).

    Structure (six sections):
      1. Aggregate three-estimator reconciliation (Pooled OLS / FE / RE
         comparison via linearmodels.compare()-style table + forest plot)
      2. Hausman vs Mundlak diagnostics + Step 07b override narrative
      3. Cluster heterogeneity (Spec C interaction, per-cluster slopes
         + bar plot)
      4. Robustness checks (boundary case, sub-period, MNAR selection)
      5. Synthesis - direct answers to the kickoff §7 narrative questions
      6. Caveats and known issues (kickoff §6.8 propagation)

    Each analytical section follows the Phase 03 / 04 three-layer
    interpretation pattern: Observation -> Explanation -> Implication.

Inputs (via the executed notebook):
    data/processed/panel_modelling.csv
    outputs/tables/phase05_s03_ols_results.csv
    outputs/tables/phase05_s04_fe_results.csv
    outputs/tables/phase05_s05_re_results.csv
    outputs/tables/phase05_s05_hausman_test.csv
    outputs/tables/phase05_s06_heterogeneity_results.csv
    outputs/tables/phase05_s06_per_cluster_slopes.csv
    outputs/tables/phase05_s07_robustness_results.csv

Outputs:
    notebooks/05_econometric_modelling.ipynb
    outputs/figures/phase05_s08_forest_plot.png
    outputs/figures/phase05_s08_cluster_slopes.png
    outputs/figures/phase05_s08_mnar_contingency.png
"""

from __future__ import annotations

import random
import sys
from pathlib import Path

import numpy as np
import nbformat as nbf
from nbconvert.preprocessors import ExecutePreprocessor

SEED = 42
random.seed(SEED)
np.random.seed(SEED)

_SCRIPT_DIR = Path(__file__).resolve().parent
_PROJECT_ROOT_GUESS = _SCRIPT_DIR.parent
if str(_PROJECT_ROOT_GUESS) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT_GUESS))

from src.paths import find_project_root  # noqa: E402


# Kernel name used for in-process execution. With a conda env's
# `python` invocation, Jupyter's default `python3` kernel resolves
# to the env's Python and inherits its packages (linearmodels,
# pandas, etc.). If the env has a custom kernel registered (e.g.
# `python -m ipykernel install --user --name p4_education`), update
# this constant to that name.
KERNEL_NAME = "python3"
EXEC_TIMEOUT_SECONDS = 600


# ============================================================================
# CELL CONTENT
# ============================================================================

# Each cell is rendered as either a markdown or code cell. Code cells
# are executed in order against KERNEL_NAME. Markdown cells provide
# narrative scaffolding; analytical interpretations are integrated.

CELL_TITLE_MD = """\
# Phase 05 — Econometric Modelling

**Education and Income Inequality: A Cross-Country Panel Analysis**

This notebook is the portfolio-facing summary of Phase 05, the first
explanatory layer in the project's three-layer analytical framework
(scope §7.2). The descriptive layer (Phases 03–04) established that
education and Gini are negatively correlated cross-sectionally and
that countries cluster into three development regimes that
independently re-discover the Kuznets inverted-U. Phase 05 asks
**whether the education–inequality relationship survives panel
identification**, and if so, **whether it varies across the
development regimes**.

## Phase 05 design summary

- **Three specifications** — A (parsimonious, five RHS), B (full
  controls, ten RHS), C (heterogeneity via mys × cluster_kmeans_k3
  interaction).
- **Three estimators** — Pooled OLS, two-way Fixed Effects (country
  + year), Random Effects (with year dummies).
- **Country-clustered standard errors** throughout.
- **Hausman test** for FE vs RE; **Mundlak alternative-Hausman**
  for robustness.
- **Two diagnostic axes** — boundary-case algorithm sensitivity
  (BRA/ZAF/MEX/ARG → Cluster 1) and MNAR selection diagnostic
  (Spec A complete vs incomplete).

A late-phase adaptive override (Step 07b in PROJECT_LOG.md) replaced
the pre-registered "Hausman picks one estimator" rule with a
dual/tri-headline reporting structure — Pooled OLS, FE, and RE are
all reported in parallel because the Hausman and Mundlak tests
returned conflicting answers under cluster-robust SE.
"""


CELL_SETUP_CODE = """\
# Setup: imports, paths, and styling
from pathlib import Path
import sys

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from IPython.display import display, Markdown

# Locate project root from the notebook's own location.
def _locate_root(start: Path) -> Path:
    cur = start.resolve()
    for parent in [cur, *cur.parents]:
        if (parent / "PROJECT_LOG.md").exists():
            return parent
    raise FileNotFoundError("PROJECT_LOG.md not found above " + str(cur))

PROJECT_ROOT = _locate_root(Path.cwd())
TABLES = PROJECT_ROOT / "outputs" / "tables"
FIGURES = PROJECT_ROOT / "outputs" / "figures"
FIGURES.mkdir(parents=True, exist_ok=True)

# Plotting defaults
plt.rcParams.update({
    "figure.dpi": 110,
    "savefig.dpi": 300,
    "axes.spines.top": False,
    "axes.spines.right": False,
    "axes.titlesize": 12,
    "axes.labelsize": 11,
    "xtick.labelsize": 10,
    "ytick.labelsize": 10,
    "legend.fontsize": 10,
    "figure.titlesize": 13,
    "font.family": "DejaVu Sans",
})

print(f"Project root: {PROJECT_ROOT}")
print(f"Tables dir:   {TABLES}")
print(f"Figures dir:  {FIGURES}")
"""


CELL_HELPERS_CODE = """\
# Helper functions for loading CSV outputs from Phase 05 step scripts.
def load_long(filename: str) -> pd.DataFrame:
    return pd.read_csv(TABLES / filename)

def filter_coef(df: pd.DataFrame, spec: str | None = None,
                variable: str | None = None) -> pd.DataFrame:
    out = df.loc[df["kind"] == "coef"].copy() if "kind" in df.columns else df.copy()
    if spec is not None and "spec" in out.columns:
        out = out.loc[out["spec"] == spec]
    if variable is not None and "variable" in out.columns:
        out = out.loc[out["variable"] == variable]
    return out

def fit_value(df: pd.DataFrame, spec: str, name: str) -> float:
    row = df.loc[(df.get("kind") == "fit") & (df.get("spec") == spec)
                 & (df.get("variable") == name)]
    return float(row["value"].iloc[0]) if not row.empty else float("nan")

def stars(p):
    if not np.isfinite(p):
        return ""
    if p < 0.001: return "***"
    if p < 0.01:  return "**"
    if p < 0.05:  return "*"
    if p < 0.10:  return "."
    return ""

def fmt_coef(coef, se, p):
    return f"{coef:+.3f}{stars(p)} ({se:.3f})"

print("Helpers ready.")
"""


# ----------------------------------------------------------------------------
# Section 1: Aggregate three-estimator reconciliation
# ----------------------------------------------------------------------------

CELL_S1_INTRO_MD = """\
## 1. Aggregate education–inequality relationship

The first task is to compare three estimators of the same model. Each
makes different assumptions about how the unobserved country-specific
component enters the data:

- **Pooled OLS** treats the panel as one stack of independent
  observations. The coefficient on `mean_years_schooling` reflects
  how Gini differs between countries with different education levels.
- **Fixed Effects (country + year)** absorbs all time-invariant
  country characteristics and common year shocks. The coefficient
  reflects how Gini changes within a country as its education level
  changes, after netting out global trends.
- **Random Effects** combines the two via GLS. Under the assumption
  that country-specific effects are uncorrelated with the regressors,
  RE is consistent and more efficient than FE.

We report all three in parallel for **Spec A** (parsimonious, five
RHS) and **Spec B** (full controls, ten RHS).
"""


CELL_S1_TABLE_CODE = """\
# Build the headline three-estimator coefficient table
ols = load_long("phase05_s03_ols_results.csv")
fe = load_long("phase05_s04_fe_results.csv")
re = load_long("phase05_s05_re_results.csv")

def build_estimator_table(spec: str) -> pd.DataFrame:
    rows = []
    # OLS rows
    o = filter_coef(ols, spec=spec)
    o = o.loc[o["variable"] != "Intercept"]
    # FE rows  
    f = filter_coef(fe, spec=spec)
    f = f.loc[(f["variable"] != "Intercept")
             & ~f["variable"].str.startswith("C(year")]
    # RE rows
    r = filter_coef(re, spec=spec)
    r = r.loc[r["variable"] != "Intercept"]

    variables = list(o["variable"])
    for var in variables:
        o_row = o.loc[o["variable"] == var]
        f_row = f.loc[f["variable"] == var]
        r_row = r.loc[r["variable"] == var]
        rows.append({
            "Variable": var,
            "Pooled OLS": fmt_coef(
                o_row["value"].iloc[0],
                o_row["std_error"].iloc[0],
                o_row["pvalue"].iloc[0],
            ) if not o_row.empty else "—",
            "FE (two-way)": fmt_coef(
                f_row["value"].iloc[0],
                f_row["std_error"].iloc[0],
                f_row["pvalue"].iloc[0],
            ) if not f_row.empty else "—",
            "RE": fmt_coef(
                r_row["value"].iloc[0],
                r_row["std_error"].iloc[0],
                r_row["pvalue"].iloc[0],
            ) if not r_row.empty else "—",
        })
    # Append fit-stat rows
    for nm, label in [("n_obs", "N (country-years)"),
                      ("n_countries", "N (countries)"),
                      ("rsquared_within", "R² within"),
                      ("rsquared", "R²")]:
        ov = fit_value(ols, spec, nm)
        fv = fit_value(fe, spec, nm)
        rv = fit_value(re, spec, nm)
        if nm in ("n_obs", "n_countries"):
            fmt = lambda v: f"{int(v):,}" if np.isfinite(v) else "—"
        else:
            fmt = lambda v: f"{v:+.4f}" if np.isfinite(v) else "—"
        rows.append({
            "Variable": label,
            "Pooled OLS": fmt(ov),
            "FE (two-way)": fmt(fv),
            "RE": fmt(rv),
        })
    return pd.DataFrame(rows)

display(Markdown("### Spec A — parsimonious (five RHS controls)"))
display(build_estimator_table("A"))
display(Markdown("Format: coefficient (clustered SE). Stars: *** p<0.001, ** p<0.01, * p<0.05, . p<0.10."))
"""


CELL_S1_TABLE_B_CODE = """\
display(Markdown("### Spec B — full controls (ten RHS, adds sector trio + trade + gov)"))
display(build_estimator_table("B"))
"""


CELL_S1_FOREST_CODE = """\
# Forest plot: mean_years_schooling coefficient across estimators × specs
HEADLINE = "mean_years_schooling"

def headline_row(df, spec):
    r = filter_coef(df, spec=spec, variable=HEADLINE)
    if r.empty:
        return None
    return {
        "coef": float(r["value"].iloc[0]),
        "ci_low": float(r["ci_low"].iloc[0]),
        "ci_high": float(r["ci_high"].iloc[0]),
        "p": float(r["pvalue"].iloc[0]),
    }

records = []
for spec in ["A", "B"]:
    for estimator, df in [("Pooled OLS", ols), ("FE (two-way)", fe), ("RE", re)]:
        h = headline_row(df, spec)
        if h is not None:
            records.append({
                "label": f"Spec {spec} · {estimator}",
                "spec": spec,
                **h,
            })

forest_df = pd.DataFrame(records)
forest_df = forest_df.iloc[::-1].reset_index(drop=True)  # display top-to-bottom

fig, ax = plt.subplots(figsize=(8.0, 4.5))
y = np.arange(len(forest_df))
colors = ["#3b6cb7" if s == "A" else "#c44d2e" for s in forest_df["spec"]]
ax.errorbar(
    forest_df["coef"], y,
    xerr=[forest_df["coef"] - forest_df["ci_low"],
          forest_df["ci_high"] - forest_df["coef"]],
    fmt="o", color="black", ecolor="black",
    elinewidth=1.4, capsize=4, markersize=7,
    markerfacecolor="white", markeredgewidth=1.4,
)
for yi, color in zip(y, colors):
    ax.scatter(forest_df["coef"].iloc[yi], yi, s=70,
               facecolor=color, edgecolor="black", linewidth=1.0, zorder=5)

ax.axvline(0, color="#888", linestyle="--", linewidth=1, zorder=0)
ax.set_yticks(y)
ax.set_yticklabels(forest_df["label"])
ax.set_xlabel("Coefficient on mean_years_schooling (Gini points per year)")
ax.set_title("Headline coefficient under three estimators (95% CI)")
ax.grid(axis="x", linestyle=":", linewidth=0.6, alpha=0.7)

plt.tight_layout()
fig_path = FIGURES / "phase05_s08_forest_plot.png"
plt.savefig(fig_path, dpi=300, bbox_inches="tight")
plt.show()
print(f"Saved {fig_path.name}")
"""


CELL_S1_INTERPRETATION_MD = """\
**Observation.** The Pooled OLS coefficient on `mean_years_schooling`
is large and significantly negative (Spec A: −1.328, p<0.001;
Spec B: −1.204, p<0.001). Under FE the coefficient attenuates by
roughly 70% and loses statistical significance (Spec A: −0.384,
p=0.366). Under RE, the coefficient sits between the two
(Spec A: −0.688, p=0.016) and remains significant.

**Explanation.** The three estimators identify the relationship from
different sources of variation in the panel. Pooled OLS exploits
the cross-country pattern: countries with more education on average
also have lower Gini on average. FE absorbs all time-invariant
country characteristics, so its coefficient is identified purely
from within-country variation: do the same country's Gini
measurements move when its education level moves? The answer is
"weakly, on average". RE pools both sources via GLS with theta=0.82
(strongly weighted toward FE) but recovers a moderate negative
relationship by allowing the small remaining between-country
information to contribute.

**Implication.** The strong negative association established
descriptively in Phase 03 is largely a between-country
phenomenon — in Phase 04's terms, it reflects the shared
development gradient (PC1, 63.2% of variance) on which education
and inequality both load. Within a single country across time, the
short-run relationship is weak. Both findings are real; they
answer different questions. The Phase 05 reconciliation story is
how these three numbers fit together.
"""


# ----------------------------------------------------------------------------
# Section 2: Hausman vs Mundlak — why three estimators rather than one
# ----------------------------------------------------------------------------

CELL_S2_INTRO_MD = """\
## 2. Why three estimators? — Hausman and Mundlak diagnostics

Step 01 Decision 3 pre-registered the Hausman test as the deciding
diagnostic between FE and RE. Step 05 ran the Hausman tests for
Spec A and Spec B; one returned an interpretable result, the other
returned a numerical degeneracy known to occur for the Hausman test
under cluster-robust SE in finite samples. Step 07 added a Mundlak
alternative-Hausman test (Wald statistic on country-mean
coefficients in an RE specification), which is asymptotically
equivalent to Hausman but stable under cluster-robust SE.

The two tests gave **conflicting answers** on Spec B and **weakly
disagreed** on Spec A. Phase 05's audit trail (PROJECT_LOG.md
Step 07b) records the resulting adaptive override: report
Pooled OLS / FE / RE in parallel rather than selecting a single
estimator from a contradictory diagnostic set.
"""


CELL_S2_TABLE_CODE = """\
# Diagnostic comparison table
hausman = load_long("phase05_s05_hausman_test.csv")
robust = load_long("phase05_s07_robustness_results.csv")
mundlak = robust.loc[robust["check"] == "mundlak"].copy()

rows = []
for spec in ["A", "B"]:
    h = hausman.loc[hausman["spec"] == spec]
    m = mundlak.loc[mundlak["subgroup"] == f"Spec_{spec}"]
    h_stat = float(h["statistic"].iloc[0]) if not h.empty else float("nan")
    h_p = float(h["pvalue"].iloc[0]) if not h.empty else float("nan")
    h_concl = h["conclusion"].iloc[0] if not h.empty else "—"
    m_stat = float(m["statistic"].iloc[0]) if not m.empty else float("nan")
    m_p = float(m["pvalue"].iloc[0]) if not m.empty else float("nan")
    m_concl = m["notes"].iloc[0].split(";")[-1].strip() if not m.empty else "—"
    rows.append({
        "Spec": spec,
        "Hausman χ²": f"{h_stat:+.3f}" if np.isfinite(h_stat) else "—",
        "Hausman p": f"{h_p:.4f}" if np.isfinite(h_p) else "—",
        "Hausman conclusion": h_concl,
        "Mundlak Wald": f"{m_stat:+.3f}" if np.isfinite(m_stat) else "—",
        "Mundlak p": f"{m_p:.4f}" if np.isfinite(m_p) else "—",
        "Mundlak conclusion": m_concl,
    })

display(Markdown("### Hausman vs Mundlak (FE vs RE diagnostics)"))
display(pd.DataFrame(rows))
"""


CELL_S2_INTERPRETATION_MD = """\
**Observation.** For Spec A, Hausman fails to reject H0 at p=0.40
(supports RE), but Mundlak rejects at the 10% level (p=0.080,
points toward FE). For Spec B, Hausman returns a negative test
statistic — a finite-sample failure under cluster-robust SE that
carries no information — while Mundlak rejects strongly at
p<0.0001 (points toward FE).

**Explanation.** The Hausman test relies on the inversion of
Σ_FE − Σ_RE, which is positive semi-definite under H0 in
large samples but often slightly indefinite under cluster-robust
SE in samples of moderate cluster count (~140 countries here). The
Mundlak formulation reframes the same null hypothesis as "country
means of the regressors have zero coefficients in an RE
specification" and tests via Wald, which is numerically stable.
The Spec B Mundlak rejection at p<0.0001 specifically identifies
that country-specific unobservables are correlated with the
regressors when sector composition, trade openness, and
government expenditure enter the model.

**Implication.** No single estimator is unambiguously preferred.
The conservative analytical position is to report all three with
explicit diagnostic context, which is what this notebook does.
The Cluster 1 heterogeneity finding (Section 3 below) is itself
robust across estimators and stands independently of this
diagnostic conflict.
"""


# ----------------------------------------------------------------------------
# Section 3: Heterogeneity — Spec C
# ----------------------------------------------------------------------------

CELL_S3_INTRO_MD = """\
## 3. Heterogeneity by development cluster (Spec C)

Phase 04 partitioned 167 countries into three K-means clusters
(K=3) using only education and economic indicators (Gini was not an
input). The clustering re-discovered the Phase 03 Kuznets pattern:
mean Gini was highest in Cluster 1 (39.05, "middle-development /
Kuznets transition"), followed by Cluster 0 (38.24,
"low-development") and Cluster 2 (34.72, "high-development /
mature economies").

Phase 05 Spec C tests whether the within-country slope of
`mean_years_schooling` on Gini differs across these clusters. The
specification is

```
gini ~ 1 + mean_years_schooling × C(cluster_kmeans_k3)
       + enrol_secondary + log_gdp_per_capita_ppp
       + log_population + urban_population_pct
       + (FE: EntityEffects + TimeEffects |
          RE: C(year_factor))
```

The cluster main-effect dummies are time-invariant (one cluster
per country) and so are absorbed by EntityEffects under FE; the
interaction terms remain identified. Per-cluster slopes are
linear combinations of the fitted coefficients with delta-method
standard errors:

- **Cluster 0**: slope = β_mys
- **Cluster 1**: slope = β_mys + β_{mys×Cluster1}
- **Cluster 2**: slope = β_mys + β_{mys×Cluster2}
"""


CELL_S3_TABLE_CODE = """\
slopes = load_long("phase05_s06_per_cluster_slopes.csv")

def slope_table(estimator: str) -> pd.DataFrame:
    df = slopes.loc[slopes["estimator"] == estimator].sort_values("cluster")
    return pd.DataFrame([
        {
            "Cluster": int(r["cluster"]),
            "Slope": f"{r['slope']:+.3f}{stars(r['pvalue'])}",
            "SE": f"{r['std_error']:.3f}",
            "p-value": f"{r['pvalue']:.4f}",
            "95% CI": f"[{r['ci_low']:+.3f}, {r['ci_high']:+.3f}]",
        }
        for _, r in df.iterrows()
    ])

display(Markdown("### FE Spec C — per-cluster within-country slope of mys on Gini"))
display(slope_table("FE"))
display(Markdown("### RE Spec C — per-cluster within-country slope of mys on Gini"))
display(slope_table("RE"))
"""


CELL_S3_BAR_CODE = """\
# Per-cluster slope bar plot
fig, ax = plt.subplots(figsize=(8.0, 4.5))

clusters = [0, 1, 2]
labels = ["Cluster 0\\n(low-development)", "Cluster 1\\n(middle / transition)",
          "Cluster 2\\n(mature economies)"]
x = np.arange(len(clusters))
width = 0.35

fe_data = slopes.loc[slopes["estimator"] == "FE"].sort_values("cluster")
re_data = slopes.loc[slopes["estimator"] == "RE"].sort_values("cluster")

fe_err = [fe_data["slope"] - fe_data["ci_low"],
          fe_data["ci_high"] - fe_data["slope"]]
re_err = [re_data["slope"] - re_data["ci_low"],
          re_data["ci_high"] - re_data["slope"]]

bars_fe = ax.bar(x - width/2, fe_data["slope"], width,
                 yerr=fe_err, label="FE", color="#9bb7d4",
                 edgecolor="black", linewidth=0.8,
                 error_kw=dict(ecolor="black", capsize=4, lw=1.2))
bars_re = ax.bar(x + width/2, re_data["slope"], width,
                 yerr=re_err, label="RE", color="#3b6cb7",
                 edgecolor="black", linewidth=0.8,
                 error_kw=dict(ecolor="black", capsize=4, lw=1.2))

# Highlight Cluster 1 (the significant heterogeneity finding) with
# subtle background shading
ax.axvspan(0.5, 1.5, color="#fff4cc", alpha=0.5, zorder=0)

ax.axhline(0, color="#888", linewidth=1)
ax.set_xticks(x)
ax.set_xticklabels(labels)
ax.set_ylabel("Within-country slope of mys on Gini (Gini points / year)")
ax.set_title("Per-cluster heterogeneity in the education–Gini slope (95% CI)")
ax.legend(loc="lower right")
ax.grid(axis="y", linestyle=":", linewidth=0.6, alpha=0.7)

# Annotate Cluster 1 RE finding
re_c1 = re_data.loc[re_data["cluster"] == 1].iloc[0]
ax.annotate(
    f"−1.19 (p=0.010)\\nKuznets transition",
    xy=(1 + width/2, re_c1["slope"]),
    xytext=(1 + width/2 + 0.2, re_c1["slope"] - 0.4),
    fontsize=9, ha="left",
    arrowprops=dict(arrowstyle="-", color="black", linewidth=0.8),
)

plt.tight_layout()
fig_path = FIGURES / "phase05_s08_cluster_slopes.png"
plt.savefig(fig_path, dpi=300, bbox_inches="tight")
plt.show()
print(f"Saved {fig_path.name}")
"""


CELL_S3_INTERPRETATION_MD = """\
**Observation.** Under RE, the within-country slope of mys on Gini
is statistically significant only in Cluster 1 (slope = −1.19,
p = 0.010). Cluster 0 and Cluster 2 slopes are not statistically
distinguishable from zero (p = 0.13 and p = 0.42 respectively). The
FE estimator shows the same direction (Cluster 1 slope = −0.84) but
with wider standard errors that prevent statistical significance.

**Explanation.** Cluster 1 is the "middle-development / Kuznets
transition" group identified in Phase 04 — countries with mean
years of schooling around 8.85 and per-capita GDP around \\$11,700
PPP. These are exactly the economies in which (i) education is
still expanding rapidly enough to generate substantial within-
country variation, and (ii) inequality is at its peak in the
Kuznets pattern, leaving room for educational expansion to be
equalising. Cluster 0 (low-development, mys ≈ 4.2) has
insufficient variation to identify the slope precisely; Cluster 2
(mature economies, mys ≈ 11.4) has near-saturated education with
diminishing returns to further expansion in inequality terms.

**Implication.** The aggregate "weak within-country effect" finding
of Section 1 hides systematic heterogeneity. Education does reduce
inequality within countries — but mainly during the development
phase where the Kuznets curve peaks. This finding is the principal
quantitative contribution of Phase 05's explanatory layer beyond
the descriptive Phase 03/04 anchors.
"""


# ----------------------------------------------------------------------------
# Section 4: Robustness
# ----------------------------------------------------------------------------

CELL_S4_INTRO_MD = """\
## 4. Robustness checks

Three checks were pre-registered (Step 01 Decisions 5–7) plus a
Mundlak alternative-Hausman test added in Step 07. The Mundlak
results were already presented in Section 2; the remaining three
are summarised here.
"""


CELL_S4_BOUNDARY_CODE = """\
# Boundary-case reassignment: BRA, ZAF, MEX, ARG → Cluster 1
boundary = robust.loc[(robust["check"] == "boundary_case")
                      & (robust["item_kind"] == "slope")].copy()

def boundary_table() -> pd.DataFrame:
    rows = []
    for cid in [0, 1, 2]:
        b = boundary.loc[(boundary["subgroup"] == "baseline")
                         & (boundary["item_name"] == f"cluster_{cid}")]
        r = boundary.loc[(boundary["subgroup"] == "reassigned")
                         & (boundary["item_name"] == f"cluster_{cid}")]
        rows.append({
            "Cluster": cid,
            "Baseline slope": f"{float(b['value'].iloc[0]):+.3f}{stars(float(b['pvalue'].iloc[0]))}",
            "Baseline 95% CI": f"[{float(b['ci_low'].iloc[0]):+.3f}, {float(b['ci_high'].iloc[0]):+.3f}]",
            "Reassigned slope": f"{float(r['value'].iloc[0]):+.3f}{stars(float(r['pvalue'].iloc[0]))}",
            "Reassigned 95% CI": f"[{float(r['ci_low'].iloc[0]):+.3f}, {float(r['ci_high'].iloc[0]):+.3f}]",
            "Δ slope": f"{float(r['value'].iloc[0]) - float(b['value'].iloc[0]):+.3f}",
        })
    return pd.DataFrame(rows)

display(Markdown("### Robustness 1 — Boundary case reassignment (RE Spec C)"))
display(Markdown("BRA, ZAF, MEX, ARG identified as K-means/Ward boundary "
                 "cases in Phase 04. Reassigned from Cluster 2 to Cluster 1."))
display(boundary_table())
"""


CELL_S4_SUBPERIOD_CODE = """\
# Sub-period 2010–2019: Spec A FE + RE
sub = robust.loc[robust["check"] == "subperiod_2010_2019"].copy()

# Full-panel headline for comparison
fe_full = filter_coef(fe, spec="A", variable=HEADLINE).iloc[0]
re_full = filter_coef(re, spec="A", variable=HEADLINE).iloc[0]

rows = []
for label, coef, se, p, ci_low, ci_high, n_obs in [
    ("Full panel · FE", fe_full["value"], fe_full["std_error"],
     fe_full["pvalue"], fe_full["ci_low"], fe_full["ci_high"],
     fit_value(fe, "A", "n_obs")),
    ("Full panel · RE", re_full["value"], re_full["std_error"],
     re_full["pvalue"], re_full["ci_low"], re_full["ci_high"],
     fit_value(re, "A", "n_obs")),
]:
    rows.append({
        "Sample · Estimator": label,
        "mys coefficient": f"{coef:+.3f}{stars(p)}",
        "SE": f"{se:.3f}",
        "p-value": f"{p:.4f}",
        "95% CI": f"[{ci_low:+.3f}, {ci_high:+.3f}]",
        "N (cy)": f"{int(n_obs):,}",
    })
for _, r in sub.iterrows():
    label = "Sub-period 2010-19 · " + r["subgroup"].split("_")[-1]
    rows.append({
        "Sample · Estimator": label,
        "mys coefficient": f"{float(r['value']):+.3f}{stars(float(r['pvalue']))}",
        "SE": f"{float(r['std_error']):.3f}",
        "p-value": f"{float(r['pvalue']):.4f}",
        "95% CI": f"[{float(r['ci_low']):+.3f}, {float(r['ci_high']):+.3f}]",
        "N (cy)": f"{int(r['n_obs']):,}",
    })

display(Markdown("### Robustness 2 — Sub-period 2010–2019 (Spec A)"))
display(pd.DataFrame(rows))
"""


CELL_S4_MNAR_CODE = """\
# MNAR selection diagnostic: who is in the gini-using sample?
mnar_cont = robust.loc[(robust["check"] == "mnar_selection")
                       & (robust["subgroup"] == "continuous_cy")].copy()
mnar_cat = robust.loc[(robust["check"] == "mnar_selection")
                      & (robust["subgroup"] == "categorical_country")].copy()

cont_rows = []
for var in ["mean_years_schooling", "log_gdp_per_capita_ppp",
            "urban_population_pct"]:
    welch = mnar_cont.loc[(mnar_cont["item_kind"] == "welch_t")
                          & (mnar_cont["item_name"] == var)]
    ks = mnar_cont.loc[(mnar_cont["item_kind"] == "ks_test")
                       & (mnar_cont["item_name"] == var)]
    if welch.empty or ks.empty:
        continue
    cont_rows.append({
        "Variable": var,
        "diff (A − B)": f"{float(welch['value'].iloc[0]):+.3f}",
        "Welch t": f"{float(welch['statistic'].iloc[0]):+.2f}",
        "Welch p": f"{float(welch['pvalue'].iloc[0]):.2e}",
        "KS D": f"{float(ks['statistic'].iloc[0]):.4f}",
        "KS p": f"{float(ks['pvalue'].iloc[0]):.2e}",
    })

cat_rows = []
for _, r in mnar_cat.iterrows():
    cat_rows.append({
        "Variable": r["item_name"],
        "χ²": f"{float(r['statistic']):.3f}",
        "df": int(r["df"]),
        "p-value": f"{float(r['pvalue']):.4f}",
        "n_A countries": int(r["n_obs"]),
        "n_B countries": int(r["n_countries"]),
    })

display(Markdown("### Robustness 3 — MNAR selection diagnostic"))
display(Markdown("**Continuous variables (country-year level):** Group A = "
                 "Spec A listwise complete (1,642 cy). Group B = "
                 "incomplete (5,736 cy)."))
display(pd.DataFrame(cont_rows))
display(Markdown("**Categorical variables (country level):** A country is "
                 "in Group A if at least one of its country-years is "
                 "Spec A complete (153 of 217). Group B are countries "
                 "with zero complete years (64 of 217)."))
display(pd.DataFrame(cat_rows))
"""


CELL_S4_MNAR_PLOT_CODE = """\
# Stacked-bar visualisation of income-level distribution by group
panel = pd.read_csv(PROJECT_ROOT / "data" / "processed" / "panel_modelling.csv")
spec_a_cols = ["gini", "mean_years_schooling", "enrol_secondary",
               "log_gdp_per_capita_ppp", "log_population",
               "urban_population_pct"]
is_complete = panel[spec_a_cols].notna().all(axis=1)
country_a = set(panel.loc[is_complete, "iso3"].unique())
panel_iso = set(panel["iso3"].unique())
country_b = panel_iso - country_a

# Country-level table; income_level_name from panel side
country_table = (panel.drop_duplicates("iso3")
                      .reset_index(drop=True)
                      .copy())
country_table["income_level_name"] = (
    country_table["income_level_name"].astype(str).str.strip()
)
country_table["mnar_group"] = np.where(
    country_table["iso3"].isin(country_a), "A (gini-using)", "B (excluded)"
)
ct = pd.crosstab(country_table["income_level_name"],
                 country_table["mnar_group"])

# Order: low → lower middle → upper middle → high → not classified
order = ["Low income", "Lower middle income", "Upper middle income",
         "High income", "Not classified"]
ct = ct.reindex([o for o in order if o in ct.index])

# Convert to share within group
ct_share = ct.div(ct.sum(axis=0), axis=1) * 100

fig, ax = plt.subplots(figsize=(8.0, 4.5))
groups = ct_share.columns.tolist()
x = np.arange(len(groups))
bottom = np.zeros(len(groups))
colors = ["#3b6cb7", "#7a9ec4", "#c8a465", "#c44d2e", "#999999"]
for i, level in enumerate(ct_share.index):
    vals = ct_share.loc[level].values
    ax.bar(x, vals, bottom=bottom, label=level,
           color=colors[i % len(colors)],
           edgecolor="white", linewidth=1.2)
    # Annotate raw counts inside the bars
    for xi, val, bot in zip(x, vals, bottom):
        if val > 4:
            count = int(ct.loc[level].iloc[xi])
            ax.text(xi, bot + val/2, f"{level}\\n(n={count})",
                    ha="center", va="center", fontsize=8, color="white",
                    fontweight="semibold")
    bottom += vals

ax.set_xticks(x)
ax.set_xticklabels(groups)
ax.set_ylabel("Share of countries in group (%)")
ax.set_title("Income-level distribution by MNAR group "
             "(country level, n=217)")
ax.set_ylim(0, 100)
ax.grid(axis="y", linestyle=":", linewidth=0.6, alpha=0.7)

plt.tight_layout()
fig_path = FIGURES / "phase05_s08_mnar_contingency.png"
plt.savefig(fig_path, dpi=300, bbox_inches="tight")
plt.show()
print(f"Saved {fig_path.name}")
"""


CELL_S4_INTERPRETATION_MD = """\
**Observation.** (1) The Cluster 1 RE slope shifts from −1.19
(p=0.010) to −1.15 (p=0.008) when BRA/ZAF/MEX/ARG are reassigned
to Cluster 1; the heterogeneity finding is essentially unaffected
by this algorithm-induced uncertainty. Cluster 2's slope flips
sign (−0.33 → +0.20), which is intuitive: removing four upper-
middle-income economies from Cluster 2 leaves a more homogeneous
high-income group with negligible education-Gini dynamics.
(2) The full-panel RE coefficient on mys (−0.69, p=0.016)
matches the sub-period 2010–2019 RE coefficient (−0.74, p=0.014)
to within a sampling error. (3) The gini-using sample is
significantly more educated, richer, and more urbanised than the
excluded sample, with effect sizes of roughly +2.7 years of
schooling and +0.7 log-units of GDP per capita. The country-level
income distribution is unequal across groups (chi² p=0.0017): high-
income countries are over-represented in the excluded sample
relative to lower-middle and upper-middle income countries.

**Explanation.** The Cluster 1 finding is genuinely a property of
the middle-development data, not an artefact of how the cluster
boundary was drawn. The sub-period stability indicates that the
education–Gini relationship is not concentrated in a particular
decade. The MNAR finding is interesting in its non-obvious
direction: the excluded sample is not the global poor; it is a
mix of low-income countries (where Gini surveys are scarce) and
high-income microstates / oil exporters (where the World Bank
sample composition reflects each country's own statistical agency
choices). This means the headline coefficient should be read as
applying to "countries with sufficient survey infrastructure" —
predominantly middle-income countries — rather than to all
countries.

**Implication.** The Cluster 1 heterogeneity finding is the most
robust quantitative claim in Phase 05. The aggregate three-
estimator reconciliation is sample-consistent across the 2010s
sub-period. Both findings should be qualified by the MNAR
diagnostic: results most directly describe middle-income economies
with sustained Gini reporting.
"""


# ----------------------------------------------------------------------------
# Section 5: Synthesis
# ----------------------------------------------------------------------------

CELL_S5_MD = """\
## 5. Synthesis — answers to the kickoff narrative questions

The Phase 05 kickoff posed two narrative questions
(kickoff §7). The answers below synthesise Sections 1–4.

### *Does the education–inequality relationship survive panel methods?*

**Yes, but with substantial qualification.** The headline number
splits across three estimators:

- **Pooled OLS (between-country identification):** −1.33 Gini
  points per year of schooling, highly significant. This is the
  cross-sectional pattern that motivates the question.
- **Fixed Effects (within-country identification):** −0.38 Gini
  points per year of schooling, statistically null. Within a
  country across years, education expansion does not robustly
  move the Gini.
- **Random Effects (GLS combination):** −0.69 Gini points per
  year, p=0.016. Interpolated estimate that recovers some of the
  between-country signal while controlling for unobserved country
  characteristics.

The Mundlak diagnostic suggests that the RE estimator's identifying
assumption (cov(α_i, X_it) = 0) is empirically suspect for the
rich-controls specification (Spec B Mundlak p<0.0001). The honest
analytical position is that the data identify the relationship at
the between-country level much more sharply than at the within-
country level, and any single point estimate masks this
asymmetry.

### *Does it hold across the Phase 04 cluster typology?*

**No, the relationship is concentrated in middle-development
countries.** Per the RE Spec C interaction model:

- **Cluster 0 (low-development, mys ≈ 4.2):** slope = −0.80, p=0.13.
  Borderline negative; insufficient education variation to identify
  precisely.
- **Cluster 1 (middle-development / Kuznets transition,
  mys ≈ 8.85):** slope = **−1.19, p=0.010**. The only statistically
  significant cluster-specific slope; education expansion is
  associated with a roughly 1.2-point Gini reduction per year of
  schooling within these countries.
- **Cluster 2 (mature economies, mys ≈ 11.4):** slope = −0.33,
  p=0.42. Statistically null; education near-saturated.

This finding is the principal quantitative result of Phase 05's
explanatory layer. It supplies a within-country corroboration of
the Phase 04 descriptive Kuznets pattern: the same middle-
development cluster that has the highest mean Gini (39.05) is
also the only cluster where education expansion is detectably
associated with falling inequality.

### What this means for the project as a whole

Phase 05 closes the explanatory layer of the three-layer
analytical framework. The descriptive layer (Phases 03–04)
established the structure; the explanatory layer (Phase 05)
identified that the structure is real within the cluster of
middle-development countries but weak elsewhere. The remaining
question — **whether this within-cluster relationship is causal**
— is the territory of Phase 06 (causal inference, Layer C),
which will require either an instrumental-variable strategy, a
natural-experiment design (e.g. a compulsory schooling reform),
or a difference-in-differences design.
"""


# ----------------------------------------------------------------------------
# Section 6: Caveats
# ----------------------------------------------------------------------------

CELL_S6_MD = """\
## 6. Caveats and known issues

Carried forward from prior phases (kickoff §6.8 caveat propagation):

- **MNAR / 50-country exclusion.** Of 217 countries in the panel,
  64 contribute zero observations to the Spec A listwise sample.
  These are concentrated among small high-income microstates,
  oil-exporting economies, and conflict-affected states. Section 4
  documents the resulting selection: the gini-using sample is
  systematically richer and more educated. Headline results apply
  most directly to middle-income economies with sustained survey
  reporting.

- **Cross-country Gini heterogeneity.** The World Bank PIP database
  mixes consumption-based and income-based household surveys
  inconsistently across countries. Two countries with the same
  reported Gini may not be measuring the same quantity. The
  cluster-level results (Section 3) are partially insulated from
  this concern because they identify within-country variation, but
  cross-cluster comparisons should be read with this caveat in
  mind.

- **Country-level aggregation flattens within-country
  heterogeneity.** India, Brazil, and China are particularly
  affected: each contains regional or provincial economies as
  diverse as the cross-country panel itself. A single Gini value
  per year for a country with 1.4 billion people in different
  development stages is a substantial abstraction.

- **2010s sub-period for China specifically.** China's structural
  transition in the 2010s (urbanisation, hukou reform, rapid
  middle-class growth) means that within-country variation
  averaged over 2010–2019 smears across regimes. The sub-period
  robustness check in Section 4 partially addresses this for the
  full panel but does not isolate country-specific transitions.

- **Education quality vs quantity.** `mean_years_schooling`
  measures the quantity of schooling. Two country-years with the
  same mean years can have very different educational quality and
  labour-market relevance. Phase 06 may be able to address this
  with PISA / TIMMS test-score panels for the subset of countries
  covered.

- **Reverse causality.** The interpretation that education reduces
  inequality assumes the direction of causation runs from schooling
  to Gini. Education expansion is itself shaped by inequality
  (resource distribution, political economy of public spending),
  so the within-country slopes reported here should be read as
  associations, not causal effects, until Phase 06 introduces an
  identification strategy.

A complete inventory of inherited caveats is in
`docs/phase_summaries/phase04_summary.md` §Known Issues. New
Phase 05–specific issues will be recorded in
`docs/phase_summaries/phase05_summary.md` (Step 09).

---

*Phase 05 Econometric Modelling — generated programmatically via
`scripts/phase05_s08_build_notebook.py`. Coefficient anchors loaded
from Steps 03–07 output CSVs. Three figures saved to
`outputs/figures/`. PROJECT_LOG.md entries: Phase 05 Step 01
(design decisions), Step 07b (adaptive override).*
"""


# ============================================================================
# CELL ASSEMBLY
# ============================================================================

CELLS: list[tuple[str, str]] = [
    # Title and setup
    ("md", CELL_TITLE_MD),
    ("code", CELL_SETUP_CODE),
    ("code", CELL_HELPERS_CODE),
    # Section 1
    ("md", CELL_S1_INTRO_MD),
    ("code", CELL_S1_TABLE_CODE),
    ("code", CELL_S1_TABLE_B_CODE),
    ("code", CELL_S1_FOREST_CODE),
    ("md", CELL_S1_INTERPRETATION_MD),
    # Section 2
    ("md", CELL_S2_INTRO_MD),
    ("code", CELL_S2_TABLE_CODE),
    ("md", CELL_S2_INTERPRETATION_MD),
    # Section 3
    ("md", CELL_S3_INTRO_MD),
    ("code", CELL_S3_TABLE_CODE),
    ("code", CELL_S3_BAR_CODE),
    ("md", CELL_S3_INTERPRETATION_MD),
    # Section 4
    ("md", CELL_S4_INTRO_MD),
    ("code", CELL_S4_BOUNDARY_CODE),
    ("code", CELL_S4_SUBPERIOD_CODE),
    ("code", CELL_S4_MNAR_CODE),
    ("code", CELL_S4_MNAR_PLOT_CODE),
    ("md", CELL_S4_INTERPRETATION_MD),
    # Section 5
    ("md", CELL_S5_MD),
    # Section 6
    ("md", CELL_S6_MD),
]


# ============================================================================
# BUILD + EXECUTE + SAVE
# ============================================================================


def build_notebook() -> nbf.NotebookNode:
    nb = nbf.v4.new_notebook()
    cells: list = []
    for kind, content in CELLS:
        if kind == "md":
            cells.append(nbf.v4.new_markdown_cell(content))
        elif kind == "code":
            cells.append(nbf.v4.new_code_cell(content))
        else:
            raise ValueError(f"Unknown cell kind: {kind!r}")
    nb.cells = cells
    nb.metadata["kernelspec"] = {
        "display_name": "Python 3",
        "language": "python",
        "name": KERNEL_NAME,
    }
    nb.metadata["language_info"] = {"name": "python"}
    return nb


def main() -> None:
    project_root = find_project_root()
    notebook_dir = project_root / "notebooks"
    notebook_path = notebook_dir / "05_econometric_modelling.ipynb"
    notebook_dir.mkdir(parents=True, exist_ok=True)

    print(f"[INFO] Project root: {project_root}")
    print(f"[INFO] Notebook out: {notebook_path}")
    print(f"[INFO] Kernel:       {KERNEL_NAME}")
    print(f"[INFO] Cells:        {len(CELLS)}")
    print()

    nb = build_notebook()
    print(f"[BUILD] Notebook constructed with {len(nb.cells)} cells")

    print(f"[EXEC] Executing against kernel '{KERNEL_NAME}' "
          f"(timeout {EXEC_TIMEOUT_SECONDS}s)...")
    ep = ExecutePreprocessor(
        timeout=EXEC_TIMEOUT_SECONDS, kernel_name=KERNEL_NAME
    )
    try:
        ep.preprocess(nb, {"metadata": {"path": str(notebook_dir)}})
    except Exception as exc:
        print(f"[ERROR] Notebook execution failed: {exc}")
        # Save the partial notebook for debugging.
        partial_path = notebook_path.with_suffix(".partial.ipynb")
        with open(partial_path, "w", encoding="utf-8") as fh:
            nbf.write(nb, fh)
        print(f"[ERROR] Partial notebook saved to {partial_path}")
        raise

    with open(notebook_path, "w", encoding="utf-8") as fh:
        nbf.write(nb, fh)
    print(f"[OK] Wrote {notebook_path}")

    # Report figure paths
    figures_dir = project_root / "outputs" / "figures"
    expected_figs = [
        "phase05_s08_forest_plot.png",
        "phase05_s08_cluster_slopes.png",
        "phase05_s08_mnar_contingency.png",
    ]
    for fname in expected_figs:
        fp = figures_dir / fname
        if fp.exists():
            kb = fp.stat().st_size / 1024
            print(f"[OK] Figure: {fname} ({kb:.1f} KB)")
        else:
            print(f"[WARN] Figure missing: {fname}")


if __name__ == "__main__":
    main()
