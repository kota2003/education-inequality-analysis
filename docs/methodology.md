# Methodology

**Project:** Education and Income Inequality — A Cross-Country Panel Analysis
**Status:** v1.0 closed 2026-05-04
**Companion document:** [`findings.md`](findings.md)

This document explains *how* the project was built, not *what* it found. It is intended for readers who want to understand the workflow, tooling, and design conventions that produced the project's substantive results — particularly other data scientists evaluating the project's methodological seriousness, and Kota's future self when starting follow-on work.

---

## 1. Eight-phase workflow architecture

The project is organised into eight phases, executed sequentially:

| Phase | Title | Role | Notebook |
|:---:|---|---|---|
| 00 | Scope & Setup | Decide what is being built and what is not | (no notebook) |
| 01 | Data Collection | Acquire raw data from public APIs | `01_data_collection.ipynb` |
| 02 | Data Cleaning & Integration | Build the analytical panel | `02_data_cleaning.ipynb` |
| 03 | Exploratory Data Analysis | Characterise the panel descriptively | `03_eda.ipynb` |
| 04 | Country Clustering | Build a data-driven country typology | `04_country_clustering.ipynb` |
| 05 | Econometric Modelling | Estimate the education–Gini relationship under three identification strategies | `05_econometric_modelling.ipynb` |
| 06 | Predictive Modelling & Interpretability | Train ML models, compute SHAP attributions, validate cross-method | `06_predictive_modelling.ipynb` |
| 07 | Synthesis & Policy Discussion | Integrate phases 03–06, frame causal limits, specify follow-on identification | `07_synthesis_and_policy.ipynb` |

Each phase has a dedicated branch in git (`phase-NN-...`) and is closed with a `--no-ff` merge to `main` to preserve the per-phase work history, plus a `vN.X` tag on the merge commit (`v0.5` at end of Phase 05, `v0.6` at end of Phase 06, `v1.0` at end of Phase 07 — this last marking project close).

---

## 2. Pre-registration discipline

Each analysis-bearing phase opens with a **Step 01 decisions document**: a Python script that, before any modelling or new artefact is built, appends a structured entry to `PROJECT_LOG.md` recording the design decisions for that phase. The entry includes:

- **Decisions made** (numbered list, each with rationale)
- **Alternatives considered and rejected** (with rationale)
- **Step plan** for the remaining steps in the phase
- **Pre-registered acceptance criteria** for downstream steps where applicable

The point is to make commitments **before evidence arrives**, so that when evidence arrives, any deviation must be argued rather than absorbed silently. This is closer to a clinical-trial pre-registration discipline than a typical "make a notebook and see what happens" workflow, and it is the project's primary methodological signal.

The full decision register is in [`PROJECT_LOG.md`](../PROJECT_LOG.md). Each phase summary in `docs/phase_summaries/` (gitignored, internal-handoff documents) compresses the corresponding PROJECT_LOG entries into a single self-contained handoff document.

---

## 3. The adaptive-override convention

Pre-registration is rigid by design — but real evidence sometimes requires revision. The project handles this with an explicit convention (Convention 6.3 in `PROJECT_WORKFLOW.md`):

When evidence requires deviating from a pre-registered decision, an **adaptive override entry** is appended to `PROJECT_LOG.md` immediately at the point of deviation, before the new direction is executed. The entry must contain:

1. **What was pre-registered** (verbatim from the original Step 01 entry)
2. **What evidence triggered the override** (explicit, with reference to the artefact that produced the evidence)
3. **What the new decision is** (with the rationale that distinguishes it from the original)
4. **What downstream steps are affected** (and how they propagate)

The project recorded **one** adaptive override across its eight phases: **Phase 05 Step 07b**. The Step 01 pre-registration had specified that the Hausman test would pick a single estimator (FE versus RE) for the headline reporting. After execution, the Mundlak alternative-Hausman test under cluster-robust SE returned a different recommendation than the standard Hausman test, and a single-estimator headline became unjustifiable. The override replaced "Hausman picks one" with **tri-headline reporting** (Pooled OLS / FE / RE all reported with their distinctive identification structure), and the per-cluster Spec C analysis followed the new approach. The cross-method synthesis in Phase 06 and Phase 07 is downstream of this override.

The override is not a retraction of the pre-registration — it is a documented revision, with the original commitment and the revising evidence both preserved. **An adaptive override that is logged is methodologically stronger than retroactively rewriting the pre-registration.**

---

## 4. The `src/` promotion discipline

The project promotes utilities to the `src/` package only after they are used in **two or more different scripts or notebooks**. Promotion is not speculative: a function that has only one caller stays in the script that uses it.

Five modules earned `src/` placement across the project:

| Module | Promoted in | Used by |
|---|---|---|
| `src/paths.py` | Phase 02 | Every phase (project-root locator) |
| `src/manifest.py` | Phase 02 | Phases 01–06 (data source registry) |
| `src/country_metadata.py` | Phase 02 | Phases 02, 04 (WB country metadata loader, region / income-group lookups) |
| `src/io_utils.py` | Phase 02 | Phases 02–06 (encoding-fallback CSV reader for HDR cp1252 / UTF-8 mixed source) |
| `src/log_utils.py` | Phase 04 | Phases 04–07 (idempotent PROJECT_LOG append helper) |

Functions used only once stay inline. The discipline keeps `src/` lean and prevents speculative architecture — every module in `src/` has a working caller-of-record at the time of promotion.

---

## 5. Cross-method synthesis: cite, do not recompute

The synthesis layer in Phase 07 deliberately does **not re-estimate** any model from earlier phases. Convention 6.4 in `PROJECT_WORKFLOW.md` requires that every numerical claim in Phase 07 trace to a specific artefact produced earlier in the workflow (a CSV in `outputs/tables/`, a figure in `outputs/figures/`, or a PROJECT_LOG entry).

The implementation: `outputs/tables/phase07_s02_synthesis_table.csv` is a long-format table with a dedicated `source_artefact` column on every row. Every value in the convergence figure (`outputs/figures/phase07_s03_convergence.png`) and in the synthesis notebook (`notebooks/07_synthesis_and_policy.ipynb`) is loaded from this CSV — never re-estimated.

The discipline matters for two reasons:

1. **Reproducibility.** A reader who runs Phases 03–06 will find that the same numbers appear in the synthesis layer, because the synthesis layer reads them from the same artefacts. There is no path by which a Phase 07 number could silently diverge from its source.
2. **Auditability.** Every claim in Phase 07 is traceable to an exact file path. The provenance chain is one CSV column wide, not buried in script logic.

This is the methodological inverse of the common pattern in which the synthesis section of a long analysis silently re-runs computations and gradually accumulates undocumented divergences from the upstream sections.

---

## 6. Causal language discipline

Convention 6.13 in `PROJECT_WORKFLOW.md` requires that the project never state a causal claim about education and inequality. The discipline operates at three levels:

1. **Vocabulary.** "Association," "correlation," "slope," "attribution," "estimate" — never "effect," "impact," "causes," "leads to." Where a source paper uses causal language, paraphrase to associational form.
2. **Conditional framing for policy-adjacent claims.** Policy implications (Finding 4 in [`findings.md`](findings.md)) are written in explicit "if … were causal, then …" form. The conditional is not decorative — it is the only formulation consistent with the project's own evidence on causal limits (Finding 3).
3. **Active counterclaim where the evidence supports it.** The Phase 06 boundary-case holdout (test R² = −2.4 with mys SHAP sign-flip on three of four held-out countries) is the project's strongest **internal evidence against a causal reading** of the in-sample SHAP attribution. The project does not just decline to claim causation; it surfaces and emphasises the specific evidence that argues against it.

Verification was performed on the synthesis notebook at the end of Phase 07: HTML export of `notebooks/07_synthesis_and_policy.ipynb` was searched for instances of "causal effect"; all 9 occurrences are in conditional, negation, or framing contexts. There is no point in the notebook at which the project asserts that education has a causal effect on inequality.

---

## 7. Knowledge-handoff convention

Each phase ends with a phase summary in `docs/phase_summaries/phaseNN_summary.md`. These files are **direct markdown** — written by hand or by direct generation, without a Python wrapper script. Convention 6.6 specifies that documentation deliverables follow a different routing rule from analytical artefacts:

| Artefact type | Generation method |
|---|---|
| Data artefact (CSV, parquet, panel) | Python script in `scripts/` |
| Figure | Python script in `scripts/` |
| Trained model | Python script in `scripts/` |
| Notebook | Python script in `scripts/` (using nbformat / nbconvert) |
| **Phase summary markdown** | **Direct markdown, no script** |
| **PROJECT_LOG entry** | Direct content via `src.log_utils.append_log_entry` (kept in script form for idempotency) |

The phase-summary files are gitignored — they are internal handoff documents, not public deliverables. The public-facing equivalents are:

- `README.md` (recruiter entry point, regenerated by `scripts/update_readme.py`)
- `docs/findings.md` (substantive content, this document's companion)
- `docs/methodology.md` (this document)

The split exists because phase summaries are written for *me-three-months-later* — they include internal step record, exact file paths, and decision rationale at a level of detail that is essential for handoff but excessive for portfolio reading. Compressing them into the public-facing docs is a deliberate transformation, not a copy.

---

## 8. Data routing and reproducibility

**Raw data is gitignored**; only the manifest is committed (`data/raw/manifest.yaml`). The full data layer is reproducible from public APIs (World Bank, UNDP HDR) plus the manifest, by running the Phase 01 → 02 → 04 → 05 → 06 → 07 step scripts in sequence.

The reasoning: `data/raw/` is several hundred megabytes of CSV, much of it from the World Bank API, where source files are versioned by the API itself. Storing them in git would (a) bloat the repository, (b) duplicate state already maintained by the upstream source, and (c) create the risk that a future reader sees a stale snapshot rather than the live API response. The manifest is the contract — it specifies exactly which series, which date range, which API endpoint — and the step scripts are the implementation.

`data/processed/panel.csv` (the integrated 7,378-row × 24-column analytical panel) is also gitignored for the same reason: it is a deterministic function of `data/raw/` and the Phase 02 build scripts, and any reader who clones the repo can regenerate it in a few minutes.

---

## 9. Tools and stack discipline

| Layer | Choice | Rationale |
|---|---|---|
| Language | Python 3.11 | pinned across all phases via `.python-version` and `requirements.txt` |
| Environment | conda (`p4_education`) | isolation; `requirements.txt` is the single source of truth for dependencies |
| Data handling | pandas, numpy | standard |
| Country canonicalisation | pycountry + WB country metadata | iso3-coded throughout |
| Econometrics | statsmodels, linearmodels | linearmodels for clustered SE under PanelOLS |
| Machine learning | scikit-learn, xgboost, shap | XGBoost via the official sklearn-API wrapper for compatibility |
| Visualisation | matplotlib (primary), seaborn (categorical), plotly (geographic only) | matplotlib for everything that needs to render in GitHub's notebook viewer |
| Notebook tooling | jupyter, nbformat, nbconvert | notebooks are **built programmatically** in Phases 02, 06, 07 — not authored cell-by-cell |
| Version control | git, with per-phase branches and `--no-ff` merges | preserves per-phase work history in the audit trail |

**A note on programmatic notebook construction.** From Phase 02 onwards, portfolio notebooks are built by Python scripts using nbformat — not edited interactively. This means:

- The notebook is fully reproducible: the script is the source of truth, the `.ipynb` file is the build artefact
- Re-running the script regenerates the notebook deterministically
- Cell order, cell content, and figure embeds are all specified explicitly in the build script

This is a deliberate trade-off. Interactive notebook editing is faster for exploration; programmatic construction is more disciplined for portfolio deliverables. The project uses interactive notebooks during Phases 03 / 04 / 05 (where exploration drives design) and programmatic construction in Phases 02 / 06 / 07 (where the deliverable is a polished narrative).

---

## 10. What this methodology is for

The methodology described above is consistent with the project's substantive findings (see [`findings.md`](findings.md)) but is independent of them. The same workflow architecture would have been applied if the data had told a different story — including a story in which the education–Gini association was null, or in which the Cluster 1 chain failed boundary-reassignment robustness.

The point of the methodology is to make the project **legible to a future reader**: someone deciding whether to trust the substantive findings, someone considering a synthetic-control follow-on study built from this project's panel, or Kota's future self starting Project 5. The pre-registration discipline, the cite-not-recompute synthesis, the causal-language convention, and the documented adaptive override (Phase 05 Step 07b) are all in service of that one goal — that nothing in the project has to be taken on faith, because every analytical decision has a paper trail.

The substantive contribution of the project is the Cluster 1 chained finding and its caveats. The methodological contribution is the workflow that produced it.
