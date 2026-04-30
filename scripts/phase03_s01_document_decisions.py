"""
Phase 03 - Step 01: Document EDA design decisions.

Purpose:
    Append the seven Phase 03 design decisions (geographic library, primary
    categorical lens, univariate display strategy, VIF threshold, time-series
    aggregation, pre-2000 enrolment presentation, unemployment 1990 handling)
    to PROJECT_LOG.md as a single dated entry. Idempotent: re-running this
    script does not duplicate the entry.

Inputs:
    PROJECT_LOG.md (read to check for existing entry).

Outputs:
    PROJECT_LOG.md (one new entry appended on first run; no-op afterwards).
"""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

# Make src/ importable when run from project root
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.paths import find_project_root  # noqa: E402


# Idempotency key: the exact header line that identifies this entry.
ENTRY_HEADER = "## 2026-04-30 — Phase 03, Step 01"

ENTRY_BODY = """\
**Context:** Phase 03 (Exploratory Data Analysis) opens with seven
presentation- and methodology-level decisions that shape the EDA notebook's
design. Five are open by design (geographic library, primary categorical
lens, univariate display for skewed variables, VIF flagging threshold,
time-series aggregation strategy); two were deferred from Phase 02
(pre-2000 enrolment sparsity, unemployment_rate 1990 missingness). These
are recorded before any plotting code is written so that the choices are
visible in the project's audit trail rather than hidden inside step
scripts.

**Decision:**

1. **Geographic visualisation library — plotly.** Use
   `plotly.express.choropleth` with `locations="iso3", locationmode="ISO-3"`.
   `geopandas` is not installed and will not be added in Phase 03.
2. **Primary categorical lens — both `region_name` and `income_level_name`,
   purposefully.** Region (n=7) carries the geographic narrative; income
   group (n=4) carries the structural-development narrative. Step 03 colours
   bivariate scatters by region (better discrimination across 7 categories);
   Step 05 produces parallel region- and income-faceted time-series views.
3. **Univariate display for skewed variables — raw and log side by side.**
   Applies to `gdp_per_capita_usd`, `gdp_per_capita_ppp`, and `population`.
   Other 16 variables are shown raw only.
4. **VIF flagging threshold — stratified.** `VIF > 5` flagged as "watch",
   `VIF > 10` flagged as "concern". The sector-share trio
   (`agri_value_added_gdp`, `manu_value_added_gdp`,
   `services_value_added_gdp`) is expected at the "concern" level by
   construction (sum-to-~100 constraint); separating thresholds prevents
   that structural collinearity from drowning out other multicollinearity
   signals.
5. **Time-series aggregation — region mean ± IQR and income-group mean ±
   IQR, in parallel.** Country-level lines (217 series) are unreadable.
   Where a stratum has N_countries < 5 (notably North America at n=3),
   suppress the IQR band and either show min–max range or individual lines.
6. **Pre-2000 enrolment sparsity — show full 1990–2023 with explicit
   sparsity indicator.** No truncation. Per-year N_observed annotated on
   enrolment time-series plots; interpretation text states that water-level
   comparisons should anchor on the post-2000 dense regime.
7. **`unemployment_rate` 1990 missingness — start unemployment line at
   1991 with footnote; do not restrict the rest of the panel.** ILO's
   modeled series begins in 1991 by construction. Other variables' 1990
   observations are not sacrificed for visual consistency; the
   variable-specific start year is documented per plot.

**Rationale:**

- **Plotly over geopandas:** plotly is already pinned (`plotly==6.7.0`),
  has built-in ISO-3 country lookups, and avoids the GDAL/PROJ dependency
  surface on Windows. The choropleth use case in Phase 03 is a simple
  country-level fill; the additional capabilities geopandas provides
  (projections, spatial joins) are not needed here.
- **Both categorical lenses, not one:** region and income group answer
  different analytical questions. Forcing a single lens would either
  weaken geographic storytelling (income only) or weaken inequality
  storytelling (region only). The two views together also support the
  Phase 04 clustering motivation by surfacing cases where region and
  income-group framings disagree.
- **Raw + log presentation:** raw alone hides the skew that motivates log
  transforms in Phase 05; log alone obscures the panel's actual storage
  convention (Phase 02 Decision 4: panel stores raw). Showing both makes
  the design separation between presentation and modelling explicit.
- **Stratified VIF:** a single threshold conflates two qualitatively
  different problems — structural collinearity (sector shares,
  by-construction) and incidental collinearity (e.g. between mean years
  of schooling and GDP per capita, which the modelling owns). Phase 05's
  variable-selection rationale benefits from seeing them separated.
- **Region/income parallel time-series:** the dual view supports the
  Phase 04 clustering motivation directly. Convergence within a region
  but divergence across income groups (or vice versa) is exactly the
  kind of pattern that justifies a country typology.
- **No enrolment truncation:** truncating the 1990s would silently drop
  the 20 countries with full primary-enrolment coverage and make the
  data appear cleaner than it is. Annotating sparsity is more honest
  and aligns with the project's portfolio framing on missingness.
- **Unemployment 1991 start:** restricting all subplots to 1991+ would
  discard ~190 country-year observations across other variables for the
  sake of one ILO series's start year. Per-line start years with a
  footnote is the more proportionate fix.

**Impact:**

- Step 02 (univariate distributions) emits a 5×4 grid where three
  variables (`gdp_per_capita_usd`, `gdp_per_capita_ppp`, `population`)
  appear as raw + log pairs.
- Step 03 (bivariate Gini) colours scatters by `region_name`.
- Step 04 (correlation/VIF) emits a stratified VIF table with watch/concern
  bands; Phase 05's specification rationale will cite this directly.
- Step 05 (time-series) emits two figures, one region-faceted and one
  income-faceted, with N_countries annotated and IQR bands suppressed
  where N < 5.
- Step 06 (choropleth) uses plotly only; `requirements.txt` is unchanged.
- Enrolment plots show 1990–2023 with year-level N_observed; unemployment
  time-series start at 1991 with a footnote.
- No `requirements.txt` changes for Phase 03 (geopandas not installed,
  kaleido added only if static-PNG export proves necessary in Step 06).
"""


def main() -> int:
    project_root = find_project_root(Path(__file__).resolve().parent)
    log_path = project_root / "PROJECT_LOG.md"

    if not log_path.exists():
        print(f"ERROR: {log_path} does not exist.", file=sys.stderr)
        return 1

    existing = log_path.read_text(encoding="utf-8")

    if ENTRY_HEADER in existing:
        print(f"PROJECT_LOG.md already contains '{ENTRY_HEADER}'. No-op.")
        return 0

    # Ensure the file ends with exactly one blank line before the new entry.
    sep = "" if existing.endswith("\n\n") else ("\n" if existing.endswith("\n") else "\n\n")

    new_entry = f"{sep}---\n\n{ENTRY_HEADER}\n\n{ENTRY_BODY}"

    with log_path.open("a", encoding="utf-8") as f:
        f.write(new_entry)

    # Verify
    final = log_path.read_text(encoding="utf-8")
    occurrences = final.count(ENTRY_HEADER)
    print(f"Appended Phase 03 Step 01 entry to {log_path}")
    print(f"  Header occurrences in file: {occurrences} (expected: 1)")
    print(f"  Entry size: {len(new_entry):,} chars")
    print(f"  Date: {date.today().isoformat()}")

    if occurrences != 1:
        print("  WARNING: header occurrence count != 1", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
