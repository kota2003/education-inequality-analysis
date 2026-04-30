"""
Phase 02 - Step 01: Document the five Phase 02 design decisions in PROJECT_LOG.md

Purpose:
    Append a single dated entry to PROJECT_LOG.md recording the five
    design decisions taken at the start of Phase 02 (analytical window,
    WB-only country handling, missingness strategy, log-transform
    timing, Gini provenance metadata). The append is idempotent:
    re-running the script does not duplicate the entry.

Inputs:
    <project_root>/PROJECT_LOG.md

Outputs:
    <project_root>/PROJECT_LOG.md  (appended in place if entry absent)

Notes:
    Non-stochastic: no RNG seeds required.
    The script locates the project root by walking up from its own
    location until it finds PROJECT_LOG.md, so it can be run from
    anywhere inside the project tree.
"""

from __future__ import annotations

import sys
from pathlib import Path


# The header line is used as the idempotency key. If a line equal to
# ENTRY_HEADER already exists in PROJECT_LOG.md, the script is a no-op.
ENTRY_HEADER = "## 2026-04-25 — Phase 02, Step 01"

ENTRY_BODY = """\
## 2026-04-25 — Phase 02, Step 01

**Context:** Phase 02 (Data Cleaning & Integration) opens with five
unresolved design questions inherited from Phase 01: analytical window,
treatment of WB-only countries lacking HDR coverage, missingness
strategy, timing of log transforms, and whether to attach Gini
provenance metadata. These choices shape the schema of
`data/processed/panel.csv` and constrain every downstream phase, so
they are recorded before any integration code is written.

**Decision:**

1. **Analytical window — keep 1990–2023 in the panel.** The panel
   retains the full Scope-committed window. Specification-level sample
   restrictions (e.g. enrolment-dependent models effectively becoming
   2000–2023) are handled at modelling time, not at panel construction.
2. **WB-only 22 countries — retain with `mys` = NaN.** The 22 entities
   (21 high-income territories/SARs + Kosovo) are kept in the panel.
   Listwise deletion at the specification level will drop them
   automatically from any model that includes `mys`.
3. **Missingness — listwise default, per-specification reporting.**
   Multiple imputation is rejected: Gini's missingness concentrates in
   low-income countries and is plausibly MNAR, so MI under MAR would
   inflate apparent power without controlling bias direction. Each
   model specification will report N and country coverage explicitly.
4. **Log transforms — apply at modelling time (Phase 05).** `panel.csv`
   stores `gdp_per_capita` and `population` in raw units. EDA in
   Phase 03 sees the genuine skewed distributions; Phase 05 applies
   `np.log` at use.
5. **Gini provenance metadata — not attached in Phase 02.** Adding a
   PIP method-type column would require a separate API call surface
   and would yield a partially-populated column that pollutes the
   panel. The Gini measurement-heterogeneity caveat is deferred to
   Phase 07's causal discussion (already flagged in Scope §12).

**Rationale:**

- **Discard data only once, at the latest stage.** Trimming the window
  or dropping countries during cleaning forecloses analyses (early-
  1990s descriptive statistics, high-income heterogeneity) that the
  panel could otherwise support at near-zero storage cost.
- **Statistical honesty over statistical convenience.** MI on a
  plausibly MNAR target inflates effective sample size without
  trustworthy bias control. Phase 07 framing the missingness as a
  named threat to identification is more credible than imputed
  coefficients.
- **Schema discipline.** Storing log columns or a half-populated
  provenance column duplicates state and creates "which column do I
  use?" ambiguity. The panel stores raw values; modelling-time code
  applies transformations.

**Impact:**

- Phase 02 Steps 02–05 build a panel keyed on (iso3, year) over
  1990–2023 with 217 WB countries (HDR's 195 plus the 22 WB-only).
- Phase 03 EDA can plot raw `gdp_per_capita` and `population`
  distributions directly; log scale is a plotting choice, not a
  schema fact.
- Phase 05 modelling code owns log transforms and listwise sample
  construction, and must report N + country coverage per
  specification.
- Phase 07 inherits the Gini measurement caveat and the MNAR
  selection-bias discussion.
"""


def find_project_root(start: Path) -> Path:
    """Walk upward from `start` until a directory containing PROJECT_LOG.md is found."""
    for candidate in [start, *start.parents]:
        if (candidate / "PROJECT_LOG.md").exists():
            return candidate
    raise FileNotFoundError(
        "Could not locate PROJECT_LOG.md by walking up from "
        f"{start}. Run this script from inside the project tree."
    )


def main() -> int:
    script_path = Path(__file__).resolve()
    project_root = find_project_root(script_path.parent)
    log_path = project_root / "PROJECT_LOG.md"

    print(f"[phase02_s01] project root : {project_root}")
    print(f"[phase02_s01] target log   : {log_path.relative_to(project_root)}")

    current = log_path.read_text(encoding="utf-8")
    current_lines = current.count("\n")

    # Idempotency check: bail out if this exact entry header is already
    # present anywhere in the file.
    if ENTRY_HEADER in current:
        print(
            f"[phase02_s01] entry header already present "
            f"({ENTRY_HEADER!r}); no changes made."
        )
        print(f"[phase02_s01] file unchanged: {current_lines} lines.")
        return 0

    # Normalise trailing whitespace, then append:
    #   <existing content>\n
    #   \n---\n\n
    #   <ENTRY_BODY>
    # which mirrors the existing inter-entry separator pattern.
    trimmed = current.rstrip("\n") + "\n"
    separator = "\n---\n\n"
    new_content = trimmed + separator + ENTRY_BODY
    if not new_content.endswith("\n"):
        new_content += "\n"

    log_path.write_text(new_content, encoding="utf-8")

    new_lines = new_content.count("\n")
    added = new_lines - current_lines
    print(
        f"[phase02_s01] appended Phase 02 Step 01 entry: "
        f"{current_lines} -> {new_lines} lines (+{added})."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
