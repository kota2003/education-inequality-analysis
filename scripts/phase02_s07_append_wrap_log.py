"""
Phase 02 - Step 07: Append the Phase 02 completion entry to PROJECT_LOG.md

Purpose:
    Append a single dated wrap entry to PROJECT_LOG.md recording the
    closure of Phase 02 (analytical panel locked, missingness profile
    documented, three discoveries carried forward to Phase 03+). The
    append is idempotent: re-running the script does not duplicate the
    entry.

Inputs:
    <project_root>/PROJECT_LOG.md

Outputs:
    <project_root>/PROJECT_LOG.md  (appended in place if entry absent)

Notes:
    Non-stochastic. Same idempotent append pattern as
    phase02_s01_document_decisions.py: idempotency key is the exact
    ENTRY_HEADER line.
"""

from __future__ import annotations

import sys
from pathlib import Path


ENTRY_HEADER = "## 2026-04-25 — Phase 02 Completion"

ENTRY_BODY = """\
## 2026-04-25 — Phase 02 Completion

**Context:** Phase 02 — Data Cleaning & Integration — produced the
analytical panel from the raw layer Phase 01 deposited. Six step
scripts plus a portfolio notebook completed in sequence; the entire
processed layer is reproducible from a fresh clone given only the raw
files.

**Decision (closure):**

- The analytical panel is fixed at `data/processed/panel.csv`:
  7,378 rows × 24 columns (217 countries × 34 years; 5 metadata
  columns plus 19 variables in canonical manifest declaration order).
- Missingness is documented along three axes: per-variable in
  `outputs/tables/phase02_missingness_report.csv`, spatiotemporal in
  `outputs/figures/phase02_missingness_matrix.png`, narrative in
  `notebooks/02_data_cleaning.ipynb`.
- Phase 03 EDA inherits this panel without modification. Any future
  panel revision must produce a versioned successor and document the
  diff.

**Rationale:**

Three discoveries during Phase 02 carry forward as binding constraints
or named caveats:

1. **24 WB countries lack a usable `mys` value** (Phase 01 had
   predicted 22). The 2-country gap traces to HDR iso3 rows that
   exist but contain no observed `mys` for any year. Listwise
   deletion handles this automatically per Decision 2.
2. **Removing Gini more than doubles the listwise sample.** The
   illustrative `core_education_economic` specification (with Gini)
   yields 1,423 rows; the same specification without Gini yields
   3,041. This is the quantitative baseline for the MNAR caveat
   recorded in Step 01 Decision 3.
3. **`unemployment_rate` first observed year is 1991.** Any 1990
   cross-section using ILO modeled unemployment will be empty;
   FE specifications are unaffected.

**Impact:**

- Phase 03 EDA reads `panel.csv` directly; variables are in manifest
  declaration order, metadata in columns 3-5. `src.manifest` provides
  programmatic access to variable lists.
- Phase 05 modelling code owns log transforms, listwise deletion, and
  per-specification N reporting.
- Phase 07 inherits three named threats to identification:
  Gini measurement heterogeneity, MNAR selection (low-income
  under-coverage), and reverse causality.
- `src/` now contains `paths.py`, `manifest.py`, and
  `country_metadata.py` in addition to the `io_utils.py` promoted
  in Phase 01.
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

    print(f"[phase02_s07] project root : {project_root}")
    print(f"[phase02_s07] target log   : {log_path.relative_to(project_root)}")

    current = log_path.read_text(encoding="utf-8")
    current_lines = current.count("\n")

    if ENTRY_HEADER in current:
        print(
            f"[phase02_s07] entry header already present "
            f"({ENTRY_HEADER!r}); no changes made."
        )
        print(f"[phase02_s07] file unchanged: {current_lines} lines.")
        return 0

    trimmed = current.rstrip("\n") + "\n"
    separator = "\n---\n\n"
    new_content = trimmed + separator + ENTRY_BODY
    if not new_content.endswith("\n"):
        new_content += "\n"

    log_path.write_text(new_content, encoding="utf-8")

    new_lines = new_content.count("\n")
    added = new_lines - current_lines
    print(
        f"[phase02_s07] appended Phase 02 wrap entry: "
        f"{current_lines} -> {new_lines} lines (+{added})."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
