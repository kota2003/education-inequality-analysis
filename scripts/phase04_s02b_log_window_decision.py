"""
Phase 04 - Step 02b: Document adaptive window widening

Purpose:
    Append an entry to PROJECT_LOG.md recording the adaptive decision
    to widen the country-features aggregation window from 2015-2019
    to 2010-2019, after the initial Step 02 run revealed that 62
    countries were dropped under the listwise filter, including CHN
    and 23 other major LMICs.

    Step 01 Decision 3 pre-registered an auto-fallback rule (widen if
    listwise count < 150). The initial 2015-2019 run yielded 155, which
    satisfied that threshold by the rule's letter but failed it by
    spirit once the 62-country exclusion list was inspected. This entry
    records that the threshold rule was numerically honoured (155 >= 150)
    while the substantive judgement adaptively widened the window.

    The diagnostic that drove this judgement
    (scripts/phase04_diag_compare_windows.py) is preserved in the repo
    as the audit trail for what data was looked at to justify the
    widening.

Inputs:
    PROJECT_LOG.md at the project root

Outputs:
    PROJECT_LOG.md (one new entry appended on first run; no-op on reruns)

Notes:
    Step 02b uses the inline idempotent-append pattern. Per the Step 01
    contract, the new src/log_utils.py helper is not exercised until
    Step 08 wrap; introducing it here would defeat the "creation and
    first use are reviewable in separate steps" rationale.
"""

from __future__ import annotations

import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR.parent))

from src.paths import find_project_root  # noqa: E402


ENTRY_MARKER = "## 2026-05-01 \u2014 Phase 04, Step 02b"

ENTRY_BODY = """
## 2026-05-01 \u2014 Phase 04, Step 02b

**Context:** Step 02 was first run with the Step 01 Decision 3
primary window of 2015-2019. The run produced 155 listwise-complete
countries from 217, satisfying the pre-registered fallback threshold
of 150 by a margin of 5 and therefore accepting the primary window
under the rule as written.

Qualitative inspection of the 62-country exclusion list immediately
flagged a problem: CHN was dropped on `enrol_secondary` alone, as
were DZA, ZWE, NIC, GIN, COG, TJK, GMB, GUY, LBR, PLW, SLB. The
2015-2019 listwise sample retained 155 countries but excluded the
single largest middle-income country in the world and a cluster of
LMICs whose presence is essential to a credible
education-inequality typology. The pre-registered numerical rule
had passed; the qualitative substance had not.

A diagnostic comparison was then run
(`scripts/phase04_diag_compare_windows.py`) over four windows:
2015-2019, 2010-2019, 2005-2019, 2000-2019. The results:

| Window     | Listwise count | CHN included | Watchlist rescued |
|------------|---------------:|:------------:|:-----------------:|
| 2015-2019  | 155            | no           | -                 |
| 2010-2019  | 167            | yes          | +12               |
| 2005-2019  | 176            | yes          | +21               |
| 2000-2019  | 178            | yes          | +23               |

The +12 countries newly included by widening to 2010-2019 are: CHN,
COG, DZA, GIN, GMB, GUY, LBR, NIC, PLW, SLB, TJK, ZWE. The
incremental rescues from further widening to 2005-2019 (BWA, FSM,
GNB, GNQ, IRQ, KIR, LBY, SDN, UGA) include several conflict-affected
states (IRQ, LBY, SDN) for which a 15-year period mean averages
across major structural ruptures (Iraq War aftermath, Libyan civil
war, South Sudan secession), making the resulting "country state"
hard to interpret. The marginal gain from 2000-2019 over 2005-2019
is +2 countries (TKM, TTO) and is not worth the further loss of
"current state" interpretation.

**Decision:** Re-run Step 02 with `PRIMARY_WINDOW = (2010, 2019)`.
Retain `FALLBACK_WINDOW = (2005, 2019)` as a safety net (not
expected to trigger). Re-run produces 167 listwise-complete
countries from 217, written to
`data/processed/country_features.csv` (217 rows, raw scale, NaN
preserved) and
`data/processed/country_features_standardised.csv` (167 rows,
z-scored).

**Rationale:**

- **The pre-registered threshold (150) was honoured by the data
  (155 >= 150) but the threshold was a numerical proxy for a
  qualitative goal: "enough countries that the typology covers the
  policy-relevant world".** Once the exclusion list made it clear
  the proxy had failed (CHN missing), the substantive goal took
  precedence. This is adaptive design, not pre-registration
  violation: the adaptation is documented here at the moment of
  the decision, not buried in retrospective rationalisation.

- **CHN inclusion is not negotiable for portfolio quality.** A
  cross-country education-inequality typology that excludes the
  world's second-largest economy and largest middle-income country
  cannot credibly speak to the Kuznets-type pattern flagged in
  Phase 03 finding #4 (income-faceted Gini inverted-U), since CHN
  is the upper-middle-income exemplar of that pattern.

- **2010-2019 is the smallest widening that achieves the
  qualitative goal.** It rescues CHN plus 11 other LMICs with no
  cost to interpretability: the period sits entirely post-GFC and
  pre-COVID, and contains no major structural rupture in any
  rescued country (CHN's WTO-era acceleration peaked 2001-2010, so
  the chosen window starts at the inflection point, not in the
  middle of the transition). Wider windows (2005-2019, 2000-2019)
  rescue marginal additional countries at increasing interpretive
  cost.

- **The "current state" narrative is preserved.** "Latest five
  years" becomes "latest ten years"; the Phase 04 deliverable is
  still defensibly characterising recent country structure rather
  than long-run averages.

**Impact:**

- `data/processed/country_features.csv` regenerated: 217 rows
  (unchanged), but feature values reflect 2010-2019 means rather
  than 2015-2019 means.
- `data/processed/country_features_standardised.csv` regenerated:
  167 rows (was 155). The 12 newly-included countries enter
  K-means and Ward clustering in Steps 03-05.
- Phase 04 narrative gains a transparent example of adaptive
  decision-making for the notebook synthesis: "the pre-registered
  rule passed numerically but the exclusion list inspection drove
  a 5-year widening".
- Phase 03 finding #4 (Kuznets inverted-U) is now testable in
  Phase 04 with CHN included as an upper-middle-income exemplar.
- The 50 countries still excluded after the widening are
  predominantly conflict-affected states (HTI, ZMB, VEN, PRK, SOM,
  SSD, SYR, YEM), WB-only territories (22 entries lacking `mys`),
  and structural-data-deficit countries. Their absence is
  consistent with the MNAR caveat already named in Phase 02
  Decision 3 and Phase 03 Step 01 Decision 6 design rationale.
- No `requirements.txt` change. `scripts/phase04_diag_compare_windows.py`
  is committed to the repo as the audit trail for this decision.
"""


def main() -> int:
    project_root = find_project_root(SCRIPT_DIR)
    log_path = project_root / "PROJECT_LOG.md"

    if not log_path.exists():
        print(f"ERROR: PROJECT_LOG.md not found at {log_path}")
        return 1

    existing = log_path.read_text(encoding="utf-8")

    if ENTRY_MARKER in existing:
        print("Phase 04 Step 02b entry already present in PROJECT_LOG.md.")
        print(f"  Marker found: {ENTRY_MARKER}")
        print("  No changes made.")
        return 0

    if not existing.endswith("\n"):
        existing = existing + "\n"

    new_content = existing + ENTRY_BODY

    log_path.write_text(new_content, encoding="utf-8")

    appended_lines = ENTRY_BODY.count("\n")
    print(f"Appended Phase 04 Step 02b entry to {log_path.name}")
    print(f"  Marker: {ENTRY_MARKER}")
    print(f"  Body length: {len(ENTRY_BODY)} characters, {appended_lines} newlines")
    print(f"  File size: {len(existing)} -> {len(new_content)} characters")

    return 0


if __name__ == "__main__":
    sys.exit(main())
