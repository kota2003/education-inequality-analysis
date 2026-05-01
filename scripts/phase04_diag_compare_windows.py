"""
Phase 04 - Diagnostic: Compare aggregation windows

Purpose:
    Sanity check whether widening the aggregation window from 2015-2019
    to 2010-2019 (or 2005-2019) would meaningfully change the
    listwise-complete country set, and in particular whether it would
    rescue CHN, DZA, HTI, IRQ, UGA, ZMB, ZWE and other major LMICs
    that were dropped under the primary 2015-2019 window.

    This script does NOT write any CSV. It is a one-off diagnostic to
    inform whether to re-run Step 02 with a wider window.

Inputs:
    data/processed/panel.csv

Outputs:
    stdout only (counts and a list of "rescued" countries per widening)
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR.parent))

from src.paths import find_project_root  # noqa: E402

RAW_FEATURES = [
    "mean_years_schooling",
    "gdp_per_capita_ppp",
    "enrol_secondary",
    "agri_value_added_gdp",
    "manu_value_added_gdp",
    "services_value_added_gdp",
    "urban_population_pct",
]

# Major LMIC + headline countries we want to track explicitly.
WATCHLIST = [
    "CHN", "DZA", "HTI", "IRQ", "NIC", "UGA", "ZMB", "ZWE",
    "BWA", "GIN", "GNB", "TTO", "TJK", "LBY", "DJI", "COG",
    "VEN", "PRK", "SOM", "SSD", "SYR", "YEM",  # plausibly unsalvageable
    "XKX",  # WB-only, unsalvageable
]


def aggregate_window(panel: pd.DataFrame, window: tuple[int, int]) -> pd.DataFrame:
    start, end = window
    sub = panel[(panel["year"] >= start) & (panel["year"] <= end)]
    agg = (
        sub.groupby("iso3")[RAW_FEATURES]
        .agg(lambda s: np.nan if s.isna().all() else np.nanmean(s))
    )
    # Apply the log transform so listwise mirrors the real Step 02 logic
    agg["log_gdp_per_capita_ppp"] = np.log(agg["gdp_per_capita_ppp"])
    agg = agg.drop(columns=["gdp_per_capita_ppp"])
    return agg


def listwise_set(agg: pd.DataFrame) -> set[str]:
    final_features = [c for c in agg.columns]  # all are features here
    return set(agg.dropna(subset=final_features).index)


def main() -> int:
    project_root = find_project_root(SCRIPT_DIR)
    panel = pd.read_csv(project_root / "data" / "processed" / "panel.csv")

    windows = [(2015, 2019), (2010, 2019), (2005, 2019), (2000, 2019)]

    print(f"Panel: {panel.shape}, {panel['iso3'].nunique()} countries\n")

    sets: dict[tuple[int, int], set[str]] = {}
    for w in windows:
        agg = aggregate_window(panel, w)
        s = listwise_set(agg)
        sets[w] = s
        print(f"Window {w[0]}-{w[1]}: listwise-complete = {len(s)} / 217")

    # Rescued countries by widening from primary
    primary = sets[(2015, 2019)]
    print(f"\nWatchlist status (countries we want to include if possible):")
    print(f"  {'iso3':5s}  in 2015-2019  in 2010-2019  in 2005-2019  in 2000-2019")
    for iso in WATCHLIST:
        flags = ["YES" if iso in sets[w] else "no " for w in windows]
        print(f"  {iso:5s}  {flags[0]:12s}  {flags[1]:12s}  {flags[2]:12s}  {flags[3]:12s}")

    # Newly-rescued sets
    print(f"\nCountries newly included by widening:")
    for w in [(2010, 2019), (2005, 2019), (2000, 2019)]:
        rescued = sorted(sets[w] - primary)
        print(f"  {w[0]}-{w[1]}: +{len(rescued)} countries")
        if rescued:
            # Just iso3 codes for readability
            print(f"    {', '.join(rescued)}")

    # Check that 2015-2019 isn't strictly superior (countries lost by widening)
    for w in [(2010, 2019), (2005, 2019), (2000, 2019)]:
        lost = sorted(primary - sets[w])
        if lost:
            print(f"\n  WARNING: {w[0]}-{w[1]} loses {len(lost)} countries that 2015-2019 had: {lost}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
