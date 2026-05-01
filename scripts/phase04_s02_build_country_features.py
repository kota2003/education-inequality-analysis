"""
Phase 04 - Step 02: Build country-level feature matrix

Purpose:
    Aggregate the analytical panel to one row per country with the
    seven Step 01-approved features, applying the log transform to
    `gdp_per_capita_ppp`, and emit two CSVs: the raw 217-row matrix
    (with NaN preserved) and the listwise-complete z-scored matrix
    used by Step 03 onward.

    The 2015-2019 aggregation window is preferred per Step 01
    Decision 3. If listwise-complete countries < 150 (~70% of 217),
    the window auto-widens to 2010-2019 and the change is reported
    in stdout for transparent inclusion in the Step 02 PROJECT_LOG
    entry (the Phase 04 wrap log will pick this up; Step 02 does not
    append to PROJECT_LOG itself).

Inputs:
    data/processed/panel.csv

Outputs:
    data/processed/country_features.csv
    data/processed/country_features_standardised.csv
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

# Make src/ importable when running from scripts/
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT_GUESS = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT_GUESS))

from src.paths import find_project_root  # noqa: E402

# Reproducibility (no stochastic operations in this step, but pinned by
# Workflow convention)
SEED = 42
np.random.seed(SEED)


# Feature set per Phase 04 Step 01 Decision 1
RAW_FEATURES = [
    "mean_years_schooling",
    "gdp_per_capita_ppp",
    "enrol_secondary",
    "agri_value_added_gdp",
    "manu_value_added_gdp",
    "services_value_added_gdp",
    "urban_population_pct",
]

# Features to log-transform per Phase 03 skewness reference
LOG_FEATURES = ["gdp_per_capita_ppp"]

# Final feature column names after log transform
def _final_feature_names() -> list[str]:
    out = []
    for f in RAW_FEATURES:
        if f in LOG_FEATURES:
            out.append(f"log_{f}")
        else:
            out.append(f)
    return out


FINAL_FEATURES = _final_feature_names()

METADATA_COLS = ["iso3", "country_name", "region_name", "income_level_name"]

# Per Phase 04 Step 01 Decision 3, with adaptive widening recorded in
# Phase 04 Step 02b: the initial 2015-2019 primary window was empirically
# replaced after a diagnostic comparison showed that widening to
# 2010-2019 rescues CHN and 11 other policy-relevant LMICs (CHN, COG,
# DZA, GIN, GMB, GUY, LBR, NIC, PLW, SLB, TJK, ZWE) with a 155 -> 167
# country gain. The 2005-2019 fallback remains as a safety net but is
# not expected to trigger.
PRIMARY_WINDOW = (2010, 2019)
FALLBACK_WINDOW = (2005, 2019)
LISTWISE_THRESHOLD = 150


def aggregate_window(panel: pd.DataFrame, window: tuple[int, int]) -> pd.DataFrame:
    """Return one row per country: 5-year (or 10-year) nanmean of each feature.

    Metadata columns are taken from the first occurrence of each iso3
    (they are time-invariant in the panel by construction, per Phase
    02 Step 04: metadata merged on iso3).
    """
    start, end = window
    sub = panel[(panel["year"] >= start) & (panel["year"] <= end)].copy()

    # Aggregate features (nanmean ignores NaN; if all NaN for a country,
    # produces NaN, which is then handled by listwise filter)
    feat_agg = (
        sub.groupby("iso3")[RAW_FEATURES]
        .agg(lambda s: np.nan if s.isna().all() else np.nanmean(s))
        .reset_index()
    )

    # Pull metadata from the panel (one row per iso3; take first)
    meta = (
        panel[METADATA_COLS]
        .drop_duplicates(subset=["iso3"], keep="first")
        .reset_index(drop=True)
    )

    out = meta.merge(feat_agg, on="iso3", how="left")
    return out


def apply_log_transforms(df: pd.DataFrame) -> pd.DataFrame:
    """Replace each LOG_FEATURE column with its natural log, renamed `log_<col>`."""
    df = df.copy()
    for col in LOG_FEATURES:
        # np.log(NaN) = NaN, np.log of negatives = NaN; values in
        # gdp_per_capita_ppp are strictly positive in the WB data
        df[f"log_{col}"] = np.log(df[col])
        df = df.drop(columns=[col])
    return df


def listwise_complete(df: pd.DataFrame, feature_cols: list[str]) -> pd.DataFrame:
    """Return rows where every column in feature_cols is non-NaN."""
    mask = df[feature_cols].notna().all(axis=1)
    return df.loc[mask].copy()


def zscore_features(df: pd.DataFrame, feature_cols: list[str]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Z-score the listed feature columns; return (standardised_df, summary_df).

    summary_df has one row per feature with raw mean/std and post-zscore
    mean/std (the latter pair is a sanity check: should be ~0 / ~1).
    """
    df = df.copy()
    rows = []
    for col in feature_cols:
        raw_mean = df[col].mean()
        raw_std = df[col].std(ddof=0)  # population std for clustering use
        df[col] = (df[col] - raw_mean) / raw_std
        rows.append(
            {
                "feature": col,
                "raw_mean": raw_mean,
                "raw_std": raw_std,
                "post_zscore_mean": df[col].mean(),
                "post_zscore_std": df[col].std(ddof=0),
            }
        )
    return df, pd.DataFrame(rows)


def main() -> int:
    project_root = find_project_root(SCRIPT_DIR)
    panel_path = project_root / "data" / "processed" / "panel.csv"
    out_raw_path = project_root / "data" / "processed" / "country_features.csv"
    out_std_path = (
        project_root / "data" / "processed" / "country_features_standardised.csv"
    )

    if not panel_path.exists():
        print(f"ERROR: panel.csv not found at {panel_path}")
        return 1

    print(f"Reading panel from: {panel_path}")
    panel = pd.read_csv(panel_path)
    print(f"  panel shape: {panel.shape}")
    print(f"  unique iso3: {panel['iso3'].nunique()}")
    print(f"  year range: {panel['year'].min()}-{panel['year'].max()}")

    # Verify required columns are present
    missing_cols = [c for c in METADATA_COLS + RAW_FEATURES + ["year"] if c not in panel.columns]
    if missing_cols:
        print(f"ERROR: panel.csv is missing required columns: {missing_cols}")
        return 1

    # ---- Aggregate using primary window ----
    print(f"\nAggregating with primary window {PRIMARY_WINDOW[0]}-{PRIMARY_WINDOW[1]}...")
    agg_primary = aggregate_window(panel, PRIMARY_WINDOW)
    agg_primary = apply_log_transforms(agg_primary)

    complete_primary = listwise_complete(agg_primary, FINAL_FEATURES)
    print(f"  countries in aggregated frame: {len(agg_primary)}")
    print(f"  listwise-complete countries: {len(complete_primary)}")
    print(f"  threshold: {LISTWISE_THRESHOLD}")

    # ---- Decide which window to use ----
    if len(complete_primary) >= LISTWISE_THRESHOLD:
        window_used = PRIMARY_WINDOW
        agg = agg_primary
        complete = complete_primary
        print(f"  -> using primary window {PRIMARY_WINDOW[0]}-{PRIMARY_WINDOW[1]}")
    else:
        print(
            f"  listwise-complete count {len(complete_primary)} < "
            f"{LISTWISE_THRESHOLD}; falling back to "
            f"{FALLBACK_WINDOW[0]}-{FALLBACK_WINDOW[1]}"
        )
        agg_fallback = aggregate_window(panel, FALLBACK_WINDOW)
        agg_fallback = apply_log_transforms(agg_fallback)
        complete_fallback = listwise_complete(agg_fallback, FINAL_FEATURES)
        print(f"  fallback listwise-complete countries: {len(complete_fallback)}")
        window_used = FALLBACK_WINDOW
        agg = agg_fallback
        complete = complete_fallback

    print(f"\nFinal aggregation window: {window_used[0]}-{window_used[1]}")
    print(f"Pre-listwise rows (all WB countries): {len(agg)}")
    print(f"Post-listwise rows (used for clustering): {len(complete)}")

    # ---- Per-feature non-null counts on the raw 217-row frame ----
    print("\nPer-feature non-null counts on raw 217-row frame:")
    for col in FINAL_FEATURES:
        nn = agg[col].notna().sum()
        pct = 100.0 * nn / len(agg)
        print(f"  {col:35s} {nn:3d} / {len(agg)} ({pct:5.1f}%)")

    # ---- Dropped countries ----
    dropped = agg.loc[~agg[FINAL_FEATURES].notna().all(axis=1)]
    print(f"\nDropped countries (any NaN among 7 features): {len(dropped)}")
    if len(dropped) > 0:
        # Compact listing: iso3 plus which features are NaN
        for _, row in dropped.sort_values("iso3").iterrows():
            missing_feats = [c for c in FINAL_FEATURES if pd.isna(row[c])]
            iso3 = row["iso3"]
            cname = row["country_name"]
            print(f"  {iso3} ({cname}): missing {missing_feats}")

    # ---- Standardisation on the listwise-complete frame ----
    standardised, std_summary = zscore_features(complete, FINAL_FEATURES)

    print("\nStandardisation summary (pre and post z-score):")
    with pd.option_context("display.float_format", "{:8.4f}".format):
        print(std_summary.to_string(index=False))

    # ---- NaN sanity checks ----
    nan_in_std = standardised[FINAL_FEATURES].isna().sum().sum()
    print(f"\nNaN count in standardised features: {nan_in_std} (must be 0)")
    assert nan_in_std == 0, "Standardised frame must have no NaN"

    # ---- Write outputs ----
    # Reorder columns to declared order: metadata first, then features
    # in FINAL_FEATURES order. (apply_log_transforms drops and appends,
    # so log_gdp_per_capita_ppp would otherwise land at the end.)
    column_order = METADATA_COLS + FINAL_FEATURES
    agg = agg[column_order]
    standardised = standardised[column_order]

    out_raw_path.parent.mkdir(parents=True, exist_ok=True)
    agg.to_csv(out_raw_path, index=False)
    standardised.to_csv(out_std_path, index=False)

    print(f"\nWrote:")
    print(f"  {out_raw_path.relative_to(project_root)}  ({len(agg)} rows x {len(agg.columns)} cols)")
    print(
        f"  {out_std_path.relative_to(project_root)}  "
        f"({len(standardised)} rows x {len(standardised.columns)} cols)"
    )

    print(f"\nFeature set used (final names): {FINAL_FEATURES}")
    print(f"Aggregation window used: {window_used[0]}-{window_used[1]}")
    print(f"Listwise-complete countries: {len(complete)} / {len(agg)}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
