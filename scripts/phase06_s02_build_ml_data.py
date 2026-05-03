"""
Phase 06 - Step 02: Build modelling-ready ML dataset.

Purpose:
    Construct panel_ml.csv for Phase 06 from panel_modelling.csv.
    Apply the Spec A listwise row filter, retain the 13 Decision-1
    features plus target plus metadata, materialise the temporal
    train (year <= 2018) / test (2019-2023) split as a column.
    NaN handling for extended features is deferred to per-model
    preprocessing in Steps 03-05 (Decision 5).

Inputs:
    data/processed/panel_modelling.csv   (7,378 x 30 from Phase 05 Step 02)

Outputs:
    data/processed/panel_ml.csv          (1,642 x ~21)
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

# Make src importable.
PROJECT_ROOT_HINT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT_HINT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT_HINT))

from src.paths import find_project_root
from src.io_utils import read_csv_with_encoding_fallback

SEED = 42
np.random.seed(SEED)

# Decision 1: 13-feature extended specification.
SPEC_A_FEATURES = [
    "mean_years_schooling",
    "enrol_secondary",
    "log_gdp_per_capita_ppp",
    "log_population",
    "urban_population_pct",
]

EXTENDED_FEATURES = [
    "agri_value_added_gdp",
    "manu_value_added_gdp",
    "services_value_added_gdp",
    "trade_openness",
    "gov_expenditure_gdp",
    "inflation_cpi",
    "unemployment_rate",
]

CLUSTER_FEATURE = "cluster_kmeans_k3"

ALL_FEATURES = SPEC_A_FEATURES + EXTENDED_FEATURES + [CLUSTER_FEATURE]

TARGET = "gini"

METADATA_COLUMNS = ["iso3", "country_name", "year", "region_name", "income_level_name"]

# Decision 3: temporal holdout. Train year boundary inclusive on the train side.
TRAIN_YEAR_MAX = 2018
TEST_YEAR_MIN = 2019
TEST_YEAR_MAX = 2023

# Phase 05 anchor for the Spec A listwise sample size.
EXPECTED_N_ROWS = 1642
EXPECTED_N_COUNTRIES = 153


def main() -> None:
    project_root = find_project_root()
    in_path = project_root / "data" / "processed" / "panel_modelling.csv"
    out_path = project_root / "data" / "processed" / "panel_ml.csv"

    print(f"[phase06_s02] project root: {project_root}")
    print(f"[phase06_s02] input:        {in_path}")
    print(f"[phase06_s02] output:       {out_path}")

    if not in_path.exists():
        raise FileNotFoundError(
            f"Phase 05 Step 02 output not found at {in_path}. "
            f"Cannot proceed."
        )

    # Load panel_modelling.csv (read_csv_with_encoding_fallback returns a tuple).
    df_full, encoding_used = read_csv_with_encoding_fallback(in_path)
    print(f"[phase06_s02] loaded:       {df_full.shape[0]:,} rows x "
          f"{df_full.shape[1]} cols (encoding={encoding_used})")

    # Sanity: check expected columns are present.
    missing_required = [
        c for c in METADATA_COLUMNS + ALL_FEATURES + [TARGET]
        if c not in df_full.columns
    ]
    if missing_required:
        raise KeyError(
            f"Required columns missing from panel_modelling.csv: "
            f"{missing_required}. Available: {sorted(df_full.columns.tolist())}"
        )

    # Step A: Spec A listwise filter.
    spec_a_required = [TARGET] + SPEC_A_FEATURES
    print(f"\n[phase06_s02] Spec A listwise filter on: {spec_a_required}")
    pre_n = len(df_full)
    df = df_full.dropna(subset=spec_a_required).copy()
    post_n = len(df)
    print(f"[phase06_s02] rows: {pre_n:,} -> {post_n:,} "
          f"(dropped {pre_n - post_n:,})")

    n_countries = df["iso3"].nunique()
    print(f"[phase06_s02] countries: {n_countries}")

    # Sanity: match Phase 05 anchor.
    if post_n != EXPECTED_N_ROWS:
        print(f"[phase06_s02] WARNING: row count {post_n:,} != Phase 05 "
              f"anchor {EXPECTED_N_ROWS:,}")
    if n_countries != EXPECTED_N_COUNTRIES:
        print(f"[phase06_s02] WARNING: country count {n_countries} != "
              f"Phase 05 anchor {EXPECTED_N_COUNTRIES}")

    # Step B: select the columns we keep.
    keep_columns = METADATA_COLUMNS + [TARGET] + ALL_FEATURES
    df_ml = df[keep_columns].copy()
    print(f"\n[phase06_s02] selected:    {len(keep_columns)} columns "
          f"({len(METADATA_COLUMNS)} metadata + 1 target + "
          f"{len(ALL_FEATURES)} features)")

    # Step C: materialise train / test split.
    year = df_ml["year"].astype(int)
    df_ml["year"] = year

    train_mask = year <= TRAIN_YEAR_MAX
    test_mask = (year >= TEST_YEAR_MIN) & (year <= TEST_YEAR_MAX)

    df_ml["split"] = np.where(train_mask, "train",
                              np.where(test_mask, "test", "exclude"))

    n_train = (df_ml["split"] == "train").sum()
    n_test = (df_ml["split"] == "test").sum()
    n_exclude = (df_ml["split"] == "exclude").sum()
    print(f"\n[phase06_s02] split (year-based):")
    print(f"  train (year <= {TRAIN_YEAR_MAX}): {n_train:,} rows "
          f"({100 * n_train / post_n:.1f}%)")
    print(f"  test  ({TEST_YEAR_MIN}-{TEST_YEAR_MAX}):     {n_test:,} rows "
          f"({100 * n_test / post_n:.1f}%)")
    print(f"  exclude:                  {n_exclude:,} rows "
          f"({100 * n_exclude / post_n:.1f}%)")

    if n_exclude > 0:
        excluded_years = sorted(df_ml.loc[df_ml["split"] == "exclude",
                                          "year"].unique().tolist())
        print(f"  excluded years: {excluded_years}")

    # No-leakage assertion: train / test years disjoint.
    train_years = set(df_ml.loc[df_ml["split"] == "train", "year"].unique())
    test_years = set(df_ml.loc[df_ml["split"] == "test", "year"].unique())
    overlap = train_years & test_years
    if overlap:
        raise AssertionError(
            f"Train / test year overlap detected: {sorted(overlap)}. "
            f"This is a leakage bug."
        )
    print(f"[phase06_s02] leakage check: train years and test years "
          f"disjoint (OK)")

    # Step D: per-feature missingness on the analytical sample.
    print(f"\n[phase06_s02] per-feature missingness on analytical sample "
          f"(N={post_n:,}):")
    for col in ALL_FEATURES:
        n_missing = df_ml[col].isna().sum()
        pct_missing = 100 * n_missing / post_n
        flag = "  Spec A" if col in SPEC_A_FEATURES else (
            "  cluster" if col == CLUSTER_FEATURE else "  extended")
        print(f"  {col:<32} missing={n_missing:>5,} "
              f"({pct_missing:>5.1f}%){flag}")

    # Step E: cluster column distribution sanity (Phase 04 IDs are 0/1/2).
    print(f"\n[phase06_s02] cluster_kmeans_k3 distribution:")
    cluster_counts = df_ml[CLUSTER_FEATURE].value_counts(dropna=False).sort_index()
    for cluster_val, count in cluster_counts.items():
        label = f"cluster {int(cluster_val)}" if pd.notna(cluster_val) else "NaN (Phase 04 excluded)"
        print(f"  {label}: {count:,}")

    # Sanity: all non-NaN cluster values must be 0, 1, or 2.
    valid_clusters = {0, 1, 2}
    observed_clusters = set(
        df_ml[CLUSTER_FEATURE].dropna().astype(int).unique().tolist()
    )
    invalid = observed_clusters - valid_clusters
    if invalid:
        raise AssertionError(
            f"Unexpected cluster values: {invalid}. Expected subset of "
            f"{valid_clusters}."
        )
    print(f"[phase06_s02] cluster value check: {observed_clusters} subset of "
          f"{{0, 1, 2}} (OK)")

    # Step F: gini sanity (panel mean Gini ~ 35-40 across phases).
    print(f"\n[phase06_s02] target (gini) summary:")
    print(f"  mean: {df_ml[TARGET].mean():.2f}")
    print(f"  std:  {df_ml[TARGET].std():.2f}")
    print(f"  min:  {df_ml[TARGET].min():.2f}")
    print(f"  max:  {df_ml[TARGET].max():.2f}")

    # Step G: write output.
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df_ml.to_csv(out_path, index=False, encoding="utf-8")
    print(f"\n[phase06_s02] wrote: {out_path}")
    print(f"[phase06_s02]   shape: {df_ml.shape[0]:,} rows x "
          f"{df_ml.shape[1]} cols")
    print(f"[phase06_s02]   columns: {df_ml.columns.tolist()}")

    print("\n[phase06_s02] Step 02 complete.")


if __name__ == "__main__":
    main()
