"""
Phase 04 - Step 05: Cluster profiles

Purpose:
    Characterise each of the K=3 K-means clusters along three axes:
    (a) demographic composition (income group counts, region counts);
    (b) feature statistics (mean and quartiles of the 7 raw-scale
        features that defined the clustering);
    (c) descriptive Gini statistics computed as country-level Gini
        means over 2010-2019, then aggregated per cluster.

    The Gini statistics here are descriptive only - they characterise
    the inequality outcome distribution within each cluster but do
    NOT make explanatory claims. Phase 05 owns the explanatory layer.

Inputs:
    data/processed/panel.csv (for Gini)
    data/processed/country_features.csv (raw-scale features, 217 rows)
    outputs/tables/phase04_s04_cluster_assignments.csv (167 rows;
        cluster_kmeans_k3 is the primary cluster column consumed here)

Outputs:
    outputs/tables/phase04_s05_cluster_profiles.csv (3 rows x ~50 cols)
    stdout: per-cluster narrative profile + cross-cluster Gini summary
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR.parent))

from src.paths import find_project_root  # noqa: E402

# ---- Configuration ----
GINI_WINDOW = (2010, 2019)
PRIMARY_CLUSTER_COL = "cluster_kmeans_k3"

# Cluster ordering after Step 04 canonicalisation:
# cluster 0 = lowest mean_years_schooling, cluster 2 = highest
CLUSTER_LABELS = {
    0: "Low-development / Sub-Saharan-led",
    1: "Middle-development / Kuznets transition",
    2: "High-development / mature economies",
}

# Feature columns in country_features.csv (post log-transform)
FEATURE_COLS = [
    "mean_years_schooling",
    "log_gdp_per_capita_ppp",
    "enrol_secondary",
    "agri_value_added_gdp",
    "manu_value_added_gdp",
    "services_value_added_gdp",
    "urban_population_pct",
]

# Canonical income-group abbreviations (and order)
INCOME_ORDER = ["Low income", "Lower middle income", "Upper middle income",
                "High income", "Not classified"]
INCOME_ABBREV = {
    "Low income": "LIC",
    "Lower middle income": "LMC",
    "Upper middle income": "UMC",
    "High income": "HIC",
    "Not classified": "NC",
}

# Canonical region abbreviations (and order)
# WB renamed MENA in 2024 to include AFG + PAK; both spellings are mapped
# in case a future data refresh reverts. The data Phase 02 produced uses
# "Middle East, North Africa, Afghanistan & Pakistan".
REGION_ABBREV = {
    "East Asia & Pacific": "EAP",
    "Europe & Central Asia": "ECA",
    "Latin America & Caribbean": "LAC",
    "Middle East, North Africa, Afghanistan & Pakistan": "MENAAP",
    "Middle East & North Africa": "MENA",  # legacy
    "North America": "NA",
    "South Asia": "SAS",
    "Sub-Saharan Africa": "SSA",
}
REGION_ORDER_ABBREV = ["EAP", "ECA", "LAC", "MENAAP", "MENA", "NA", "SAS", "SSA"]

METADATA_COLS = ["iso3", "country_name", "region_name", "income_level_name"]


def country_gini_means(panel: pd.DataFrame, window: tuple[int, int]) -> pd.Series:
    """Per-country Gini mean over `window` years; NaN if no observations."""
    sub = panel[(panel["year"] >= window[0]) & (panel["year"] <= window[1])]
    return sub.groupby("iso3")["gini"].mean()


def build_cluster_profile(cluster_df: pd.DataFrame) -> dict:
    """Compute the wide-format profile dict for one cluster."""
    profile: dict = {}
    profile["n_countries"] = len(cluster_df)

    # Income distribution
    inc_counts = cluster_df["income_level_name"].value_counts()
    for inc_name in INCOME_ORDER:
        abbrev = INCOME_ABBREV[inc_name]
        profile[f"income_{abbrev}_n"] = int(inc_counts.get(inc_name, 0))

    # Region distribution (strip trailing whitespace, then map)
    cluster_df = cluster_df.copy()
    cluster_df["region_stripped"] = cluster_df["region_name"].str.strip()
    region_counts = cluster_df["region_stripped"].value_counts()
    # Initialise with zeros so column schema is stable
    for abbrev in REGION_ORDER_ABBREV:
        profile[f"region_{abbrev}_n"] = 0
    # Fill from data
    unmapped: list[str] = []
    for region_name, n in region_counts.items():
        abbrev = REGION_ABBREV.get(region_name)
        if abbrev is None:
            unmapped.append(region_name)
            continue
        profile[f"region_{abbrev}_n"] = int(n)
    if unmapped:
        print(f"  WARNING: unmapped region(s) in cluster: {unmapped}")

    # Feature statistics (raw scale)
    for feat in FEATURE_COLS:
        vals = cluster_df[feat].dropna()
        profile[f"{feat}_mean"] = float(vals.mean()) if len(vals) else np.nan
        profile[f"{feat}_p25"] = float(vals.quantile(0.25)) if len(vals) else np.nan
        profile[f"{feat}_p50"] = float(vals.quantile(0.50)) if len(vals) else np.nan
        profile[f"{feat}_p75"] = float(vals.quantile(0.75)) if len(vals) else np.nan

    # Gini statistics
    gini_vals = cluster_df["gini_country_mean"].dropna()
    profile["gini_n"] = int(len(gini_vals))
    profile["gini_mean"] = float(gini_vals.mean()) if len(gini_vals) else np.nan
    profile["gini_p25"] = float(gini_vals.quantile(0.25)) if len(gini_vals) else np.nan
    profile["gini_p50"] = float(gini_vals.quantile(0.50)) if len(gini_vals) else np.nan
    profile["gini_p75"] = float(gini_vals.quantile(0.75)) if len(gini_vals) else np.nan

    return profile


def print_cluster_narrative(cluster_id: int, profile: dict) -> None:
    """Print a human-readable narrative for one cluster."""
    label = CLUSTER_LABELS.get(cluster_id, f"Cluster {cluster_id}")
    n = profile["n_countries"]
    print(f"\n=== Cluster {cluster_id}: \"{label}\" (n={n}) ===")

    # Income line: only show non-zero categories
    inc_parts = []
    for inc_name in INCOME_ORDER:
        ab = INCOME_ABBREV[inc_name]
        c = profile[f"income_{ab}_n"]
        if c > 0:
            inc_parts.append(f"{c} {ab}")
    print(f"Income:  {', '.join(inc_parts)}")

    # Region line: only show non-zero categories
    reg_parts = []
    for ab in REGION_ORDER_ABBREV:
        c = profile.get(f"region_{ab}_n", 0)
        if c > 0:
            reg_parts.append(f"{c} {ab}")
    print(f"Region:  {', '.join(reg_parts)}")

    # Features
    print("Features (raw scale, mean [P25, P75]):")
    for feat in FEATURE_COLS:
        m = profile[f"{feat}_mean"]
        p25 = profile[f"{feat}_p25"]
        p75 = profile[f"{feat}_p75"]
        # Annotate log_gdp with USD equivalent
        annot = ""
        if feat == "log_gdp_per_capita_ppp":
            usd = np.exp(m)
            annot = f"  (~${usd:,.0f} PPP)"
        # Format: percentage features get a percent suffix
        is_pct = feat.endswith(("_pct", "_secondary", "_gdp"))
        unit = "%" if is_pct else " "
        print(f"  {feat:28s}: {m:7.2f}{unit} [{p25:6.2f}, {p75:6.2f}]{annot}")

    # Gini
    gn = profile["gini_n"]
    gn_pct = 100.0 * gn / n if n > 0 else 0.0
    if gn > 0:
        gm = profile["gini_mean"]
        gmed = profile["gini_p50"]
        g25 = profile["gini_p25"]
        g75 = profile["gini_p75"]
        print(
            f"Gini ({GINI_WINDOW[0]}-{GINI_WINDOW[1]} country mean, descriptive only):"
        )
        print(f"  n with Gini: {gn}/{n} ({gn_pct:.0f}%)")
        print(f"  mean:        {gm:5.2f}")
        print(f"  median:      {gmed:5.2f}")
        print(f"  IQR:         [{g25:.2f}, {g75:.2f}]")
    else:
        print(f"Gini: no observations available in cluster")


def main() -> int:
    project_root = find_project_root(SCRIPT_DIR)
    panel_path = project_root / "data" / "processed" / "panel.csv"
    feat_path = project_root / "data" / "processed" / "country_features.csv"
    cluster_path = (
        project_root / "outputs" / "tables" / "phase04_s04_cluster_assignments.csv"
    )
    out_csv = project_root / "outputs" / "tables" / "phase04_s05_cluster_profiles.csv"

    for p in (panel_path, feat_path, cluster_path):
        if not p.exists():
            print(f"ERROR: required input not found at {p}")
            return 1

    panel = pd.read_csv(panel_path)
    features = pd.read_csv(feat_path)
    clusters = pd.read_csv(cluster_path)

    print(f"Loaded:")
    print(f"  panel.csv                       : {panel.shape}")
    print(f"  country_features.csv            : {features.shape}")
    print(f"  phase04_s04_cluster_assignments : {clusters.shape}")
    print(f"  Gini window                     : {GINI_WINDOW[0]}-{GINI_WINDOW[1]}")
    print(f"  Primary cluster column          : {PRIMARY_CLUSTER_COL}")

    # ---- Compute country-level Gini means over window ----
    gini_country = country_gini_means(panel, GINI_WINDOW).rename("gini_country_mean")
    print(f"\nCountry-level Gini coverage ({GINI_WINDOW[0]}-{GINI_WINDOW[1]}):")
    print(f"  countries with at least one Gini observation: "
          f"{gini_country.notna().sum()}")

    # ---- Merge ----
    # Cluster assignments give us the 167 rows of interest with their cluster ids;
    # join raw features (drop overlapping metadata) and Gini onto that.
    feat_keep = [c for c in features.columns if c == "iso3" or c in FEATURE_COLS]
    working = clusters.merge(features[feat_keep], on="iso3", how="left")
    working = working.merge(
        gini_country.reset_index(), on="iso3", how="left"
    )
    print(f"  working frame                   : {working.shape}")
    assert len(working) == len(clusters), "merge changed row count"

    # ---- Per-cluster profile ----
    print(f"\n--- Cluster profiles (K=3 K-means primary) ---")
    profile_rows = []
    for cluster_id in sorted(working[PRIMARY_CLUSTER_COL].unique()):
        sub = working[working[PRIMARY_CLUSTER_COL] == cluster_id]
        profile = build_cluster_profile(sub)
        profile = {
            "cluster": int(cluster_id),
            "label_proposed": CLUSTER_LABELS.get(cluster_id, f"Cluster {cluster_id}"),
            **profile,
        }
        profile_rows.append(profile)
        print_cluster_narrative(int(cluster_id), profile)

    profiles_df = pd.DataFrame(profile_rows)

    # ---- Cross-cluster Gini summary ----
    print(f"\n=== Cross-cluster Gini summary ===")
    print(f"  (descriptive: mean of country-level Gini means over "
          f"{GINI_WINDOW[0]}-{GINI_WINDOW[1]})")
    print(f"  Cluster | n_w_gini |  mean | median |    IQR")
    for row in profile_rows:
        c = row["cluster"]
        gn = row["gini_n"]
        gm = row["gini_mean"]
        gmed = row["gini_p50"]
        g25 = row["gini_p25"]
        g75 = row["gini_p75"]
        if gn > 0:
            print(
                f"  {c:7d} | {gn:8d} | {gm:5.2f} |  {gmed:5.2f} | "
                f"[{g25:.2f}, {g75:.2f}]"
            )
        else:
            print(f"  {c:7d} | {gn:8d} | (no Gini observations)")

    # Identify which cluster has the highest mean Gini
    means = {row["cluster"]: row["gini_mean"] for row in profile_rows
             if not np.isnan(row["gini_mean"])}
    if means:
        max_c = max(means, key=means.get)
        print(f"\n  Highest mean Gini: Cluster {max_c} "
              f"(\"{CLUSTER_LABELS[max_c]}\") at {means[max_c]:.2f}")

    # ---- Save profiles CSV ----
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    profiles_df.to_csv(out_csv, index=False)
    print(f"\nWrote: {out_csv.relative_to(project_root)}")
    print(f"  shape: {profiles_df.shape}")
    print(f"  columns: {len(profiles_df.columns)}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
