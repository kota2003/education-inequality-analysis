"""
Phase 02 - Step 04: Pivot master long to wide panel and attach country metadata

Purpose:
    Convert data/processed/panel_long.csv from long format (one row per
    iso3 × year × declared_name) into the analytical wide panel
    data/processed/panel.csv (one row per iso3 × year, one column per
    declared variable). The wide panel is reindexed onto the complete
    Cartesian grid of 217 WB countries × 34 years (1990-2023) so that
    missing observations are explicit NaN rows rather than missing
    rows. Country metadata (country_name, region_name,
    income_level_name) is attached for use in Phase 04 clustering and
    Phase 05 heterogeneity analysis.

Inputs:
    data/processed/panel_long.csv
    data/raw/manifest.yaml                       (variable order)
    data/raw/world_bank/wb_country_metadata.csv  (country metadata)

Outputs:
    data/processed/panel.csv

Notes:
    Non-stochastic: no RNG seeds required.
    Uses pd.pivot (not pd.pivot_table) to fail loudly on duplicate
    (iso3, year, declared_name) keys; uniqueness was validated in
    Step 03 but the strict pivot guards against future regressions.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import yaml


YEAR_MIN = 1990
YEAR_MAX = 2023

METADATA_COLUMNS = ["country_name", "region_name", "income_level_name"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def find_project_root(start: Path) -> Path:
    """Walk upward from `start` until a directory containing PROJECT_LOG.md is found."""
    for candidate in [start, *start.parents]:
        if (candidate / "PROJECT_LOG.md").exists():
            return candidate
    raise FileNotFoundError(
        "Could not locate PROJECT_LOG.md by walking up from "
        f"{start}. Run this script from inside the project tree."
    )


def load_manifest(path: Path) -> dict:
    with path.open(encoding="utf-8") as f:
        return yaml.safe_load(f)


def manifest_variable_order(manifest: dict) -> list[str]:
    """Return declared variable names in manifest declaration order (WB then HDR)."""
    wb_vars = [ind["name"] for ind in manifest["sources"]["world_bank_wdi"]["indicators"]]
    hdr_vars = [var["target_name"] for var in manifest["sources"]["undp_hdr"]["variables"]]
    return wb_vars + hdr_vars


def load_country_metadata(metadata_path: Path) -> pd.DataFrame:
    """Read WB country metadata, return real-country rows with the columns we need."""
    df = pd.read_csv(
        metadata_path,
        keep_default_na=False,   # literal "NA" must stay as string
        na_values=[""],
        dtype=str,
    )
    df = df.loc[df["region_name"] != "Aggregates"].copy()
    df = df.rename(columns={"country_iso3": "iso3"})
    keep = ["iso3", "country_name", "region_name", "income_level_name"]
    return df[keep].reset_index(drop=True)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> int:
    script_path = Path(__file__).resolve()
    project_root = find_project_root(script_path.parent)
    print(f"[phase02_s04] project root: {project_root}")

    panel_long_path = project_root / "data/processed/panel_long.csv"
    manifest_path = project_root / "data/raw/manifest.yaml"
    metadata_path = project_root / "data/raw/world_bank/wb_country_metadata.csv"
    out_path = project_root / "data/processed/panel.csv"

    for p in (panel_long_path, manifest_path, metadata_path):
        if not p.exists():
            raise FileNotFoundError(f"Missing required input: {p}")

    # ---- Load ----
    panel_long = pd.read_csv(panel_long_path)
    print(f"[panel_long]  read: {len(panel_long):,} rows, {len(panel_long.columns)} cols")

    manifest = load_manifest(manifest_path)
    var_order = manifest_variable_order(manifest)
    print(f"[manifest]    variables (declaration order): {len(var_order)}")

    metadata_df = load_country_metadata(metadata_path)
    print(f"[metadata]    countries: {len(metadata_df)} (with {len(METADATA_COLUMNS)} attribute columns)")
    print()

    # ---- Pivot long -> wide ----
    # Strict pivot fails on duplicate keys. Step 03 validated uniqueness;
    # this guards against future regressions if intermediates are regenerated.
    wide = panel_long.pivot(
        index=["iso3", "year"],
        columns="declared_name",
        values="value",
    )
    wide = wide.rename_axis(None, axis=1)
    print(f"[pivot]       shape: {wide.shape} (rows = sparse iso3×year combos before reindex)")

    # ---- Reindex onto complete cartesian grid ----
    iso3_list = sorted(metadata_df["iso3"].tolist())
    years = list(range(YEAR_MIN, YEAR_MAX + 1))
    grid = pd.MultiIndex.from_product([iso3_list, years], names=["iso3", "year"])
    wide = wide.reindex(grid)
    print(
        f"[reindex]     shape: {wide.shape} "
        f"(target: {len(iso3_list)} × {len(years)} = {len(iso3_list) * len(years):,})"
    )

    # ---- Order variable columns by manifest order ----
    missing_in_pivot = [v for v in var_order if v not in wide.columns]
    if missing_in_pivot:
        raise ValueError(f"variables in manifest but not in pivot: {missing_in_pivot}")
    extra_in_pivot = [c for c in wide.columns if c not in var_order]
    if extra_in_pivot:
        raise ValueError(f"variables in pivot but not in manifest: {extra_in_pivot}")
    wide = wide[var_order]
    print(f"[order]       columns reordered to manifest declaration order")

    # ---- Attach metadata ----
    wide = wide.reset_index()
    panel = wide.merge(metadata_df, on="iso3", how="left", validate="many_to_one")
    print(f"[merge]       attached metadata: {panel.shape}")

    # ---- Final column order ----
    final_cols = ["iso3", "year"] + METADATA_COLUMNS + var_order
    panel = panel[final_cols]
    panel = panel.sort_values(["iso3", "year"]).reset_index(drop=True)

    # ---- Validation ----
    expected_rows = len(iso3_list) * len(years)
    if len(panel) != expected_rows:
        raise ValueError(
            f"row count mismatch: got {len(panel):,}, expected {expected_rows:,}"
        )
    print(f"[validate]    row count OK: {len(panel):,}")

    if panel["iso3"].nunique() != len(iso3_list):
        raise ValueError(
            f"unique iso3 mismatch: got {panel['iso3'].nunique()}, "
            f"expected {len(iso3_list)}"
        )
    if panel["year"].nunique() != len(years):
        raise ValueError(
            f"unique years mismatch: got {panel['year'].nunique()}, "
            f"expected {len(years)}"
        )
    print(
        f"[validate]    unique iso3: {panel['iso3'].nunique()}, "
        f"unique years: {panel['year'].nunique()}, "
        f"year range: {int(panel['year'].min())}-{int(panel['year'].max())}"
    )

    # All metadata columns must be fully populated (every iso3 has metadata)
    for col in METADATA_COLUMNS:
        n_null = int(panel[col].isna().sum())
        if n_null:
            raise ValueError(f"metadata column {col!r} has {n_null} nulls")
    print(f"[validate]    metadata columns: no nulls")

    # Cross-check non-null counts of variables against panel_long
    long_counts = panel_long.groupby("declared_name").size().to_dict()
    mismatches = []
    for var in var_order:
        wide_n = int(panel[var].notna().sum())
        long_n = long_counts.get(var, 0)
        if wide_n != long_n:
            mismatches.append((var, wide_n, long_n))
    if mismatches:
        raise ValueError(
            f"non-null count mismatches between wide and long: {mismatches}"
        )
    print(f"[validate]    non-null counts match panel_long for all {len(var_order)} variables")

    # ---- Diagnostics ----
    print()
    print("[summary]     non-null count per variable:")
    for var in var_order:
        n = int(panel[var].notna().sum())
        pct = 100 * n / len(panel)
        print(f"                {var:<32s} {n:>5,}  ({pct:>4.1f}%)")

    print()
    print("[summary]     panel head (first 3 countries × 3 years):")
    preview_cols = ["iso3", "year", "country_name", "region_name", "gini", "mean_years_schooling"]
    print(
        panel[preview_cols]
        .head(9)
        .to_string(index=False)
    )

    # ---- Write ----
    panel.to_csv(out_path, index=False)
    print()
    print(
        f"[phase02_s04] wrote {out_path.relative_to(project_root)} "
        f"({len(panel):,} rows, {len(panel.columns)} cols)"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
