"""
Phase 02 - Step 03: Concatenate WB and HDR intermediates into master long panel

Purpose:
    Combine data/processed/wb_long.csv and data/processed/hdr_long.csv
    into a single master long-format panel covering all 19 declared
    variables (18 from WB WDI + 1 from UNDP HDR). Validate the result
    on five axes (uniqueness, completeness of declared_names, year
    range, iso3 subset relation, no nulls in key/value columns) and
    write data/processed/panel_long.csv as the auditable input to
    Step 04 (pivot to wide).

Inputs:
    data/processed/wb_long.csv
    data/processed/hdr_long.csv
    data/raw/manifest.yaml                       (declared-name expectations)
    data/raw/world_bank/wb_country_metadata.csv  (217 WB country iso3 set)

Outputs:
    data/processed/panel_long.csv

Notes:
    Non-stochastic: no RNG seeds required.
    The two intermediates already share the schema
        iso3, year, indicator_code, declared_name, value
    so concatenation is the right operation; no key-merge needed.
    Source attribution is preserved implicitly via `indicator_code`
    (HDR uses "mys"; WB uses dotted codes like "SI.POV.GINI").
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import yaml


YEAR_MIN = 1990
YEAR_MAX = 2023


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


def expected_all_declared(manifest: dict) -> set[str]:
    wb = {ind["name"] for ind in manifest["sources"]["world_bank_wdi"]["indicators"]}
    hdr = {var["target_name"] for var in manifest["sources"]["undp_hdr"]["variables"]}
    return wb | hdr


def load_wb_country_iso3_set(metadata_path: Path) -> set[str]:
    df = pd.read_csv(
        metadata_path,
        keep_default_na=False,   # literal "NA" in region_id must stay as string
        na_values=[""],
        dtype=str,
    )
    return set(df.loc[df["region_name"] != "Aggregates", "country_iso3"])


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> int:
    script_path = Path(__file__).resolve()
    project_root = find_project_root(script_path.parent)
    print(f"[phase02_s03] project root: {project_root}")

    # Inputs
    wb_path = project_root / "data/processed/wb_long.csv"
    hdr_path = project_root / "data/processed/hdr_long.csv"
    manifest_path = project_root / "data/raw/manifest.yaml"
    metadata_path = project_root / "data/raw/world_bank/wb_country_metadata.csv"

    for p in (wb_path, hdr_path, manifest_path, metadata_path):
        if not p.exists():
            raise FileNotFoundError(f"Missing required input: {p}")

    out_path = project_root / "data/processed/panel_long.csv"

    # Manifest expectations
    manifest = load_manifest(manifest_path)
    expected_names = expected_all_declared(manifest)
    print(f"[manifest]    expected declared_names: {len(expected_names)}")
    print()

    # ---- Read intermediates ----
    wb_long = pd.read_csv(wb_path)
    hdr_long = pd.read_csv(hdr_path)
    print(
        f"[wb_long]     {len(wb_long):,} rows | "
        f"iso3={wb_long['iso3'].nunique()} | "
        f"declared={wb_long['declared_name'].nunique()}"
    )
    print(
        f"[hdr_long]    {len(hdr_long):,} rows | "
        f"iso3={hdr_long['iso3'].nunique()} | "
        f"declared={hdr_long['declared_name'].nunique()}"
    )

    # ---- Concatenate ----
    panel_long = pd.concat([wb_long, hdr_long], ignore_index=True)
    print(
        f"[concat]      {len(panel_long):,} rows | "
        f"iso3={panel_long['iso3'].nunique()} | "
        f"declared={panel_long['declared_name'].nunique()}"
    )
    print()

    # ---- Validation 1: uniqueness on (iso3, year, declared_name) ----
    dup = int(panel_long.duplicated(["iso3", "year", "declared_name"]).sum())
    if dup:
        raise ValueError(
            f"panel_long has {dup} duplicate (iso3, year, declared_name) rows"
        )
    print(f"[validate 1]  uniqueness OK: 0 duplicate (iso3, year, declared_name)")

    # ---- Validation 2: all expected declared_names present, no extras ----
    actual_names = set(panel_long["declared_name"].unique())
    missing = expected_names - actual_names
    extra = actual_names - expected_names
    if missing:
        raise ValueError(f"missing declared_names: {sorted(missing)}")
    if extra:
        raise ValueError(f"unexpected declared_names: {sorted(extra)}")
    print(
        f"[validate 2]  all {len(expected_names)} declared_names present, "
        f"no extras"
    )

    # ---- Validation 3: year range within [1990, 2023] ----
    yr_min = int(panel_long["year"].min())
    yr_max = int(panel_long["year"].max())
    if yr_min < YEAR_MIN or yr_max > YEAR_MAX:
        raise ValueError(
            f"year out of range: observed [{yr_min}, {yr_max}], "
            f"expected within [{YEAR_MIN}, {YEAR_MAX}]"
        )
    print(f"[validate 3]  year range OK: {yr_min}-{yr_max}")

    # ---- Validation 4: HDR iso3 ⊆ WB country set ----
    wb_country_set = load_wb_country_iso3_set(metadata_path)
    wb_long_iso3 = set(wb_long["iso3"].unique())
    hdr_iso3 = set(hdr_long["iso3"].unique())

    if wb_long_iso3 != wb_country_set:
        diff = wb_long_iso3.symmetric_difference(wb_country_set)
        raise ValueError(
            f"WB long iso3 set differs from country metadata set: "
            f"sym_diff={sorted(diff)[:10]}..."
        )

    hdr_not_in_wb = hdr_iso3 - wb_country_set
    if hdr_not_in_wb:
        raise ValueError(
            f"HDR contains iso3 not in WB country set: {sorted(hdr_not_in_wb)}"
        )
    print(
        f"[validate 4]  iso3 sets:  WB country meta={len(wb_country_set)} | "
        f"WB long={len(wb_long_iso3)} | HDR long={len(hdr_iso3)}"
    )
    print(f"[validate 4]  HDR iso3 ⊆ WB country set: OK")

    wb_only = wb_country_set - hdr_iso3
    print(
        f"[validate 4]  WB countries with no HDR mys: {len(wb_only)} "
        f"(Phase 01 expected 22; +2 if HDR all-NaN-mys countries are present)"
    )

    # ---- Validation 5: no nulls in key columns or value ----
    for col in ("iso3", "year", "declared_name", "value"):
        n_null = int(panel_long[col].isna().sum())
        if n_null:
            raise ValueError(f"column {col!r} has {n_null} null values")
    print(f"[validate 5]  no nulls in iso3 / year / declared_name / value")

    # ---- Diagnostics ----
    print()
    print("[summary]     rows per declared_name (sorted desc):")
    by_name = panel_long.groupby("declared_name").size().sort_values(ascending=False)
    for name, n in by_name.items():
        print(f"                {name:<32s} {n:>7,}")

    # Year coverage range per source (informative — not a validation failure)
    print()
    print("[summary]     rows per year (every 5 years):")
    by_year = panel_long.groupby("year").size()
    for yr in range(YEAR_MIN, YEAR_MAX + 1, 5):
        if yr in by_year.index:
            print(f"                {yr}: {by_year[yr]:>7,}")
    if YEAR_MAX in by_year.index and YEAR_MAX % 5 != 0:
        print(f"                {YEAR_MAX}: {by_year[YEAR_MAX]:>7,}")

    # ---- Write ----
    panel_long.to_csv(out_path, index=False)
    print()
    print(
        f"[phase02_s03] wrote {out_path.relative_to(project_root)} "
        f"({len(panel_long):,} rows, {len(panel_long.columns)} cols)"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
