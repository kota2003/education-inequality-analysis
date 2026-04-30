"""
Phase 02 - Step 02: Build intermediate long-format files from WB and HDR raw layers

Purpose:
    Produce two reviewable intermediate files that are the inputs for the
    Step 03 merge:

      - data/processed/wb_long.csv  : WB WDI restricted to real countries
                                       and to the 18 declared indicators,
                                       with `declared_name` already attached.
      - data/processed/hdr_long.csv : HDR `mys` reshaped to long format,
                                       with HDR aggregates (ZZ*) removed.

    Both files share the same schema:
        iso3, year, indicator_code, declared_name, value

    so that Step 03 can concatenate them and pivot in a single operation.

Inputs:
    data/raw/manifest.yaml
    data/raw/world_bank/wb_country_metadata.csv
    data/raw/world_bank/wb_wdi.csv
    data/raw/undp_hdr/hdr_composite_indices.csv

Outputs:
    data/processed/wb_long.csv
    data/processed/hdr_long.csv

Notes:
    Non-stochastic: no RNG seeds required.
    HDR is read via src.io_utils.read_csv_with_encoding_fallback. That helper
    returns a tuple (df, encoding) for traceability; this script unpacks
    defensively to tolerate either tuple ordering.
    The WB country metadata CSV uses the literal string "NA" for aggregate
    region/income IDs, so it must be read with keep_default_na=False to
    avoid pandas silently coercing those strings to NaN (see Phase 01
    Step 05 incident).
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import pandas as pd
import yaml


HDR_AGGREGATE_PREFIX = "ZZ"


# ---------------------------------------------------------------------------
# Path / manifest helpers
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


def expected_wb_declared_names(manifest: dict) -> set[str]:
    return {
        ind["name"]
        for ind in manifest["sources"]["world_bank_wdi"]["indicators"]
    }


def expected_hdr_target(manifest: dict) -> tuple[str, str]:
    """Return (source_name, target_name) for the single HDR variable."""
    var = manifest["sources"]["undp_hdr"]["variables"][0]
    return var["source_name"], var["target_name"]


def unwrap_csv_result(result) -> tuple[pd.DataFrame, object | None]:
    """
    Tolerate any of these return shapes from read_csv_with_encoding_fallback:
        - DataFrame
        - (DataFrame, encoding_str)
        - (encoding_str, DataFrame)
    Returns (df, encoding_or_None).
    """
    if isinstance(result, pd.DataFrame):
        return result, None
    if isinstance(result, tuple):
        dfs = [x for x in result if isinstance(x, pd.DataFrame)]
        others = [x for x in result if not isinstance(x, pd.DataFrame)]
        if not dfs:
            raise TypeError(
                "read_csv_with_encoding_fallback returned a tuple with no "
                f"DataFrame: types={[type(x).__name__ for x in result]}"
            )
        return dfs[0], (others[0] if others else None)
    raise TypeError(
        "read_csv_with_encoding_fallback returned unexpected type: "
        f"{type(result).__name__}"
    )


# ---------------------------------------------------------------------------
# WB long
# ---------------------------------------------------------------------------


def load_country_iso3_set(metadata_path: Path) -> set[str]:
    """Read WB country metadata, return iso3 set for real countries (excludes aggregates)."""
    df = pd.read_csv(
        metadata_path,
        keep_default_na=False,   # critical: literal "NA" must stay as string
        na_values=[""],
        dtype=str,
    )
    print(f"[wb_meta] read: {len(df)} rows, {len(df.columns)} cols")

    is_aggregate = df["region_name"].eq("Aggregates")
    n_agg = int(is_aggregate.sum())
    countries = df.loc[~is_aggregate, "country_iso3"].tolist()
    print(
        f"[wb_meta] aggregates dropped: {n_agg}; "
        f"real countries kept: {len(countries)}"
    )
    return set(countries)


def build_wb_long(
    wdi_path: Path,
    country_set: set[str],
    expected_names: set[str],
) -> pd.DataFrame:
    df = pd.read_csv(wdi_path)
    print(f"[wb_wdi]  read: {len(df):,} rows, {len(df.columns)} cols")
    n_initial = len(df)

    df = df[df["country_iso3"].isin(country_set)].copy()
    print(f"[wb_wdi]  country filter:  {n_initial:,} -> {len(df):,} rows")

    n_pre_dropna = len(df)
    df = df.dropna(subset=["value"]).copy()
    print(f"[wb_wdi]  drop null value: {n_pre_dropna:,} -> {len(df):,} rows")

    out = (
        df[["country_iso3", "year", "indicator_code", "declared_name", "value"]]
        .rename(columns={"country_iso3": "iso3"})
        .reset_index(drop=True)
    )
    out["year"] = out["year"].astype(int)

    dup = int(out.duplicated(["iso3", "year", "declared_name"]).sum())
    if dup:
        raise ValueError(
            f"WB long has {dup} duplicate (iso3, year, declared_name) rows"
        )

    actual_names = set(out["declared_name"].unique())
    missing = expected_names - actual_names
    extra = actual_names - expected_names
    if missing:
        raise ValueError(f"WB long missing declared_names: {sorted(missing)}")
    if extra:
        raise ValueError(f"WB long has unexpected declared_names: {sorted(extra)}")

    print(f"[wb_wdi]  unique iso3:        {out['iso3'].nunique()}")
    print(f"[wb_wdi]  unique declared:    {len(actual_names)} (expected {len(expected_names)})")
    print(f"[wb_wdi]  year range:         {out['year'].min()}-{out['year'].max()}")

    print("[wb_wdi]  rows per indicator:")
    by_ind = out.groupby("declared_name").size().sort_values(ascending=False)
    for name, n in by_ind.items():
        print(f"             {name:<32s} {n:>7,}")

    return out


# ---------------------------------------------------------------------------
# HDR long
# ---------------------------------------------------------------------------


def build_hdr_long(
    hdr_path: Path,
    project_root: Path,
    source_name: str,
    target_name: str,
) -> pd.DataFrame:
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    from src.io_utils import read_csv_with_encoding_fallback

    raw_result = read_csv_with_encoding_fallback(hdr_path)
    df, encoding_used = unwrap_csv_result(raw_result)
    enc_msg = f" (encoding: {encoding_used})" if encoding_used else ""
    print(f"[hdr]     read: {len(df)} rows, {len(df.columns)} cols{enc_msg}")

    year_pattern = re.compile(rf"^{re.escape(source_name)}_(\d{{4}})$")
    year_cols = [c for c in df.columns if year_pattern.match(c)]
    if not year_cols:
        raise ValueError(
            f"No columns matching {source_name}_YYYY in HDR file. "
            f"First 30 columns: {list(df.columns)[:30]}"
        )
    print(
        f"[hdr]     year cols: {len(year_cols)} "
        f"({year_cols[0]} ... {year_cols[-1]})"
    )

    long = df.melt(
        id_vars=["iso3"],
        value_vars=year_cols,
        var_name="_year_col",
        value_name="value",
    )
    n_initial = len(long)
    long["year"] = long["_year_col"].str.replace(
        f"{source_name}_", "", regex=False
    ).astype(int)
    long = long.drop(columns=["_year_col"])

    n_pre_dropna = len(long)
    long = long.dropna(subset=["value"]).copy()
    print(
        f"[hdr]     melt: {n_initial:,} cells -> "
        f"non-null: {n_pre_dropna:,} -> {len(long):,} rows"
    )

    is_agg = long["iso3"].str.startswith(HDR_AGGREGATE_PREFIX)
    n_agg = int(is_agg.sum())
    n_pre_agg = len(long)
    long = long[~is_agg].copy()
    print(f"[hdr]     drop ZZ* aggregates: {n_pre_agg:,} -> {len(long):,} rows ({n_agg} dropped)")

    long["indicator_code"] = source_name
    long["declared_name"] = target_name

    out = long[["iso3", "year", "indicator_code", "declared_name", "value"]].reset_index(drop=True)

    dup = int(out.duplicated(["iso3", "year"]).sum())
    if dup:
        raise ValueError(f"HDR long has {dup} duplicate (iso3, year) rows")

    if not (out["declared_name"] == target_name).all():
        raise ValueError("HDR long contains rows with unexpected declared_name")

    print(f"[hdr]     unique iso3:    {out['iso3'].nunique()}")
    print(f"[hdr]     year range:     {out['year'].min()}-{out['year'].max()}")

    return out


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> int:
    script_path = Path(__file__).resolve()
    project_root = find_project_root(script_path.parent)
    print(f"[phase02_s02] project root: {project_root}")

    manifest_path = project_root / "data/raw/manifest.yaml"
    metadata_path = project_root / "data/raw/world_bank/wb_country_metadata.csv"
    wdi_path = project_root / "data/raw/world_bank/wb_wdi.csv"
    hdr_path = project_root / "data/raw/undp_hdr/hdr_composite_indices.csv"

    for p in (manifest_path, metadata_path, wdi_path, hdr_path):
        if not p.exists():
            raise FileNotFoundError(f"Missing required input: {p}")

    out_dir = project_root / "data/processed"
    out_dir.mkdir(parents=True, exist_ok=True)
    wb_out = out_dir / "wb_long.csv"
    hdr_out = out_dir / "hdr_long.csv"

    manifest = load_manifest(manifest_path)
    expected_wb = expected_wb_declared_names(manifest)
    hdr_source, hdr_target = expected_hdr_target(manifest)
    print(f"[manifest]    WB declared indicators: {len(expected_wb)}")
    print(f"[manifest]    HDR variable: {hdr_source} -> {hdr_target}")
    print()

    print("=== WB WDI ===")
    country_set = load_country_iso3_set(metadata_path)
    wb_long = build_wb_long(wdi_path, country_set, expected_wb)
    wb_long.to_csv(wb_out, index=False)
    print(
        f"[wb_wdi]  wrote {wb_out.relative_to(project_root)} "
        f"({len(wb_long):,} rows, {len(wb_long.columns)} cols)"
    )
    print()

    print("=== UNDP HDR ===")
    hdr_long = build_hdr_long(hdr_path, project_root, hdr_source, hdr_target)
    hdr_long.to_csv(hdr_out, index=False)
    print(
        f"[hdr]     wrote {hdr_out.relative_to(project_root)} "
        f"({len(hdr_long):,} rows, {len(hdr_long.columns)} cols)"
    )
    print()

    print("[phase02_s02] done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
