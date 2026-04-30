"""
World Bank country metadata loaders.

The WB country metadata CSV at `data/raw/world_bank/wb_country_metadata.csv`
contains 296 entities: 217 real countries plus 79 aggregates (regional
and income-group rollups). Aggregates are identified by
`region_name == "Aggregates"`.

The CSV uses the literal string "NA" for the region_id and
income_level_id of aggregate entities. pandas's default behaviour
silently coerces "NA" to NaN, which masks the aggregate sentinel. This
caused a filter bug in Phase 01 Step 05 (documented in
phase01_summary.md). All readers in this module pass
`keep_default_na=False, na_values=[""]` to preserve the literal "NA"
strings.

The loaders also rename the source CSV's `country_iso3` column to
`iso3`, which is the canonical key name used across processed data
and the panel.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd


_AGGREGATE_REGION_NAME = "Aggregates"

# Columns most commonly needed downstream. After the country_iso3 -> iso3
# rename, these are the canonical column names.
_DEFAULT_KEEP_COLUMNS = [
    "iso3",
    "country_name",
    "region_name",
    "income_level_name",
]


def load_country_metadata(
    metadata_path: Path,
    *,
    drop_aggregates: bool = True,
    columns: list[str] | None = None,
) -> pd.DataFrame:
    """Read the WB country metadata CSV with proper "NA" handling.

    Reads the CSV with all values as strings, preserves the literal "NA"
    sentinel for aggregate regions/income groups, renames `country_iso3`
    to the canonical `iso3`, optionally drops aggregate entities, and
    optionally restricts to a column subset.

    Parameters
    ----------
    metadata_path : Path
        Path to wb_country_metadata.csv.
    drop_aggregates : bool, default True
        If True, exclude rows where region_name == "Aggregates".
    columns : list[str], optional
        Columns to keep, by their canonical (post-rename) names. If None,
        keeps a default set of (iso3, country_name, region_name,
        income_level_name). Pass an explicit list to override; pass an
        empty list ([]) to keep all columns.

    Returns
    -------
    pd.DataFrame
        Country metadata with reset index. All values are strings.
    """
    df = pd.read_csv(
        metadata_path,
        keep_default_na=False,   # critical: literal "NA" must stay as string
        na_values=[""],
        dtype=str,
    )
    df = df.rename(columns={"country_iso3": "iso3"})

    if drop_aggregates:
        df = df.loc[df["region_name"] != _AGGREGATE_REGION_NAME].copy()

    if columns is None:
        df = df[_DEFAULT_KEEP_COLUMNS]
    elif columns:  # explicit non-empty list
        df = df[columns]
    # else (empty list): keep all columns

    return df.reset_index(drop=True)


def get_real_country_iso3_set(metadata_path: Path) -> set[str]:
    """Return the set of iso3 codes for real countries (excludes aggregates).

    Convenience wrapper around `load_country_metadata` for the common
    case of "give me the canonical 217-country iso3 set".
    """
    df = load_country_metadata(
        metadata_path,
        drop_aggregates=True,
        columns=["iso3"],
    )
    return set(df["iso3"])
