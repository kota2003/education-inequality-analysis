"""
Phase 01 - Step 05: Inspect coverage of raw data sources.

Purpose:
    Survey the raw data collected in Steps 02 and 04. For every declared
    variable in the manifest, report country-year availability; produce a
    country × year availability matrix for a selection of key variables;
    and surface the ISO-3 overlap between UNDP HDR and the World Bank
    country list so Phase 02 knows the reconciliation surface area.

Inputs:
    data/raw/manifest.yaml
    data/raw/world_bank/wb_wdi.csv
    data/raw/world_bank/wb_country_metadata.csv
    data/raw/undp_hdr/hdr_composite_indices.csv

Outputs:
    outputs/tables/phase01_s05_coverage_summary.csv
    outputs/figures/phase01_s05_coverage_matrix.png

Notes:
    - No stochastic components, so no random seed is set.
    - For visualisation, entities are filtered to real countries (WB
      metadata region_id != "NA"). Aggregates are kept in the raw files;
      filtering here is view-time only and does not modify raw.
    - HDR CSV is read with encoding fallback (utf-8 → utf-8-sig → cp1252
      → latin-1). The raw file on disk is never rewritten.
"""

from __future__ import annotations

import sys
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
MANIFEST_PATH = PROJECT_ROOT / "data" / "raw" / "manifest.yaml"
WB_WDI_PATH = PROJECT_ROOT / "data" / "raw" / "world_bank" / "wb_wdi.csv"
WB_META_PATH = PROJECT_ROOT / "data" / "raw" / "world_bank" / "wb_country_metadata.csv"
HDR_PATH = PROJECT_ROOT / "data" / "raw" / "undp_hdr" / "hdr_composite_indices.csv"

TABLE_DIR = PROJECT_ROOT / "outputs" / "tables"
FIG_DIR = PROJECT_ROOT / "outputs" / "figures"
SUMMARY_OUT = TABLE_DIR / "phase01_s05_coverage_summary.csv"
MATRIX_OUT = FIG_DIR / "phase01_s05_coverage_matrix.png"

START_YEAR = 1990
END_YEAR = 2023
YEARS = list(range(START_YEAR, END_YEAR + 1))
N_YEARS = len(YEARS)

ENCODING_CANDIDATES = ("utf-8", "utf-8-sig", "cp1252", "latin-1")

# Variables to render as availability heatmaps. (declared_name, source).
VIS_VARS: list[tuple[str, str]] = [
    ("gini", "wb"),
    ("enrol_primary", "wb"),
    ("enrol_secondary", "wb"),
    ("enrol_tertiary", "wb"),
    ("mean_years_schooling", "hdr"),
    ("gdp_per_capita_ppp", "wb"),
]


# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------

def load_manifest() -> dict:
    with MANIFEST_PATH.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def read_hdr_csv() -> tuple[pd.DataFrame, str]:
    last_err: Exception | None = None
    for enc in ENCODING_CANDIDATES:
        try:
            return pd.read_csv(HDR_PATH, encoding=enc), enc
        except UnicodeDecodeError as exc:
            last_err = exc
    raise RuntimeError(f"Could not decode HDR CSV with any fallback — {last_err!r}")


# ---------------------------------------------------------------------------
# Per-variable long-format extraction
# ---------------------------------------------------------------------------

def wb_variable_long(wdi: pd.DataFrame, declared_name: str) -> pd.DataFrame:
    """
    Long-format slice of wb_wdi for one declared variable, restricted to rows
    with an ISO-3 code and a Scope-range year. Returns columns:
    country_iso3, year, value.
    """
    sub = wdi.loc[wdi["declared_name"] == declared_name, ["country_iso3", "year", "value"]].copy()
    sub = sub.dropna(subset=["country_iso3", "year"])
    sub["year"] = sub["year"].astype(int)
    sub = sub[(sub["year"] >= START_YEAR) & (sub["year"] <= END_YEAR)]
    return sub


def hdr_mys_long(hdr_df: pd.DataFrame) -> pd.DataFrame:
    """Melt mys_YYYY columns to long format. Returns country_iso3, year, value."""
    mys_cols = [
        c for c in hdr_df.columns
        if c.startswith("mys_") and c.split("_", 1)[1].isdigit()
    ]
    sub = hdr_df[["iso3"] + mys_cols].rename(columns={"iso3": "country_iso3"})
    long = sub.melt(id_vars=["country_iso3"], var_name="col", value_name="value")
    long["year"] = long["col"].str.split("_", n=1).str[1].astype(int)
    long = long[(long["year"] >= START_YEAR) & (long["year"] <= END_YEAR)]
    return long.dropna(subset=["country_iso3"])[["country_iso3", "year", "value"]]


# ---------------------------------------------------------------------------
# Coverage summary
# ---------------------------------------------------------------------------

def summarise_variable(
    long_df: pd.DataFrame, variable: str, source: str, country_set: set[str]
) -> dict:
    total_rows = len(long_df)
    non_null = long_df["value"].notna().sum()
    country_only = long_df[long_df["country_iso3"].isin(country_set)]
    non_null_countries = country_only["value"].notna().sum()
    uniq = country_only.loc[country_only["value"].notna(), "country_iso3"].nunique()
    years = country_only.loc[country_only["value"].notna(), "year"]
    y_min = int(years.min()) if len(years) else None
    y_max = int(years.max()) if len(years) else None
    full_cells = len(country_set) * N_YEARS
    completeness = (
        100.0 * non_null_countries / full_cells if full_cells else float("nan")
    )
    return {
        "variable": variable,
        "source": source,
        "raw_rows": int(total_rows),
        "non_null_all_entities": int(non_null),
        "non_null_countries_only": int(non_null_countries),
        "unique_countries_with_data": int(uniq),
        "year_min_country_only": y_min,
        "year_max_country_only": y_max,
        "country_year_completeness_pct": round(completeness, 2),
    }


# ---------------------------------------------------------------------------
# Availability matrix
# ---------------------------------------------------------------------------

def availability_matrix(
    long_df: pd.DataFrame, countries_ordered: list[str]
) -> np.ndarray:
    """Return a (n_countries × n_years) binary matrix. 1 = value present."""
    present = long_df.dropna(subset=["value"])
    # Pivot to country × year with any() aggregation
    present["has"] = 1
    pivot = (
        present.pivot_table(
            index="country_iso3", columns="year", values="has", aggfunc="max", fill_value=0
        )
        .reindex(index=countries_ordered, columns=YEARS, fill_value=0)
        .astype(int)
    )
    return pivot.to_numpy()


def plot_coverage_matrices(
    matrices: dict[str, np.ndarray],
    countries_ordered: list[str],
    out_path: Path,
) -> None:
    n = len(matrices)
    cols = 3
    rows = (n + cols - 1) // cols
    fig, axes = plt.subplots(rows, cols, figsize=(18, 5 * rows), constrained_layout=True)
    axes = np.atleast_2d(axes).ravel()

    for ax, (var_label, mat) in zip(axes, matrices.items()):
        total_cells = mat.size
        present = int(mat.sum())
        pct = 100.0 * present / total_cells if total_cells else 0.0
        ax.imshow(
            mat,
            aspect="auto",
            cmap="Greys",
            interpolation="nearest",
            vmin=0,
            vmax=1,
        )
        ax.set_title(
            f"{var_label}\n{present:,} / {total_cells:,} cells "
            f"({pct:.1f}%)",
            fontsize=11,
        )
        ax.set_xlabel("Year")
        ax.set_ylabel(f"Countries (n={len(countries_ordered)}, sorted by density)")
        # Year tick labels
        year_ticks = [0, 5, 10, 15, 20, 25, 30, N_YEARS - 1]
        year_ticks = [t for t in year_ticks if t < N_YEARS]
        ax.set_xticks(year_ticks)
        ax.set_xticklabels([str(YEARS[t]) for t in year_ticks], fontsize=9)
        ax.set_yticks([])

    # Blank any unused panels
    for ax in axes[len(matrices):]:
        ax.axis("off")

    fig.suptitle(
        "Country × Year availability — Phase 01 key variables "
        f"({START_YEAR}–{END_YEAR})",
        fontsize=14,
    )
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


# ---------------------------------------------------------------------------
# Reconciliation
# ---------------------------------------------------------------------------

def iso3_reconciliation(
    wb_country_set: set[str],
    hdr_country_set: set[str],
    meta: pd.DataFrame,
    hdr_df: pd.DataFrame,
) -> None:
    inter = wb_country_set & hdr_country_set
    wb_only = wb_country_set - hdr_country_set
    hdr_only = hdr_country_set - wb_country_set

    print("\n=== ISO-3 reconciliation: WB countries vs UNDP HDR ===")
    print(f"  WB countries             : {len(wb_country_set)}")
    print(f"  HDR iso3s                : {len(hdr_country_set)}")
    print(f"  Intersection             : {len(inter)}")
    print(f"  WB-only                  : {len(wb_only)}")
    print(f"  HDR-only                 : {len(hdr_only)}")

    if wb_only:
        wb_names = (
            meta.loc[meta["country_iso3"].isin(wb_only), ["country_iso3", "country_name"]]
            .sort_values("country_iso3")
        )
        print("\n  WB-only (first up to 20):")
        for _, row in wb_names.head(20).iterrows():
            print(f"    {row['country_iso3']:5s}  {row['country_name']}")

    if hdr_only:
        hdr_names = (
            hdr_df.loc[hdr_df["iso3"].isin(hdr_only), ["iso3", "country"]]
            .drop_duplicates()
            .sort_values("iso3")
        )
        print("\n  HDR-only (first up to 20):")
        for _, row in hdr_names.head(20).iterrows():
            print(f"    {row['iso3']:5s}  {row['country']}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    print(f"Loading manifest: {MANIFEST_PATH}")
    manifest = load_manifest()
    wb_indicators = manifest["sources"]["world_bank_wdi"]["indicators"]

    print("Loading WB country metadata ...")
    meta = pd.read_csv(WB_META_PATH)
    # Aggregates in WB metadata have region.id == "NA" (the literal string),
    # but pandas.read_csv converts the token "NA" to NaN by default. So
    # aggregates appear as NaN in the region_id column after loading. Filter
    # to non-null region_id to keep real countries only.
    country_mask = meta["region_id"].notna()
    wb_country_set: set[str] = set(meta.loc[country_mask, "country_iso3"].dropna().unique())
    n_wb_agg = int((~country_mask).sum())
    print(f"  WB real countries: {len(wb_country_set)} (excluded {n_wb_agg} aggregates)")

    print("Loading WB WDI long data ...")
    wdi = pd.read_csv(WB_WDI_PATH)
    print(f"  WDI rows: {len(wdi):,}")

    print("Loading UNDP HDR CSV ...")
    hdr_df, enc_used = read_hdr_csv()
    print(f"  HDR rows: {len(hdr_df):,}  (encoding used: {enc_used})")
    # HDR aggregates use iso3 codes prefixed "ZZ" (ZZA.VHHD = Very High HD,
    # ZZK.WORLD, ZZE.AS = Arab States, etc.). Exclude them for the country
    # comparison; they are still kept in the raw file.
    hdr_all_iso3 = hdr_df["iso3"].dropna().astype(str)
    hdr_agg_mask = hdr_all_iso3.str.startswith("ZZ")
    hdr_country_set: set[str] = set(hdr_all_iso3[~hdr_agg_mask].unique())
    print(
        f"  HDR real entities: {len(hdr_country_set)} "
        f"(excluded {int(hdr_agg_mask.sum())} ZZ* aggregates)"
    )

    TABLE_DIR.mkdir(parents=True, exist_ok=True)
    FIG_DIR.mkdir(parents=True, exist_ok=True)

    # ---- Per-variable coverage summary ----
    print("\nBuilding per-variable coverage summary ...")
    summary_rows: list[dict] = []

    for ind in wb_indicators:
        name = ind["name"]
        long = wb_variable_long(wdi, name)
        summary_rows.append(summarise_variable(long, name, "WB WDI", wb_country_set))

    mys_long = hdr_mys_long(hdr_df)
    summary_rows.append(
        summarise_variable(mys_long, "mean_years_schooling", "UNDP HDR", wb_country_set)
    )

    summary = pd.DataFrame(summary_rows)
    summary = summary.sort_values(
        "non_null_countries_only", ascending=False
    ).reset_index(drop=True)
    summary.to_csv(SUMMARY_OUT, index=False)
    print(f"Saved coverage summary: {SUMMARY_OUT}")

    # Printable view
    print("\nCoverage (countries only, sorted by non-null count):")
    print(
        summary[
            [
                "variable",
                "source",
                "non_null_countries_only",
                "unique_countries_with_data",
                "country_year_completeness_pct",
            ]
        ].to_string(index=False)
    )

    # ---- Availability matrices (visualised key variables) ----
    print("\nBuilding availability matrices for key variables ...")

    # Build long slices for VIS_VARS
    vis_longs: dict[str, pd.DataFrame] = {}
    for var_name, src in VIS_VARS:
        if src == "wb":
            vis_longs[var_name] = wb_variable_long(wdi, var_name)
        elif src == "hdr" and var_name == "mean_years_schooling":
            vis_longs[var_name] = mys_long
        else:
            raise ValueError(f"Unknown VIS_VARS entry: {var_name} / {src}")

    # Country ordering: by total presence across the 6 vis variables,
    # intersected with wb_country_set so only real countries appear.
    presence_counts: dict[str, int] = {iso: 0 for iso in wb_country_set}
    for long in vis_longs.values():
        sub = long.dropna(subset=["value"])
        sub = sub[sub["country_iso3"].isin(wb_country_set)]
        counts = sub.groupby("country_iso3").size()
        for iso, n in counts.items():
            presence_counts[iso] = presence_counts.get(iso, 0) + int(n)

    countries_ordered = sorted(
        wb_country_set, key=lambda c: (-presence_counts[c], c)
    )
    print(f"  Ordered {len(countries_ordered)} countries by total density across {len(VIS_VARS)} vars")

    matrices: dict[str, np.ndarray] = {}
    for var_name, _src in VIS_VARS:
        long = vis_longs[var_name]
        long_c = long[long["country_iso3"].isin(wb_country_set)]
        matrices[var_name] = availability_matrix(long_c, countries_ordered)

    plot_coverage_matrices(matrices, countries_ordered, MATRIX_OUT)
    print(f"Saved coverage matrix figure: {MATRIX_OUT}")

    # ---- ISO-3 reconciliation ----
    iso3_reconciliation(wb_country_set, hdr_country_set, meta, hdr_df)

    # ---- Summary verdict ----
    print("\n=== Step 05 summary ===")
    print(f"  Variables summarised : {len(summary)}")
    print(
        f"  Gini non-null (country-years)                : "
        f"{summary.loc[summary['variable'] == 'gini', 'non_null_countries_only'].iloc[0]:,}"
    )
    mys_row = summary[summary["variable"] == "mean_years_schooling"]
    if not mys_row.empty:
        print(
            f"  Mean years of schooling non-null              : "
            f"{mys_row['non_null_countries_only'].iloc[0]:,}"
        )
    print(f"  Coverage figure                             : {MATRIX_OUT.name}")
    print(f"  Coverage summary CSV                        : {SUMMARY_OUT.name}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
