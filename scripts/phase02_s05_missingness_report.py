"""
Phase 02 - Step 05: Missingness report on the integrated panel

Purpose:
    Characterise missingness in data/processed/panel.csv along two axes:
    per-variable (one row per variable describing where and when it is
    observed) and joint (how many country-year rows survive listwise
    deletion under candidate variable subsets). Output a CSV summary
    and a two-panel figure that visualises (A) overall coverage by
    variable and (B) Gini's country-year availability sorted by region
    -- Gini being the binding constraint at 30% completeness and the
    most informative single view of the panel's MNAR-flavoured
    missingness.

Inputs:
    data/processed/panel.csv
    data/raw/manifest.yaml

Outputs:
    outputs/tables/phase02_missingness_report.csv
    outputs/figures/phase02_missingness_matrix.png

Notes:
    Non-stochastic: no RNG seeds required.
    Joint specification statistics are printed to stdout only; Phase 02
    intentionally does not select model specifications -- that is
    Phase 05's job. The four named specifications below are
    illustrative bounds for the analytical sample.
"""

from __future__ import annotations

import sys
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import yaml


YEAR_MIN = 1990
YEAR_MAX = 2023
N_YEARS = YEAR_MAX - YEAR_MIN + 1  # 34

# Illustrative specifications (NOT final modelling choices -- those belong to Phase 05)
JOINT_SPECIFICATIONS: dict[str, list[str]] = {
    "all_19_variables": [],  # populated from manifest at runtime
    "core_education_economic": [
        "gini",
        "enrol_primary", "enrol_secondary", "enrol_tertiary",
        "gdp_per_capita_ppp", "population", "urban_population_pct",
    ],
    "minimal_with_mys": [
        "gini", "mean_years_schooling", "gdp_per_capita_ppp", "unemployment_rate",
    ],
    "no_gini_diagnostic": [
        "enrol_primary", "enrol_secondary", "enrol_tertiary",
        "gdp_per_capita_ppp", "population", "urban_population_pct",
    ],
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def find_project_root(start: Path) -> Path:
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
    wb_vars = [ind["name"] for ind in manifest["sources"]["world_bank_wdi"]["indicators"]]
    hdr_vars = [var["target_name"] for var in manifest["sources"]["undp_hdr"]["variables"]]
    return wb_vars + hdr_vars


# ---------------------------------------------------------------------------
# Per-variable stats
# ---------------------------------------------------------------------------


def per_variable_stats(panel: pd.DataFrame, variables: list[str]) -> pd.DataFrame:
    n_total = len(panel)
    rows = []
    for var in variables:
        col = panel[var]
        observed = col.notna()
        n_obs = int(observed.sum())

        # Per-country observation counts (max possible = N_YEARS = 34)
        per_country = panel.assign(_obs=observed).groupby("iso3")["_obs"].sum()
        n_countries_any = int((per_country > 0).sum())
        n_countries_full = int((per_country == N_YEARS).sum())

        observed_years = panel.loc[observed, "year"]
        year_min = int(observed_years.min()) if len(observed_years) else None
        year_max = int(observed_years.max()) if len(observed_years) else None

        rows.append({
            "variable": var,
            "n_observed": n_obs,
            "pct_observed": round(100 * n_obs / n_total, 2),
            "n_countries_any_obs": n_countries_any,
            "n_countries_full_coverage": n_countries_full,
            "first_observed_year": year_min,
            "last_observed_year": year_max,
        })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Joint specification stats
# ---------------------------------------------------------------------------


def joint_specification_stats(
    panel: pd.DataFrame,
    specs: dict[str, list[str]],
) -> pd.DataFrame:
    n_total = len(panel)
    rows = []
    for name, vars_ in specs.items():
        if not vars_:
            continue
        complete = panel[vars_].notna().all(axis=1)
        n_rows = int(complete.sum())
        n_countries = int(panel.loc[complete, "iso3"].nunique())
        years_covered = panel.loc[complete, "year"]
        rows.append({
            "specification": name,
            "n_variables": len(vars_),
            "n_complete_rows": n_rows,
            "pct_of_panel": round(100 * n_rows / n_total, 2),
            "n_countries_with_any_complete_row": n_countries,
            "year_min": int(years_covered.min()) if len(years_covered) else None,
            "year_max": int(years_covered.max()) if len(years_covered) else None,
        })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Figure
# ---------------------------------------------------------------------------


def make_figure(
    panel: pd.DataFrame,
    per_var_df: pd.DataFrame,
    out_path: Path,
) -> None:
    fig, (ax_a, ax_b) = plt.subplots(
        2, 1,
        figsize=(12, 14),
        gridspec_kw={"height_ratios": [1, 3]},
    )

    # ---- Panel A: per-variable coverage bars ----
    df_sorted = per_var_df.sort_values("pct_observed", ascending=True).reset_index(drop=True)
    bars = ax_a.barh(df_sorted["variable"], df_sorted["pct_observed"], color="steelblue")
    ax_a.set_xlim(0, 105)
    ax_a.set_xlabel("% observed (of 7,378 country-year cells)")
    ax_a.set_title("A. Per-variable coverage on the integrated panel")
    ax_a.axvline(100, color="black", linewidth=0.5, linestyle=":")
    for i, v in enumerate(df_sorted["pct_observed"]):
        ax_a.text(v + 1, i, f"{v:.1f}%", va="center", fontsize=8)
    ax_a.grid(axis="x", linestyle=":", alpha=0.5)

    # ---- Panel B: Gini availability heatmap, countries sorted by region ----
    order_df = (
        panel.drop_duplicates("iso3")
        .sort_values(["region_name", "iso3"])
        .reset_index(drop=True)
    )
    ordered_iso3 = order_df["iso3"].tolist()

    matrix = (
        panel.pivot(index="iso3", columns="year", values="gini")
        .notna()
        .astype(int)
        .reindex(ordered_iso3)
    )

    ax_b.imshow(matrix.values, aspect="auto", cmap="binary", vmin=0, vmax=1, interpolation="nearest")
    ax_b.set_xticks(range(0, N_YEARS, 5))
    ax_b.set_xticklabels(range(YEAR_MIN, YEAR_MAX + 1, 5))
    ax_b.set_xlabel("Year")
    ax_b.set_ylabel("Country (sorted by region)")
    ax_b.set_title("B. Gini availability by country and year (black = observed, white = missing)")
    ax_b.set_yticks([])  # 217 ticks would be unreadable

    # Region boundaries + labels
    region_starts = order_df[
        order_df["region_name"].ne(order_df["region_name"].shift())
    ].index.tolist()
    region_starts_with_end = region_starts + [len(order_df)]

    for boundary in region_starts[1:]:
        ax_b.axhline(boundary - 0.5, color="red", linewidth=0.6, alpha=0.7)

    for i, start in enumerate(region_starts_with_end[:-1]):
        end = region_starts_with_end[i + 1]
        midpoint = (start + end) / 2
        region = order_df.iloc[start]["region_name"]
        n_in_region = end - start
        ax_b.text(
            -0.5, midpoint,
            f"{region.strip()} (n={n_in_region})",
            va="center", ha="right",
            fontsize=8,
        )

    fig.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> int:
    script_path = Path(__file__).resolve()
    project_root = find_project_root(script_path.parent)
    print(f"[phase02_s05] project root: {project_root}")

    panel_path = project_root / "data/processed/panel.csv"
    manifest_path = project_root / "data/raw/manifest.yaml"
    if not panel_path.exists():
        raise FileNotFoundError(f"Missing input: {panel_path}")
    if not manifest_path.exists():
        raise FileNotFoundError(f"Missing input: {manifest_path}")

    tables_dir = project_root / "outputs/tables"
    figures_dir = project_root / "outputs/figures"
    tables_dir.mkdir(parents=True, exist_ok=True)
    figures_dir.mkdir(parents=True, exist_ok=True)
    csv_out = tables_dir / "phase02_missingness_report.csv"
    fig_out = figures_dir / "phase02_missingness_matrix.png"

    panel = pd.read_csv(panel_path)
    print(f"[panel]       read: {len(panel):,} rows, {len(panel.columns)} cols")

    manifest = load_manifest(manifest_path)
    var_order = manifest_variable_order(manifest)
    print(f"[manifest]    variables: {len(var_order)}")
    print()

    # ---- Per-variable ----
    per_var = per_variable_stats(panel, var_order)
    per_var.to_csv(csv_out, index=False)
    print(f"[per-var]     wrote {csv_out.relative_to(project_root)} "
          f"({len(per_var)} rows, {len(per_var.columns)} cols)")
    print()
    print("[per-var]     summary (sorted by pct_observed desc):")
    print(per_var.sort_values("pct_observed", ascending=False).to_string(index=False))

    # ---- Joint ----
    JOINT_SPECIFICATIONS["all_19_variables"] = var_order
    joint = joint_specification_stats(panel, JOINT_SPECIFICATIONS)
    print()
    print("[joint]       listwise-survivor sample sizes for illustrative specifications:")
    print(joint.to_string(index=False))
    print()
    print("[joint]       (these specs are illustrative bounds; Phase 05 selects the real ones)")

    # ---- Figure ----
    make_figure(panel, per_var, fig_out)
    print()
    print(f"[figure]      wrote {fig_out.relative_to(project_root)}")
    print()
    print("[phase02_s05] done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
