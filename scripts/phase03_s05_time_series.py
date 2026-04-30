"""
Phase 03 - Step 05: Time-series of gini, mean_years_schooling, enrol_secondary
                    by region and by income group.

Purpose:
    Two figures, each with three subplots (one per variable: gini,
    mean_years_schooling, enrol_secondary):
      - phase03_s05_timeseries_by_region.png
      - phase03_s05_timeseries_by_income.png
    Each subplot shows year on x and the variable on y, faceted by stratum.
    For strata with N_countries >= 5 the cross-country mean is drawn as a
    line with an IQR (25-75 percentile) band. For strata with N_countries
    < 5 (e.g. North America at n=3) individual country lines are drawn
    instead per Step 01 Decision 5.

Inputs:
    data/processed/panel.csv

Outputs:
    outputs/figures/phase03_s05_timeseries_by_region.png
    outputs/figures/phase03_s05_timeseries_by_income.png
"""

from __future__ import annotations

import sys
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.paths import find_project_root  # noqa: E402

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

# Three variables to visualise per Step 05 spec.
TS_VARIABLES: tuple[str, ...] = ("gini", "mean_years_schooling", "enrol_secondary")

# Strata threshold per Step 01 Decision 5: below this draw individual lines.
SMALL_STRATUM_THRESHOLD = 5

# Income-group display order (Low -> High) for sensible legend ordering.
INCOME_DISPLAY_ORDER: tuple[str, ...] = (
    "Low income",
    "Lower middle income",
    "Upper middle income",
    "High income",
)

FIG_DPI = 300
FIG_W_PER_PANEL = 5.4
FIG_H_PER_PANEL = 4.0


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def stratum_label(name: str) -> str:
    """Strip stray whitespace introduced upstream (e.g. WB region names)."""
    return name.strip() if isinstance(name, str) else str(name)


def aggregate_band(
    sub: pd.DataFrame,
    var: str,
) -> pd.DataFrame:
    """Compute per-year mean, q25, q75, n_observed for a stratum slice.

    Returns a dataframe indexed by year with columns:
      mean, q25, q75, n_observed
    Years where n_observed == 0 will have NaN for mean/q25/q75 (line breaks).
    """
    g = sub.groupby("year")[var]
    agg = g.agg(["mean", "count",
                 lambda s: s.quantile(0.25),
                 lambda s: s.quantile(0.75)])
    agg.columns = ["mean", "n_observed", "q25", "q75"]
    return agg


def draw_panel(
    ax: plt.Axes,
    panel: pd.DataFrame,
    var: str,
    facet_col: str,
    facet_order: list[str],
    palette: dict[str, tuple],
    facet_country_counts: dict[str, int],
) -> tuple[int, int]:
    """Draw one (variable, facet column) subplot. Return (min_n, max_n) of
    observed-N across years and strata for title annotation."""
    overall_min_n = float("inf")
    overall_max_n = -1

    for stratum in facet_order:
        sub = panel.loc[panel[facet_col] == stratum, ["year", "iso3", var]]
        sub = sub.dropna(subset=[var])
        n_countries_total = facet_country_counts[stratum]
        colour = palette[stratum]
        label = stratum_label(stratum)

        if n_countries_total < SMALL_STRATUM_THRESHOLD:
            # Individual country lines for small strata.
            for iso3, country_sub in sub.groupby("iso3"):
                country_sub = country_sub.sort_values("year")
                ax.plot(
                    country_sub["year"], country_sub[var],
                    color=colour, alpha=0.85, linewidth=1.2,
                    label=f"{label} ({iso3})",
                )
            if not sub.empty:
                year_n = sub.groupby("year").size()
                overall_min_n = min(overall_min_n, int(year_n.min()))
                overall_max_n = max(overall_max_n, int(year_n.max()))
        else:
            agg = aggregate_band(sub, var)
            if agg.empty:
                continue
            years = agg.index.values
            mean = agg["mean"].values
            q25 = agg["q25"].values
            q75 = agg["q75"].values
            ax.plot(years, mean, color=colour, linewidth=1.7, label=label)
            ax.fill_between(years, q25, q75, color=colour, alpha=0.18, linewidth=0)

            n_arr = agg["n_observed"].values
            n_arr = n_arr[n_arr > 0]
            if n_arr.size > 0:
                overall_min_n = min(overall_min_n, int(n_arr.min()))
                overall_max_n = max(overall_max_n, int(n_arr.max()))

    if overall_min_n == float("inf"):
        overall_min_n = 0
    if overall_max_n < 0:
        overall_max_n = 0

    ax.set_xlabel("year", fontsize=9)
    ax.set_ylabel(var, fontsize=9)
    ax.tick_params(axis="both", labelsize=8)
    ax.set_xlim(1990, 2023)
    ax.grid(True, alpha=0.3, linewidth=0.6)
    ax.set_title(
        f"{var}    (observed N per year: {overall_min_n}–{overall_max_n})",
        fontsize=10,
    )

    return overall_min_n, overall_max_n


def build_palette(
    strata: list[str],
    cmap_name: str = "tab10",
) -> dict[str, tuple]:
    """Stable name -> colour mapping for a list of strata."""
    cmap = plt.get_cmap(cmap_name)
    return {s: cmap(i % cmap.N) for i, s in enumerate(strata)}


def render_figure(
    panel: pd.DataFrame,
    facet_col: str,
    facet_order: list[str],
    palette: dict[str, tuple],
    facet_country_counts: dict[str, int],
    title_suffix: str,
    out_path: Path,
) -> None:
    """Render the 1x3 figure (one subplot per variable in TS_VARIABLES)."""
    n_panels = len(TS_VARIABLES)
    fig, axes = plt.subplots(
        1, n_panels,
        figsize=(n_panels * FIG_W_PER_PANEL, FIG_H_PER_PANEL),
        sharex=True,
    )

    for ax, var in zip(axes, TS_VARIABLES):
        draw_panel(ax, panel, var, facet_col, facet_order, palette, facet_country_counts)

    # Single shared legend below.
    handles = []
    for stratum in facet_order:
        n = facet_country_counts[stratum]
        label = f"{stratum_label(stratum)} (n={n})"
        if n < SMALL_STRATUM_THRESHOLD:
            label += "  individual lines"
        handles.append(
            plt.Line2D([0], [0], color=palette[stratum], linewidth=2.0, label=label)
        )
    handles.append(
        plt.Line2D([0], [0], color="grey", linewidth=8, alpha=0.18,
                   label="IQR band (25–75 percentile)")
    )

    fig.legend(
        handles=handles,
        loc="lower center",
        ncol=min(len(handles), 4),
        frameon=False,
        fontsize=8.5,
        bbox_to_anchor=(0.5, -0.04),
    )

    fig.suptitle(
        f"Cross-country time-series by {title_suffix}\n"
        f"(line = mean, band = IQR; small strata show individual country lines)",
        fontsize=12, y=1.0,
    )
    fig.tight_layout(rect=(0, 0.06, 1, 0.94))
    fig.savefig(out_path, dpi=FIG_DPI, bbox_inches="tight")
    plt.close(fig)


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

def main() -> int:
    project_root = find_project_root(Path(__file__).resolve().parent)
    panel_path = project_root / "data" / "processed" / "panel.csv"
    fig_dir = project_root / "outputs" / "figures"
    fig_dir.mkdir(parents=True, exist_ok=True)

    if not panel_path.exists():
        print(f"ERROR: panel not found at {panel_path}", file=sys.stderr)
        return 1

    panel = pd.read_csv(panel_path)
    print(f"Loaded panel: {panel.shape[0]:,} rows x {panel.shape[1]} cols")

    needed = ("year", "iso3", "region_name", "income_level_name", *TS_VARIABLES)
    missing = [c for c in needed if c not in panel.columns]
    if missing:
        print(f"ERROR: missing columns: {missing}", file=sys.stderr)
        return 1

    # ---- Region facet -------------------------------------------------------
    region_country_counts = (
        panel[["iso3", "region_name"]]
        .drop_duplicates()
        .groupby("region_name")
        .size()
        .to_dict()
    )
    region_order = sorted(region_country_counts.keys())
    region_palette = build_palette(region_order, cmap_name="tab10")

    print("\nRegion strata (N_countries):")
    for r in region_order:
        marker = " *small*" if region_country_counts[r] < SMALL_STRATUM_THRESHOLD else ""
        print(f"  {stratum_label(r):<55}  n={region_country_counts[r]}{marker}")

    region_path = fig_dir / "phase03_s05_timeseries_by_region.png"
    render_figure(
        panel=panel,
        facet_col="region_name",
        facet_order=region_order,
        palette=region_palette,
        facet_country_counts=region_country_counts,
        title_suffix="region",
        out_path=region_path,
    )
    print(f"Saved figure: {region_path}")

    # ---- Income facet -------------------------------------------------------
    income_country_counts = (
        panel[["iso3", "income_level_name"]]
        .drop_duplicates()
        .groupby("income_level_name")
        .size()
        .to_dict()
    )

    # Display order: Low -> High where matches exist; append any extras.
    income_order = [g for g in INCOME_DISPLAY_ORDER if g in income_country_counts]
    extras = [g for g in income_country_counts if g not in income_order]
    income_order.extend(sorted(extras))
    # NOTE: viridis (sequential) gave indistinguishable purples across 5 strata
    # because build_palette indexed only the leftmost slots of the 256-stop scale.
    # Switched to tab10 (categorical) for clear discrimination. Ordinality
    # narrative (Low -> High) is now carried by legend ordering rather than hue.
    income_palette = build_palette(income_order, cmap_name="tab10")

    print("\nIncome strata (N_countries):")
    for g in income_order:
        marker = " *small*" if income_country_counts[g] < SMALL_STRATUM_THRESHOLD else ""
        print(f"  {stratum_label(g):<25}  n={income_country_counts[g]}{marker}")

    income_path = fig_dir / "phase03_s05_timeseries_by_income.png"
    render_figure(
        panel=panel,
        facet_col="income_level_name",
        facet_order=income_order,
        palette=income_palette,
        facet_country_counts=income_country_counts,
        title_suffix="income group",
        out_path=income_path,
    )
    print(f"Saved figure: {income_path}")

    # ---- Sanity total --------------------------------------------------------
    total_region_countries = sum(region_country_counts.values())
    total_income_countries = sum(income_country_counts.values())
    print(f"\nSanity: region facet covers {total_region_countries} countries "
          f"(expect 217)")
    print(f"        income facet covers {total_income_countries} countries "
          f"(expect 217)")

    if total_region_countries != 217 or total_income_countries != 217:
        print("  WARNING: country count != 217; investigate metadata.",
              file=sys.stderr)

    return 0


if __name__ == "__main__":
    sys.exit(main())
