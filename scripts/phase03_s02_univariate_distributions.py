"""
Phase 03 - Step 02: Univariate distributions.

Purpose:
    Generate a 6x4 grid of histograms covering all 19 declared variables in
    the analytical panel. Three skewed economic variables (gdp_per_capita_usd,
    gdp_per_capita_ppp, population) appear as raw + log pairs (per Phase 03
    Step 01 Decision 3), giving 22 occupied subplots in the 24-cell grid.
    Skewness statistics are written to both stdout and a CSV for the
    portfolio notebook to consume in Step 07.

Inputs:
    data/processed/panel.csv       - analytical panel (7,378 x 24)
    data/raw/manifest.yaml         - canonical variable order

Outputs:
    outputs/figures/phase03_s02_univariate_distributions.png
    outputs/tables/phase03_s02_skewness.csv
"""

from __future__ import annotations

import sys
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy import stats

# Make src/ importable when run from project root or scripts/
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.manifest import load_manifest, manifest_variable_order  # noqa: E402
from src.paths import find_project_root  # noqa: E402

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

# Variables that get a raw + log paired display per Step 01 Decision 3.
LOG_PAIR_VARS = ("gdp_per_capita_usd", "gdp_per_capita_ppp", "population")

# Histogram bin count, fixed for cross-variable comparability.
N_BINS = 30

# Grid layout: 6 rows x 4 cols = 24 cells; 22 occupied (16 single + 3 pairs).
N_ROWS = 6
N_COLS = 4

FIG_DPI = 300
FIG_W_PER_COL = 3.6   # inches per column
FIG_H_PER_ROW = 2.8   # inches per row


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def build_panel_order(manifest: dict) -> list[tuple[str, str, str]]:
    """Return the (variable, transform, subplot_label) tuples in display order.

    Each tuple is (variable_name, transform, label) where:
      - transform is "raw" or "log"
      - label is the subplot title

    Variables in LOG_PAIR_VARS produce two consecutive entries (raw, log);
    others produce one (raw).
    """
    order = manifest_variable_order(manifest)
    panels: list[tuple[str, str, str]] = []
    for var in order:
        if var in LOG_PAIR_VARS:
            panels.append((var, "raw", f"{var} (raw)"))
            panels.append((var, "log", f"{var} (log)"))
        else:
            panels.append((var, "raw", var))
    return panels


def compute_skewness(values: pd.Series) -> tuple[float, int]:
    """Return (Fisher-Pearson skewness, n_observed) for a numeric Series.

    NaN values are dropped before computation. Returns (nan, 0) when the
    series has fewer than 3 observations (skewness undefined).
    """
    arr = values.dropna().to_numpy(dtype=float)
    n = arr.size
    if n < 3:
        return float("nan"), n
    return float(stats.skew(arr, bias=False)), n


def plot_histogram(
    ax: plt.Axes,
    values: pd.Series,
    label: str,
    transform: str,
) -> tuple[float, int]:
    """Draw a histogram on `ax` and return (skewness, n_observed)."""
    arr = values.dropna().to_numpy(dtype=float)

    if transform == "log":
        # log requires strictly positive values; gdp_per_capita and population
        # are positive by construction, but guard anyway.
        arr = arr[arr > 0]
        arr = np.log(arr)

    n = arr.size
    if n >= 3:
        skewness = float(stats.skew(arr, bias=False))
    else:
        skewness = float("nan")

    if n > 0:
        ax.hist(arr, bins=N_BINS, color="#4C72B0", edgecolor="white", linewidth=0.4)
    ax.set_title(label, fontsize=9.5)
    ax.tick_params(axis="both", labelsize=7.5)

    # Annotate n_observed and skewness in the upper-right corner.
    skew_text = f"skew={skewness:.2f}" if np.isfinite(skewness) else "skew=NA"
    ax.text(
        0.97, 0.95,
        f"n={n:,}\n{skew_text}",
        transform=ax.transAxes,
        ha="right", va="top",
        fontsize=7.5,
        bbox=dict(boxstyle="round,pad=0.25", facecolor="white", edgecolor="#cccccc", alpha=0.85),
    )

    ax.set_ylabel("frequency", fontsize=8)
    ax.set_xlabel("value" if transform == "raw" else "log(value)", fontsize=8)
    return skewness, n


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

def main() -> int:
    project_root = find_project_root(Path(__file__).resolve().parent)

    panel_path = project_root / "data" / "processed" / "panel.csv"
    manifest_path = project_root / "data" / "raw" / "manifest.yaml"
    fig_dir = project_root / "outputs" / "figures"
    tbl_dir = project_root / "outputs" / "tables"
    fig_dir.mkdir(parents=True, exist_ok=True)
    tbl_dir.mkdir(parents=True, exist_ok=True)

    # --- Load inputs ---------------------------------------------------------
    if not panel_path.exists():
        print(f"ERROR: panel not found at {panel_path}", file=sys.stderr)
        return 1
    if not manifest_path.exists():
        print(f"ERROR: manifest not found at {manifest_path}", file=sys.stderr)
        return 1

    panel = pd.read_csv(panel_path)
    manifest = load_manifest(manifest_path)
    panel_order = build_panel_order(manifest)

    print(f"Loaded panel: {panel.shape[0]:,} rows x {panel.shape[1]} cols")
    print(f"Manifest variable count: {len(manifest_variable_order(manifest))}")
    print(f"Subplot count (incl. raw+log pairs): {len(panel_order)}")

    # --- Build figure --------------------------------------------------------
    fig, axes = plt.subplots(
        N_ROWS, N_COLS,
        figsize=(N_COLS * FIG_W_PER_COL, N_ROWS * FIG_H_PER_ROW),
    )
    axes_flat = axes.flatten()

    skewness_records: list[dict] = []

    for i, (var, transform, label) in enumerate(panel_order):
        ax = axes_flat[i]
        if var not in panel.columns:
            print(f"  WARNING: variable {var!r} not in panel columns; skipping",
                  file=sys.stderr)
            ax.set_visible(False)
            continue
        skew_val, n_obs = plot_histogram(ax, panel[var], label, transform)
        skewness_records.append({
            "variable": var,
            "transform": transform,
            "n_observed": n_obs,
            "skewness": skew_val,
        })

    # Hide unused cells (24 - 22 = 2 unused).
    for j in range(len(panel_order), len(axes_flat)):
        axes_flat[j].set_visible(False)

    fig.suptitle(
        "Univariate distributions of all 19 declared variables\n"
        "(raw + log shown for gdp_per_capita_usd, gdp_per_capita_ppp, population)",
        fontsize=12, y=0.995,
    )
    fig.tight_layout(rect=(0, 0, 1, 0.97))

    fig_path = fig_dir / "phase03_s02_univariate_distributions.png"
    fig.savefig(fig_path, dpi=FIG_DPI, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved figure: {fig_path}")

    # --- Save skewness CSV ---------------------------------------------------
    skew_df = pd.DataFrame(skewness_records)
    tbl_path = tbl_dir / "phase03_s02_skewness.csv"
    skew_df.to_csv(tbl_path, index=False)
    print(f"Saved skewness table: {tbl_path}")

    # --- Stdout summary ------------------------------------------------------
    print("\nPer-variable skewness (Fisher-Pearson, bias-corrected):")
    print(f"  {'variable':<28}{'transform':<10}{'n_observed':>12}{'skewness':>12}")
    print("  " + "-" * 62)
    for rec in skewness_records:
        skew_str = f"{rec['skewness']:.3f}" if np.isfinite(rec['skewness']) else "NA"
        print(f"  {rec['variable']:<28}{rec['transform']:<10}"
              f"{rec['n_observed']:>12,}{skew_str:>12}")

    # --- Cross-check n_observed against panel non-null counts ----------------
    print("\nCross-check: raw n_observed vs panel.notna().sum() for each variable")
    mismatches = 0
    for rec in skewness_records:
        if rec["transform"] != "raw":
            continue
        expected = int(panel[rec["variable"]].notna().sum())
        if expected != rec["n_observed"]:
            print(f"  MISMATCH: {rec['variable']}  expected={expected:,}  "
                  f"got={rec['n_observed']:,}", file=sys.stderr)
            mismatches += 1
    if mismatches == 0:
        print("  All 19 raw n_observed counts match panel.notna().sum() exactly.")
    else:
        print(f"  WARNING: {mismatches} mismatch(es) detected.", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
