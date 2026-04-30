"""
Phase 03 - Step 03: Bivariate analysis - Gini vs explanatory variables.

Purpose:
    For each of six candidate predictors (5 education + 1 benchmark control),
    produce a scatter of gini (Y) vs predictor (X), with points coloured by
    region_name (per Step 01 Decision 2) and a LOWESS smoother overlaid to
    surface non-linearity. Also emit a per-predictor LOWESS-vs-linear fit
    summary CSV that Phase 05 can cite when weighing polynomial / log
    specifications against the linear baseline.

Inputs:
    data/processed/panel.csv

Outputs:
    outputs/figures/phase03_s03_bivariate_gini.png
    outputs/tables/phase03_s03_lowess_vs_linear.csv
"""

from __future__ import annotations

import sys
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from statsmodels.nonparametric.smoothers_lowess import lowess

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.paths import find_project_root  # noqa: E402

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

# Six candidate predictors per prompt Step 03: 5 education + 1 benchmark control.
PREDICTORS: tuple[str, ...] = (
    "enrol_primary",
    "enrol_secondary",
    "enrol_tertiary",
    "mean_years_schooling",
    "edu_expenditure_gdp",
    "gdp_per_capita_ppp",
)

TARGET = "gini"
COLOUR_BY = "region_name"

# LOWESS parameters: frac=0.4 (EDA-standard tighter than default 0.6),
# it=3 (default robustness iterations), delta=0 (no linear interp / extrapolation).
LOWESS_FRAC = 0.4
LOWESS_IT = 3
LOWESS_DELTA = 0.0

# 2x3 grid of subplots.
N_ROWS = 2
N_COLS = 3
FIG_W_PER_COL = 4.6
FIG_H_PER_ROW = 3.8
FIG_DPI = 300

# Marker styling.
SCATTER_ALPHA = 0.45
SCATTER_SIZE = 12
LOWESS_COLOUR = "black"
LOWESS_LW = 1.6


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def fit_lowess_and_compare(x: np.ndarray, y: np.ndarray) -> dict:
    """Fit LOWESS and OLS line on (x, y) and return summary metrics.

    Returns a dict with:
        n              : sample size used
        ols_r2         : R^2 of simple linear fit y = a + bx
        lowess_r2      : R^2 of LOWESS fit (1 - SSE_lowess / SS_total)
        delta_r2       : lowess_r2 - ols_r2 (how much extra variance non-linearity explains)
        ols_slope      : slope of the linear fit
    """
    n = x.size
    if n < 10:
        return {
            "n": n,
            "ols_r2": float("nan"),
            "lowess_r2": float("nan"),
            "delta_r2": float("nan"),
            "ols_slope": float("nan"),
        }

    # Linear OLS fit
    slope, intercept = np.polyfit(x, y, deg=1)
    y_hat_ols = intercept + slope * x
    ss_total = float(np.sum((y - y.mean()) ** 2))
    ss_res_ols = float(np.sum((y - y_hat_ols) ** 2))
    ols_r2 = 1.0 - ss_res_ols / ss_total if ss_total > 0 else float("nan")

    # LOWESS fit at the observed x values (return_sorted=False keeps original order)
    y_hat_lowess = lowess(
        endog=y, exog=x,
        frac=LOWESS_FRAC, it=LOWESS_IT, delta=LOWESS_DELTA,
        return_sorted=False,
    )
    ss_res_lowess = float(np.sum((y - y_hat_lowess) ** 2))
    lowess_r2 = 1.0 - ss_res_lowess / ss_total if ss_total > 0 else float("nan")

    return {
        "n": int(n),
        "ols_r2": ols_r2,
        "lowess_r2": lowess_r2,
        "delta_r2": lowess_r2 - ols_r2,
        "ols_slope": float(slope),
    }


def plot_one_panel(
    ax: plt.Axes,
    df: pd.DataFrame,
    predictor: str,
    region_palette: dict[str, str],
) -> dict:
    """Draw scatter + LOWESS for a single (predictor, gini) pair on `ax`.

    Returns the LOWESS-vs-linear fit summary dict.
    """
    sub = df[[predictor, TARGET, COLOUR_BY]].dropna()
    n = len(sub)

    # Per-region scatter for legend handles.
    for region, colour in region_palette.items():
        rsub = sub[sub[COLOUR_BY] == region]
        if rsub.empty:
            continue
        ax.scatter(
            rsub[predictor], rsub[TARGET],
            s=SCATTER_SIZE, alpha=SCATTER_ALPHA,
            color=colour, edgecolors="none",
            label=region,
        )

    # LOWESS overlay + fit comparison (only if >=10 points).
    summary = fit_lowess_and_compare(
        x=sub[predictor].to_numpy(dtype=float),
        y=sub[TARGET].to_numpy(dtype=float),
    )

    if n >= 10:
        # Sort x for a clean line.
        x_sorted = np.sort(sub[predictor].to_numpy(dtype=float))
        y_lowess_sorted = lowess(
            endog=sub[TARGET].to_numpy(dtype=float),
            exog=sub[predictor].to_numpy(dtype=float),
            frac=LOWESS_FRAC, it=LOWESS_IT, delta=LOWESS_DELTA,
            return_sorted=True,
        )
        ax.plot(
            y_lowess_sorted[:, 0], y_lowess_sorted[:, 1],
            color=LOWESS_COLOUR, linewidth=LOWESS_LW, label="_nolegend_",
        )

    ax.set_xlabel(predictor, fontsize=9)
    ax.set_ylabel(TARGET, fontsize=9)
    ax.tick_params(axis="both", labelsize=7.5)

    # Annotate n + fit-quality summary.
    annotation = (
        f"n={n:,}\n"
        f"OLS R²={summary['ols_r2']:.3f}\n"
        f"LOWESS R²={summary['lowess_r2']:.3f}\n"
        f"Δ={summary['delta_r2']:.3f}"
    )
    ax.text(
        0.97, 0.97, annotation,
        transform=ax.transAxes,
        ha="right", va="top",
        fontsize=7.5,
        bbox=dict(boxstyle="round,pad=0.3", facecolor="white",
                  edgecolor="#cccccc", alpha=0.85),
    )

    summary["predictor"] = predictor
    return summary


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

def main() -> int:
    project_root = find_project_root(Path(__file__).resolve().parent)
    panel_path = project_root / "data" / "processed" / "panel.csv"
    fig_dir = project_root / "outputs" / "figures"
    tbl_dir = project_root / "outputs" / "tables"
    fig_dir.mkdir(parents=True, exist_ok=True)
    tbl_dir.mkdir(parents=True, exist_ok=True)

    if not panel_path.exists():
        print(f"ERROR: panel not found at {panel_path}", file=sys.stderr)
        return 1

    panel = pd.read_csv(panel_path)
    print(f"Loaded panel: {panel.shape[0]:,} rows x {panel.shape[1]} cols")

    # Sanity checks
    missing_cols = [c for c in (*PREDICTORS, TARGET, COLOUR_BY) if c not in panel.columns]
    if missing_cols:
        print(f"ERROR: missing columns in panel: {missing_cols}", file=sys.stderr)
        return 1

    # Build a stable region -> colour palette so colour assignment is identical
    # across all panels and matches one shared legend.
    regions = sorted(panel[COLOUR_BY].dropna().unique().tolist())
    cmap = plt.get_cmap("tab10")
    region_palette = {r: cmap(i % 10) for i, r in enumerate(regions)}
    print(f"Regions ({len(regions)}): {regions}")

    # --- Figure ---------------------------------------------------------------
    fig, axes = plt.subplots(
        N_ROWS, N_COLS,
        figsize=(N_COLS * FIG_W_PER_COL, N_ROWS * FIG_H_PER_ROW),
    )
    axes_flat = axes.flatten()

    summaries: list[dict] = []
    for i, predictor in enumerate(PREDICTORS):
        ax = axes_flat[i]
        s = plot_one_panel(ax, panel, predictor, region_palette)
        summaries.append(s)

    # Hide any unused subplots (none expected at 2x3 / 6 predictors).
    for j in range(len(PREDICTORS), len(axes_flat)):
        axes_flat[j].set_visible(False)

    # Single shared legend below the grid.
    handles = [
        plt.Line2D(
            [0], [0],
            marker="o", color="w",
            markerfacecolor=region_palette[r], markersize=7,
            label=r,
        )
        for r in regions
    ]
    handles.append(
        plt.Line2D([0], [0], color=LOWESS_COLOUR, lw=LOWESS_LW, label="LOWESS (frac=0.4)")
    )
    fig.legend(
        handles=handles,
        loc="lower center",
        ncol=min(len(handles), 4),
        frameon=False,
        fontsize=8.5,
        bbox_to_anchor=(0.5, -0.02),
    )

    fig.suptitle(
        "Bivariate scatter: Gini vs candidate predictors\n"
        "(point colour = region; black line = LOWESS smoother, frac=0.4)",
        fontsize=12, y=0.995,
    )
    fig.tight_layout(rect=(0, 0.06, 1, 0.96))

    fig_path = fig_dir / "phase03_s03_bivariate_gini.png"
    fig.savefig(fig_path, dpi=FIG_DPI, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved figure: {fig_path}")

    # --- Summary CSV ----------------------------------------------------------
    summary_df = pd.DataFrame(summaries)[
        ["predictor", "n", "ols_slope", "ols_r2", "lowess_r2", "delta_r2"]
    ]
    tbl_path = tbl_dir / "phase03_s03_lowess_vs_linear.csv"
    summary_df.to_csv(tbl_path, index=False)
    print(f"Saved fit-comparison table: {tbl_path}")

    # --- Stdout report --------------------------------------------------------
    print("\nLOWESS vs OLS-linear fit comparison (gini ~ predictor):")
    print(f"  {'predictor':<25}{'n':>8}{'slope':>10}{'OLS R²':>10}"
          f"{'LOWESS R²':>12}{'Δ':>9}")
    print("  " + "-" * 74)
    for s in summaries:
        print(
            f"  {s['predictor']:<25}{s['n']:>8,}"
            f"{s['ols_slope']:>10.4f}{s['ols_r2']:>10.3f}"
            f"{s['lowess_r2']:>12.3f}{s['delta_r2']:>9.3f}"
        )

    # Phase 05 hook: flag predictors where LOWESS clearly beats linear.
    print("\nPhase 05 hook (Δ R² > 0.02 suggests non-linearity worth modelling):")
    flagged = [s for s in summaries if np.isfinite(s["delta_r2"]) and s["delta_r2"] > 0.02]
    if flagged:
        for s in flagged:
            print(f"  {s['predictor']}: Δ R² = {s['delta_r2']:.3f}  "
                  f"(consider polynomial / log specification)")
    else:
        print("  No predictor exceeds the 0.02 threshold; linear baseline is "
              "consistent with the bivariate evidence.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
