"""
Phase 07 - Step 03: Build the cross-method convergence figure.

Purpose:
    Render the aggregate cross-method mys-Gini estimates from Phase 03-06
    as a single matplotlib forest plot. The figure visualises the seven
    aggregate-scope estimators on one x-axis to make the central project
    finding immediately legible: every method finds a negative
    association between mean years of schooling (mys) and the Gini
    coefficient, with magnitudes spanning roughly -0.4 to -1.4
    Gini-points per mys-year (Phase 03 Pearson r is on a different
    scale, annotated separately).

    Per Convention 6.4 (cite, do not recompute), the figure is built
    purely from the synthesis table produced in Step 02; this script
    runs no statistical computation. The per-cluster mys comparison is
    NOT plotted here because Phase 06 already produced
    `outputs/figures/phase06_s07_per_cluster_slopes.png` for that
    purpose; the notebook (Step 04) inherits that figure rather than
    re-rendering it.

Inputs:
    outputs/tables/phase07_s02_synthesis_table.csv
        Long-format synthesis table from Step 02. Filtered to
        scope == 'aggregate' for this figure. Read with
        dtype={'phase': str, ...} to preserve zero-padded phase IDs
        ('03', '05', '06') across the CSV roundtrip.

Outputs:
    outputs/figures/phase07_s03_convergence.png
        ~10 x 6 inches at 150 dpi. matplotlib backend per Phase 03
        Correction Note (no plotly / kaleido).
"""

from __future__ import annotations

import sys
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.lines import Line2D

# Make src/ importable when run from any working directory.
_HERE = Path(__file__).resolve().parent
_PROJECT_ROOT_GUESS = _HERE.parent
if str(_PROJECT_ROOT_GUESS) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT_GUESS))

from src.paths import find_project_root  # noqa: E402


# -----------------------------------------------------------------------------
# Plot configuration
# -----------------------------------------------------------------------------

# Forest-plot row order (top to bottom in y-axis).
PLOT_ORDER = [
    "univariate_pearson_r",
    "pooled_ols_spec_a",
    "fe_spec_a",
    "re_spec_a",
    "ridge_raw_scale_coef",
    "rf_mean_signed_shap",
    "xgb_mean_signed_shap",
]

# Display labels for each estimator.
DISPLAY_LABEL = {
    "univariate_pearson_r": "Phase 03  Univariate Pearson r",
    "pooled_ols_spec_a": "Phase 05  Pooled OLS  Spec A",
    "fe_spec_a": "Phase 05  FE  Spec A (two-way)",
    "re_spec_a": "Phase 05  RE  Spec A",
    "ridge_raw_scale_coef": "Phase 06  Ridge raw-scale coef",
    "rf_mean_signed_shap": "Phase 06  RF mean signed SHAP",
    "xgb_mean_signed_shap": "Phase 06  XGB mean signed SHAP",
}

# Phase-keyed visual encoding. Keys are zero-padded strings to match
# the synthesis table's `phase` column.
PHASE_COLOR = {
    "03": "#555555",  # neutral gray
    "05": "#1f77b4",  # matplotlib default C0 (blue)
    "06": "#ff7f0e",  # matplotlib default C1 (orange)
}
PHASE_MARKER = {
    "03": "s",  # square
    "05": "o",  # circle
    "06": "^",  # triangle
}

# Columns that must be read as strings to preserve formatting / avoid
# type-inference loss (notably zero-padding on the `phase` column,
# which would otherwise be coerced to int64 and lose the leading zero).
STRING_COLUMNS = ["phase", "estimator", "scope", "source_artefact"]


def p_to_stars(p: float) -> str:
    """Conventional significance stars from a p-value."""
    if pd.isna(p):
        return ""
    if p < 0.001:
        return " ***"
    if p < 0.01:
        return " **"
    if p < 0.05:
        return " *"
    return "  (ns)"


# -----------------------------------------------------------------------------
# Build the figure
# -----------------------------------------------------------------------------

def build_figure(df_aggregate: pd.DataFrame, output_path: Path) -> None:
    """Render the forest plot and save it to disk."""
    # Order rows for the forest plot.
    df = df_aggregate.copy()
    df["plot_order"] = df["estimator"].map(
        {e: i for i, e in enumerate(PLOT_ORDER)}
    )
    if df["plot_order"].isna().any():
        missing = df.loc[df["plot_order"].isna(), "estimator"].tolist()
        raise ValueError(
            f"Estimators not in PLOT_ORDER: {missing}. "
            f"Synthesis table contains an aggregate row this script "
            f"does not know how to plot."
        )
    df = df.sort_values("plot_order").reset_index(drop=True)

    # Build display label with significance stars (Phase 05 only - Phase 03
    # Pearson r and Phase 06 SHAP are not p-tested in the panel-econometric
    # sense).
    def label_for_row(row: pd.Series) -> str:
        base = DISPLAY_LABEL[row["estimator"]]
        if row["phase"] == "05":
            return base + p_to_stars(row["p"])
        return base

    df["display_label"] = df.apply(label_for_row, axis=1)

    # rcParams: clean spines, slightly larger default font.
    plt.rcParams.update({
        "font.size": 10,
        "axes.spines.top": False,
        "axes.spines.right": False,
    })

    fig, ax = plt.subplots(figsize=(10, 6))

    # y positions: top of plot = first row in PLOT_ORDER.
    y_positions = list(range(len(df)))[::-1]

    for y, (_, row) in zip(y_positions, df.iterrows()):
        color = PHASE_COLOR[row["phase"]]
        marker = PHASE_MARKER[row["phase"]]
        x = row["mys_effect"]

        if pd.notna(row["ci_lower"]) and pd.notna(row["ci_upper"]):
            xerr_left = x - row["ci_lower"]
            xerr_right = row["ci_upper"] - x
            ax.errorbar(
                x, y,
                xerr=[[xerr_left], [xerr_right]],
                fmt=marker,
                color=color,
                markersize=9,
                markeredgecolor="white",
                markeredgewidth=0.5,
                capsize=4,
                elinewidth=1.5,
                ecolor=color,
                alpha=0.95,
            )
        else:
            ax.plot(
                x, y,
                marker=marker,
                color=color,
                markersize=9,
                markeredgecolor="white",
                markeredgewidth=0.5,
                linestyle="none",
                alpha=0.95,
            )

        # Inline value annotation to the right of the marker.
        ax.annotate(
            f" {x:+.2f}",
            xy=(x, y),
            xytext=(8, 0),
            textcoords="offset points",
            va="center",
            fontsize=8.5,
            color=color,
            fontweight="bold",
        )

    # Reference line at x = 0 (no association).
    ax.axvline(
        0,
        color="gray",
        linestyle="--",
        linewidth=1,
        alpha=0.5,
        zorder=0,
    )

    # y-axis: estimator labels.
    ax.set_yticks(y_positions)
    ax.set_yticklabels(df["display_label"].tolist())
    ax.set_ylim(-0.7, len(df) - 0.3)

    # x-axis: mys effect with unit annotation.
    ax.set_xlabel(
        "mys effect on Gini  "
        "(Gini point per mys-year; Phase 03 Pearson r is unitless)",
        fontsize=10,
    )

    # x-limits with padding so annotations fit.
    x_min = min(df["mys_effect"].min(), df["ci_lower"].min(skipna=True))
    x_max = max(df["mys_effect"].max(), df["ci_upper"].max(skipna=True), 0)
    pad = 0.25 * (x_max - x_min)
    ax.set_xlim(x_min - pad, x_max + pad + 0.4)

    # Title.
    ax.set_title(
        "Cross-method mys-Gini estimates: aggregate convergence "
        "on a negative association",
        fontsize=12,
        fontweight="bold",
        loc="left",
        pad=14,
    )

    # Legend: phase color + marker.
    legend_handles = [
        Line2D(
            [0], [0],
            marker=PHASE_MARKER[p],
            color="white",
            markerfacecolor=PHASE_COLOR[p],
            markeredgecolor="white",
            markeredgewidth=0.5,
            markersize=9,
            label=f"Phase {p}",
            linestyle="none",
        )
        for p in ["03", "05", "06"]
    ]
    ax.legend(
        handles=legend_handles,
        loc="lower right",
        frameon=True,
        framealpha=0.95,
        fontsize=9,
    )

    # Source attribution footer.
    fig.text(
        0.99, 0.005,
        "Source: outputs/tables/phase07_s02_synthesis_table.csv "
        "(scope='aggregate'). 95% CI shown for Phase 05 estimates.",
        ha="right",
        fontsize=7.5,
        style="italic",
        color="gray",
    )

    fig.tight_layout(rect=(0, 0.025, 1, 1))

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

def main() -> None:
    project_root = find_project_root()
    in_path = project_root / "outputs" / "tables" / "phase07_s02_synthesis_table.csv"
    out_path = project_root / "outputs" / "figures" / "phase07_s03_convergence.png"

    if not in_path.exists():
        raise FileNotFoundError(
            f"Step 02 synthesis table not found at {in_path}. "
            f"Run scripts/phase07_s02_build_synthesis_table.py first."
        )

    # Force string dtype on text columns so the zero-padded `phase` IDs
    # ('03', '05', '06') survive the CSV roundtrip rather than being
    # type-inferred into int64 (3, 5, 6) and breaking the PHASE_COLOR /
    # PHASE_MARKER dict lookups.
    df = pd.read_csv(
        in_path,
        dtype={col: str for col in STRING_COLUMNS},
    )

    # Defensive: re-confirm the expected zero-padded phase values.
    expected_phases = {"03", "05", "06"}
    actual_phases = set(df["phase"].unique())
    if not actual_phases.issubset(expected_phases):
        raise ValueError(
            f"Unexpected phase values in synthesis table: "
            f"got {sorted(actual_phases)}, expected subset of "
            f"{sorted(expected_phases)}. Check Step 02 output."
        )

    df_aggregate = df[df["scope"] == "aggregate"].copy()

    # Sanity checks before plotting.
    assert len(df_aggregate) == 7, (
        f"Expected 7 aggregate rows, got {len(df_aggregate)}"
    )
    assert (df_aggregate["mys_effect"] < 0).all(), (
        "All seven aggregate estimates should be negative; convergence "
        "claim broken if not."
    )

    build_figure(df_aggregate, out_path)

    print(f"Project root : {project_root}")
    print(f"Input        : {in_path.relative_to(project_root)}")
    print(f"Output       : {out_path.relative_to(project_root)}")
    print(f"Aggregate rows plotted: {len(df_aggregate)}")
    print()
    print("Plotted estimates (top to bottom):")
    df_display = df_aggregate.copy()
    df_display["plot_order"] = df_display["estimator"].map(
        {e: i for i, e in enumerate(PLOT_ORDER)}
    )
    df_display = df_display.sort_values("plot_order")
    for _, row in df_display.iterrows():
        ci = ""
        if pd.notna(row["ci_lower"]) and pd.notna(row["ci_upper"]):
            ci = f"  CI [{row['ci_lower']:+.2f}, {row['ci_upper']:+.2f}]"
        p = ""
        if pd.notna(row["p"]):
            p = f"  p={row['p']:.3f}"
        print(
            f"  Phase {row['phase']}  {row['estimator']:<28s}  "
            f"{row['mys_effect']:+.3f}{ci}{p}"
        )
    print()
    print(f"[OK] Saved convergence figure to {out_path.relative_to(project_root)}")


if __name__ == "__main__":
    main()
