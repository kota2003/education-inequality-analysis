"""
Phase 03 - Step 04: Correlation matrix and VIF diagnostics.

Purpose:
    Two parallel multicollinearity diagnostics on the analytical panel:
      (1) 19x19 Pearson correlation heatmap (with Spearman written to CSV
          as a supplementary view), giving a visual map of pairwise
          linear association across all declared variables including gini.
      (2) Stratified VIF table on the 18 RHS variables (gini excluded as
          target) computed on the listwise-complete subset (matching
          Phase 02's `no_gini_diagnostic` specification, N=3,041, 186
          countries). VIF is flagged at two thresholds per Step 01
          Decision 4: VIF>5 "watch", VIF>10 "concern".

Inputs:
    data/processed/panel.csv
    data/raw/manifest.yaml

Outputs:
    outputs/figures/phase03_s04_correlation_matrix.png
    outputs/tables/phase03_s04_correlation_matrix.csv      (Pearson)
    outputs/tables/phase03_s04_correlation_spearman.csv    (Spearman, supplementary)
    outputs/tables/phase03_s04_vif.csv                     (stratified)
"""

from __future__ import annotations

import sys
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from statsmodels.stats.outliers_influence import variance_inflation_factor

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.manifest import load_manifest, manifest_variable_order  # noqa: E402
from src.paths import find_project_root  # noqa: E402

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

TARGET = "gini"

# Stratified VIF thresholds per Step 01 Decision 4.
VIF_WATCH = 5.0
VIF_CONCERN = 10.0

# Heatmap cosmetics.
HEATMAP_CMAP = "RdBu_r"
HEATMAP_VMIN = -1.0
HEATMAP_VMAX = 1.0
HEATMAP_FIG_W = 11.0
HEATMAP_FIG_H = 9.5
HEATMAP_DPI = 300


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def classify_vif(value: float) -> str:
    """Stratify a VIF value into ok / watch / concern."""
    if not np.isfinite(value):
        return "undefined"
    if value > VIF_CONCERN:
        return "concern"
    if value > VIF_WATCH:
        return "watch"
    return "ok"


def compute_vif_table(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Compute VIF for each column in `columns` using listwise-complete data.

    Adds a constant column (statsmodels VIF requires a constant in the
    design matrix to give the standard textbook interpretation), but reports
    only the VIF of the substantive predictors.
    """
    sub = df[columns].dropna().copy()
    n = len(sub)

    # Add a constant column for the design matrix.
    X = sub.copy()
    X.insert(0, "_const", 1.0)
    X_arr = X.to_numpy(dtype=float)

    rows = []
    for i, col in enumerate(columns):
        # statsmodels' VIF index is the column position in X_arr.
        # Substantive columns start at position 1 (position 0 is _const).
        try:
            vif_val = float(variance_inflation_factor(X_arr, i + 1))
        except Exception as exc:  # noqa: BLE001
            print(f"  WARNING: VIF failed for {col}: {exc}", file=sys.stderr)
            vif_val = float("nan")
        rows.append({
            "variable": col,
            "vif": vif_val,
            "flag": classify_vif(vif_val),
        })

    df_out = pd.DataFrame(rows).sort_values("vif", ascending=False).reset_index(drop=True)
    df_out.attrs["n_listwise"] = n
    return df_out


def plot_correlation_heatmap(
    corr: pd.DataFrame,
    title: str,
    fig_path: Path,
) -> None:
    """Render a 19x19 correlation heatmap with cell annotations."""
    fig, ax = plt.subplots(figsize=(HEATMAP_FIG_W, HEATMAP_FIG_H))
    im = ax.imshow(
        corr.values,
        cmap=HEATMAP_CMAP, vmin=HEATMAP_VMIN, vmax=HEATMAP_VMAX,
        aspect="auto",
    )

    n = corr.shape[0]
    ax.set_xticks(range(n))
    ax.set_yticks(range(n))
    ax.set_xticklabels(corr.columns, rotation=70, ha="right", fontsize=8)
    ax.set_yticklabels(corr.index, fontsize=8)

    # Annotate each cell with the correlation value (smaller font for 19x19).
    for i in range(n):
        for j in range(n):
            v = corr.values[i, j]
            colour = "white" if abs(v) > 0.55 else "black"
            ax.text(j, i, f"{v:.2f}", ha="center", va="center",
                    fontsize=6.0, color=colour)

    cbar = fig.colorbar(im, ax=ax, fraction=0.04, pad=0.02)
    cbar.set_label("Pearson r", fontsize=9)
    cbar.ax.tick_params(labelsize=8)

    ax.set_title(title, fontsize=12, pad=10)
    fig.tight_layout()
    fig.savefig(fig_path, dpi=HEATMAP_DPI, bbox_inches="tight")
    plt.close(fig)


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

    if not panel_path.exists():
        print(f"ERROR: panel not found at {panel_path}", file=sys.stderr)
        return 1
    if not manifest_path.exists():
        print(f"ERROR: manifest not found at {manifest_path}", file=sys.stderr)
        return 1

    panel = pd.read_csv(panel_path)
    manifest = load_manifest(manifest_path)

    var_order = manifest_variable_order(manifest)  # 19 variables
    rhs_vars = [v for v in var_order if v != TARGET]  # 18 variables, gini excluded

    missing_cols = [c for c in var_order if c not in panel.columns]
    if missing_cols:
        print(f"ERROR: missing columns in panel: {missing_cols}", file=sys.stderr)
        return 1

    print(f"Loaded panel: {panel.shape[0]:,} rows x {panel.shape[1]} cols")
    print(f"Variables in canonical manifest order ({len(var_order)}): "
          f"target={TARGET}, RHS={len(rhs_vars)}")

    # -------------------------------------------------------------------------
    # (1) Correlation matrices
    # -------------------------------------------------------------------------
    # pandas .corr() uses pairwise-complete deletion (per-pair max N).
    pearson = panel[var_order].corr(method="pearson")
    spearman = panel[var_order].corr(method="spearman")

    pearson_path = tbl_dir / "phase03_s04_correlation_matrix.csv"
    spearman_path = tbl_dir / "phase03_s04_correlation_spearman.csv"
    pearson.to_csv(pearson_path)
    spearman.to_csv(spearman_path)
    print(f"Saved Pearson correlation matrix: {pearson_path}")
    print(f"Saved Spearman correlation matrix: {spearman_path}")

    heatmap_path = fig_dir / "phase03_s04_correlation_matrix.png"
    plot_correlation_heatmap(
        pearson,
        title="Pearson correlation matrix (pairwise-complete N per cell)",
        fig_path=heatmap_path,
    )
    print(f"Saved Pearson heatmap: {heatmap_path}")

    # Highlight strongest correlations with gini.
    gini_corrs = pearson[TARGET].drop(TARGET).abs().sort_values(ascending=False)
    print("\nTop 5 |Pearson r| with gini:")
    for var, abs_r in gini_corrs.head(5).items():
        signed = pearson.loc[var, TARGET]
        print(f"  {var:<28}  r = {signed:+.3f}")

    # -------------------------------------------------------------------------
    # (2) VIF on listwise-complete 18 RHS variables
    # -------------------------------------------------------------------------
    vif_df = compute_vif_table(panel, rhs_vars)
    n_listwise = vif_df.attrs["n_listwise"]
    vif_path = tbl_dir / "phase03_s04_vif.csv"
    vif_df.to_csv(vif_path, index=False)
    print(f"\nSaved VIF table: {vif_path}")
    print(f"VIF computed on listwise-complete subset of 18 RHS variables: "
          f"N = {n_listwise:,} country-years")

    # Stdout report sorted descending.
    print("\nVIF (sorted desc; thresholds: >10 concern, >5 watch):")
    print(f"  {'variable':<28}{'VIF':>10}  flag")
    print("  " + "-" * 50)
    for _, row in vif_df.iterrows():
        vif_str = f"{row['vif']:.2f}" if np.isfinite(row['vif']) else "NA"
        print(f"  {row['variable']:<28}{vif_str:>10}  {row['flag']}")

    # Counts by flag.
    flag_counts = vif_df["flag"].value_counts().to_dict()
    print(f"\nFlag summary: {flag_counts}")

    # -------------------------------------------------------------------------
    # Phase 05 specification recommendation
    # -------------------------------------------------------------------------
    print("\n--- Phase 05 specification recommendation -----------------------")

    sector_share_vars = ("agri_value_added_gdp", "manu_value_added_gdp",
                         "services_value_added_gdp")
    sector_in_concern = [
        v for v in sector_share_vars
        if v in vif_df["variable"].values
        and vif_df.loc[vif_df["variable"] == v, "flag"].iloc[0] == "concern"
    ]
    if sector_in_concern:
        print(f"  Sector-share trio at 'concern': {sector_in_concern}")
        print("    Rationale: agri + manu + services sum to ~100% by construction.")
        print("    Action for Phase 05: drop one (e.g. services as the residual),")
        print("    or replace the trio with the first principal component.")

    other_concern = [
        v for v in vif_df.loc[vif_df["flag"] == "concern", "variable"]
        if v not in sector_share_vars
    ]
    if other_concern:
        print(f"  Non-structural 'concern' VIF: {other_concern}")
        print("    Action for Phase 05: candidate for dropping or transforming;")
        print("    revisit after applying log to gdp_per_capita / population.")
    else:
        print("  No non-structural variables at 'concern' level.")

    watch_vars = vif_df.loc[vif_df["flag"] == "watch", "variable"].tolist()
    if watch_vars:
        print(f"  'Watch' tier (5 < VIF <= 10): {watch_vars}")
        print("    Action for Phase 05: monitor; usable in baseline specification.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
