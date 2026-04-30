"""
Phase 03 - Step 06: Geographic visualisation (choropleths) for gini and mys.

Purpose:
    For each of {gini, mean_years_schooling}, produce a 2-panel figure:
      (left) latest year with broad coverage
      (right) panel-period mean
    Country fill is determined by ISO-3 code (locationmode="ISO-3").
    Static PNG export attempted via plotly's `kaleido` engine; on failure
    (Windows compatibility quirks have been intermittent), the script falls
    back to writing HTML output for the same content.

    "Latest year with broad coverage" is selected automatically as the
    most recent year in [1990, 2022] where observed N reaches >=80% of the
    variable's per-year peak coverage. The chosen year is reported to
    stdout.

Inputs:
    data/processed/panel.csv

Outputs:
    outputs/figures/phase03_s06_choropleth_gini.{png|html}
    outputs/figures/phase03_s06_choropleth_mys.{png|html}
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.paths import find_project_root  # noqa: E402

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

LATEST_YEAR_UPPER_BOUND = 2022   # 2023 often sparse due to publication lag
COVERAGE_THRESHOLD = 0.80        # year's N must be >= 80% of peak

# Variable-specific colour scales and direction.
# For gini and mys we use sequential scales; gini darker = more inequality,
# mys darker = more years of schooling. Both fine with the default direction.
VARIABLE_CONFIG = {
    "gini": {
        "title": "Gini index",
        "colorscale": "Reds",
        "value_range": None,  # auto from data
    },
    "mean_years_schooling": {
        "title": "Mean years of schooling",
        "colorscale": "Blues",
        "value_range": None,
    },
}

# Static export dimensions (per choropleth subplot pair).
EXPORT_WIDTH = 1600
EXPORT_HEIGHT = 700
EXPORT_SCALE = 2  # high-DPI


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def find_latest_broad_coverage_year(
    panel: pd.DataFrame,
    var: str,
    upper_bound: int = LATEST_YEAR_UPPER_BOUND,
    threshold: float = COVERAGE_THRESHOLD,
) -> tuple[int, int]:
    """Return (year, n_observed_in_year) for the latest year meeting threshold.

    The latest year (<= upper_bound) where observed N for `var` is at least
    `threshold` * peak observed N across all years. Falls back to argmax-N
    if no year meets the threshold.
    """
    year_n = panel.dropna(subset=[var]).groupby("year").size()
    year_n = year_n.loc[year_n.index <= upper_bound]
    if year_n.empty:
        raise RuntimeError(f"No observations for {var!r} <= {upper_bound}")

    peak = int(year_n.max())
    cutoff = peak * threshold
    eligible = year_n.loc[year_n >= cutoff]
    if eligible.empty:
        # Fallback: best year regardless of threshold.
        chosen = int(year_n.idxmax())
    else:
        chosen = int(eligible.index.max())
    return chosen, int(year_n.loc[chosen])


def panel_mean_per_country(panel: pd.DataFrame, var: str) -> pd.DataFrame:
    """Per-country mean of `var` across years (any country with >=1 observation)."""
    sub = panel.dropna(subset=[var])
    means = (
        sub.groupby("iso3")[var]
        .mean()
        .reset_index(name=var)
    )
    return means


def add_choropleth_trace(
    fig: go.Figure,
    df: pd.DataFrame,
    var: str,
    colorscale: str,
    zmin: float,
    zmax: float,
    colorbar_title: str,
    colorbar_x: float,
    row: int,
    col: int,
) -> None:
    """Add one choropleth trace to a subplots figure."""
    fig.add_trace(
        go.Choropleth(
            locations=df["iso3"],
            z=df[var],
            locationmode="ISO-3",
            colorscale=colorscale,
            zmin=zmin,
            zmax=zmax,
            colorbar=dict(
                title=dict(text=colorbar_title, side="right"),
                len=0.85,
                thickness=14,
                x=colorbar_x,
                xanchor="left",
            ),
            marker_line_color="white",
            marker_line_width=0.4,
            showscale=True,
        ),
        row=row, col=col,
    )


def render_variable(
    panel: pd.DataFrame,
    var: str,
    cfg: dict,
    out_dir: Path,
) -> dict:
    """Build the 2-panel choropleth for one variable. Return a small report dict."""
    latest_year, latest_n = find_latest_broad_coverage_year(panel, var)
    print(f"\n[{var}] latest broad-coverage year: {latest_year} (N={latest_n})")

    # --- Latest-year slice ---
    snap = panel.loc[panel["year"] == latest_year, ["iso3", var]].dropna(subset=[var])

    # --- Panel mean ---
    mean_df = panel_mean_per_country(panel, var)
    n_countries_mean = len(mean_df)
    print(f"[{var}] panel mean: countries with >=1 observation = {n_countries_mean}")

    # Shared color range across both panels for visual comparability.
    combined = pd.concat([snap[var], mean_df[var]], ignore_index=True)
    zmin = float(np.nanpercentile(combined, 2))
    zmax = float(np.nanpercentile(combined, 98))

    fig = make_subplots(
        rows=1, cols=2,
        specs=[[{"type": "choropleth"}, {"type": "choropleth"}]],
        subplot_titles=(
            f"{cfg['title']} — {latest_year} (N={len(snap)} countries)",
            f"{cfg['title']} — panel mean 1990–2023 (N={n_countries_mean} countries)",
        ),
        horizontal_spacing=0.05,
    )

    add_choropleth_trace(
        fig, snap, var,
        colorscale=cfg["colorscale"], zmin=zmin, zmax=zmax,
        colorbar_title=cfg["title"], colorbar_x=0.45,
        row=1, col=1,
    )
    add_choropleth_trace(
        fig, mean_df, var,
        colorscale=cfg["colorscale"], zmin=zmin, zmax=zmax,
        colorbar_title=cfg["title"], colorbar_x=1.0,
        row=1, col=2,
    )

    for geo_key in ("geo", "geo2"):
        fig.update_layout(**{
            geo_key: dict(
                showframe=False,
                showcoastlines=True,
                coastlinecolor="#888888",
                projection_type="natural earth",
                bgcolor="rgba(0,0,0,0)",
            )
        })

    fig.update_layout(
        title=dict(
            text=f"<b>{cfg['title']}</b> — country-level choropleth",
            x=0.5, xanchor="center",
            font=dict(size=16),
        ),
        margin=dict(l=10, r=10, t=80, b=20),
        paper_bgcolor="white",
    )

    short = "gini" if var == "gini" else "mys"
    base = out_dir / f"phase03_s06_choropleth_{short}"

    png_path = base.with_suffix(".png")
    html_path = base.with_suffix(".html")

    # Try PNG via kaleido; fall back to HTML.
    png_ok = False
    try:
        fig.write_image(
            str(png_path),
            format="png",
            width=EXPORT_WIDTH,
            height=EXPORT_HEIGHT,
            scale=EXPORT_SCALE,
            engine="kaleido",
        )
        png_ok = True
        print(f"[{var}] saved PNG: {png_path}")
    except Exception as exc:  # noqa: BLE001
        print(f"[{var}] PNG export failed ({type(exc).__name__}: {exc}). "
              f"Falling back to HTML.", file=sys.stderr)

    fig.write_html(str(html_path), include_plotlyjs="cdn")
    print(f"[{var}] saved HTML: {html_path}")

    return {
        "variable": var,
        "latest_year": latest_year,
        "latest_n_countries": len(snap),
        "panel_mean_n_countries": n_countries_mean,
        "value_range_p2_p98": (zmin, zmax),
        "png_saved": png_ok,
        "html_saved": True,
    }


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

    needed = ("year", "iso3", *VARIABLE_CONFIG.keys())
    missing = [c for c in needed if c not in panel.columns]
    if missing:
        print(f"ERROR: missing columns: {missing}", file=sys.stderr)
        return 1

    reports = []
    for var, cfg in VARIABLE_CONFIG.items():
        rep = render_variable(panel, var, cfg, fig_dir)
        reports.append(rep)

    # ---- Summary ------------------------------------------------------------
    print("\n--- Summary -----------------------------------------------------")
    for r in reports:
        png_str = "PNG ok" if r["png_saved"] else "PNG FAILED -> HTML only"
        zmin, zmax = r["value_range_p2_p98"]
        print(
            f"  {r['variable']:<25}  latest={r['latest_year']}  "
            f"latest_N={r['latest_n_countries']}  "
            f"mean_N={r['panel_mean_n_countries']}  "
            f"range≈[{zmin:.2f}, {zmax:.2f}]  ({png_str})"
        )

    if not all(r["png_saved"] for r in reports):
        print("\nNote: PNG export via kaleido failed for at least one figure.")
        print("HTML output is available; for portfolio embedding, either:")
        print("  - troubleshoot kaleido (e.g. `pip install --upgrade kaleido`), or")
        print("  - screenshot the HTML for notebook embedding.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
