"""
Phase 04 - Step 06: Cluster visualisations

Purpose:
    Produce three visualisations of the K=3 K-means cluster solution:

    1. PCA scatter (PC1 vs PC2) coloured by cluster, with cluster
       centroids and 25 watchlist countries annotated.
    2. Ward-linkage dendrogram with the K=3 cut height marked.
       Leaf labels are suppressed (167 leaves) - the message is
       structural shape, not country names.
    3. Geographic choropleth as interactive Plotly HTML (PNG export
       remains broken on this stack per Phase 03 Correction Note).

    All three figures share a consistent cluster colour scheme:
    Cluster 0 = blue, Cluster 1 = orange, Cluster 2 = green.

    Note: the dendrogram uses scipy's auto-coloring of sub-trees at
    the K=3 cut height. Those colours come from scipy's internal
    palette ordered by leaf position in the Ward tree, and are
    independent of the K-means cluster IDs - so dendrogram colours
    represent "Ward sub-trees" rather than the K-means clusters
    directly.

Inputs:
    data/processed/country_features_standardised.csv (167 rows; features)
    data/processed/panel.csv (Gini for choropleth hover)
    outputs/tables/phase04_s04_cluster_assignments.csv (K=3 K-means)

Outputs:
    outputs/figures/phase04_s06_pca_scatter.png  (300 dpi)
    outputs/figures/phase04_s06_dendrogram.png   (300 dpi)
    outputs/figures/phase04_s06_choropleth_clusters.html (self-contained)
"""

from __future__ import annotations

import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import plotly.express as px
from scipy.cluster.hierarchy import dendrogram, linkage
from sklearn.decomposition import PCA

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR.parent))

from src.paths import find_project_root  # noqa: E402

# ---- Configuration ----
SEED = 42
PRIMARY_CLUSTER_COL = "cluster_kmeans_k3"
GINI_WINDOW = (2010, 2019)

METADATA_COLS = ["iso3", "country_name", "region_name", "income_level_name"]

# Cluster colour scheme (matplotlib tab colours; CSS-equivalents for plotly)
CLUSTER_COLORS = {
    0: "#1f77b4",  # tab:blue
    1: "#ff7f0e",  # tab:orange
    2: "#2ca02c",  # tab:green
}

CLUSTER_LABELS_FULL = {
    0: "Low-development / Sub-Saharan-led",
    1: "Middle-development / Kuznets transition",
    2: "High-development / mature economies",
}

CLUSTER_LABELS_SHORT = {
    0: "C0 - Low-dev / SSA-led",
    1: "C1 - Kuznets transition",
    2: "C2 - Mature economies",
}

# Watchlist for PCA annotation: 25 portfolio-relevant countries
WATCHLIST = [
    "USA", "CHN", "JPN", "DEU", "GBR", "BRA", "IND", "IDN", "MEX", "ZAF",
    "NGA", "RUS", "ARG", "TUR", "EGY", "VNM", "ETH", "TZA", "PHL", "ITA",
    "FRA", "CAN", "AUS", "KOR", "SAU",
]


def stabilise_pca_signs(
    coords: np.ndarray,
    components: np.ndarray,
    df: pd.DataFrame,
    feature_cols: list[str],
) -> tuple[np.ndarray, np.ndarray]:
    """Flip PC signs so that:
        positive PC1 == higher mean_years_schooling (more developed)
        positive PC2 == higher services_value_added_gdp (more services-dominant)
    PCA components are sign-arbitrary; this gives a consistent reading.
    """
    mys_idx = feature_cols.index("mean_years_schooling")
    if np.corrcoef(coords[:, 0], df[feature_cols].values[:, mys_idx])[0, 1] < 0:
        coords[:, 0] *= -1
        components[0] *= -1

    if "services_value_added_gdp" in feature_cols:
        sv_idx = feature_cols.index("services_value_added_gdp")
        if np.corrcoef(coords[:, 1], df[feature_cols].values[:, sv_idx])[0, 1] < 0:
            coords[:, 1] *= -1
            components[1] *= -1

    return coords, components


def make_pca_scatter(
    df: pd.DataFrame,
    X_std: np.ndarray,
    feature_cols: list[str],
    out_path: Path,
) -> dict:
    """Save PCA scatter figure; return diagnostic dict."""
    pca = PCA(n_components=2, random_state=SEED)
    coords = pca.fit_transform(X_std)
    coords, components = stabilise_pca_signs(coords, pca.components_, df, feature_cols)

    var_pc1 = pca.explained_variance_ratio_[0] * 100
    var_pc2 = pca.explained_variance_ratio_[1] * 100

    fig, ax = plt.subplots(figsize=(13, 10))

    # Scatter by cluster
    df = df.reset_index(drop=True)
    for cid in sorted(CLUSTER_COLORS):
        mask = (df[PRIMARY_CLUSTER_COL] == cid).values
        ax.scatter(
            coords[mask, 0], coords[mask, 1],
            c=CLUSTER_COLORS[cid], s=55, alpha=0.7,
            edgecolors="white", linewidths=0.6,
            label=f"Cluster {cid}: {CLUSTER_LABELS_FULL[cid]} (n={int(mask.sum())})",
        )

    # Centroids
    for cid in sorted(CLUSTER_COLORS):
        mask = (df[PRIMARY_CLUSTER_COL] == cid).values
        cx, cy = coords[mask].mean(axis=0)
        ax.scatter(
            cx, cy, c=CLUSTER_COLORS[cid], s=320, marker="X",
            edgecolors="black", linewidths=2.0, zorder=5,
        )

    # Watchlist annotations
    annotated = 0
    for iso in WATCHLIST:
        idx = df.index[df["iso3"] == iso]
        if len(idx) == 0:
            continue
        i = int(idx[0])
        ax.annotate(
            iso,
            (coords[i, 0], coords[i, 1]),
            textcoords="offset points",
            xytext=(6, 6),
            fontsize=8,
            fontweight="bold",
            color="black",
        )
        annotated += 1

    # Reference axes through origin
    ax.axhline(0, color="gray", linewidth=0.5, alpha=0.5)
    ax.axvline(0, color="gray", linewidth=0.5, alpha=0.5)

    ax.set_xlabel(f"PC1 ({var_pc1:.1f}% variance explained)")
    ax.set_ylabel(f"PC2 ({var_pc2:.1f}% variance explained)")
    ax.set_title(
        f"Country clusters in PCA-2D space\n"
        f"K-means K=3 on {len(feature_cols)} standardised features (n={len(df)})"
    )
    ax.legend(loc="best", fontsize=9, frameon=True)
    ax.grid(alpha=0.3)

    fig.tight_layout()
    fig.savefig(out_path, dpi=300, bbox_inches="tight")
    plt.close(fig)

    diag = {
        "pc1_var": var_pc1,
        "pc2_var": var_pc2,
        "cumulative_var": var_pc1 + var_pc2,
        "watchlist_annotated": annotated,
        "watchlist_target": len(WATCHLIST),
    }
    return diag


def make_dendrogram(X_std: np.ndarray, n_obs: int, out_path: Path) -> dict:
    """Save Ward-linkage dendrogram with K=3 cut line."""
    Z = linkage(X_std, method="ward")
    # K=3 cut: between Z[-3, 2] (forms 3 clusters) and Z[-2, 2] (forms 2)
    cut_height = (Z[-3, 2] + Z[-2, 2]) / 2.0

    fig, ax = plt.subplots(figsize=(15, 7))
    dendrogram(
        Z,
        color_threshold=cut_height,
        ax=ax,
        no_labels=True,
        above_threshold_color="gray",
    )
    ax.axhline(
        cut_height, color="red", linestyle="--", linewidth=1.2,
        label=f"K=3 cut (h = {cut_height:.2f})",
    )
    ax.set_xlabel(
        f"Country (n={n_obs}; leaf labels suppressed for readability)",
    )
    ax.set_ylabel("Ward distance")
    ax.set_title(
        f"Hierarchical clustering (Ward linkage) on {n_obs} countries.  "
        f"Sub-tree colours below the cut are scipy auto-assigned and represent "
        f"Ward sub-trees, not K-means cluster IDs."
    )
    ax.legend(loc="upper right", fontsize=10)
    fig.tight_layout()
    fig.savefig(out_path, dpi=300, bbox_inches="tight")
    plt.close(fig)
    return {"cut_height": cut_height, "k3_height": Z[-3, 2], "k2_height": Z[-2, 2]}


def make_choropleth(df: pd.DataFrame, panel: pd.DataFrame, out_path: Path) -> dict:
    """Save plotly choropleth as self-contained HTML."""
    # Country-level Gini means for hover annotation
    gini_country = (
        panel[(panel["year"] >= GINI_WINDOW[0]) & (panel["year"] <= GINI_WINDOW[1])]
        .groupby("iso3")["gini"]
        .mean()
        .rename("gini_2010_2019")
        .reset_index()
    )
    df_plot = df.merge(gini_country, on="iso3", how="left").copy()

    # Map cluster id to short label for legend; full label for hover
    df_plot["cluster_short"] = df_plot[PRIMARY_CLUSTER_COL].map(CLUSTER_LABELS_SHORT)
    df_plot["cluster_full"] = df_plot[PRIMARY_CLUSTER_COL].map(CLUSTER_LABELS_FULL)

    # Strip whitespace on region for cleaner hover
    df_plot["region_name"] = df_plot["region_name"].str.strip()

    color_map = {CLUSTER_LABELS_SHORT[k]: CLUSTER_COLORS[k] for k in CLUSTER_COLORS}
    cluster_order = [CLUSTER_LABELS_SHORT[k] for k in sorted(CLUSTER_COLORS)]

    fig = px.choropleth(
        df_plot,
        locations="iso3",
        locationmode="ISO-3",
        color="cluster_short",
        hover_name="country_name",
        hover_data={
            "iso3": True,
            "cluster_short": False,  # already in legend
            "cluster_full": True,
            "region_name": True,
            "income_level_name": True,
            "gini_2010_2019": ":.2f",
        },
        color_discrete_map=color_map,
        category_orders={"cluster_short": cluster_order},
        labels={
            "cluster_short": "Cluster",
            "cluster_full": "Cluster (full)",
            "region_name": "WB Region",
            "income_level_name": "WB Income",
            "gini_2010_2019": "Gini (2010-2019 mean)",
        },
        title=(
            f"K-means K=3 country clusters  "
            f"(n={len(df)} countries; 50 dropped from listwise filter shown blank)"
        ),
    )

    fig.update_geos(
        projection_type="natural earth",
        showcountries=True,
        countrycolor="rgb(180, 180, 180)",
        showcoastlines=True,
        coastlinecolor="rgb(140, 140, 140)",
        landcolor="rgb(245, 245, 245)",
    )
    fig.update_layout(
        margin=dict(l=10, r=10, t=70, b=10),
        legend=dict(
            title=dict(text="Cluster"),
            x=0.01,
            y=0.10,
            xanchor="left",
            yanchor="bottom",
            bgcolor="rgba(255, 255, 255, 0.85)",
            bordercolor="rgba(0, 0, 0, 0.2)",
            borderwidth=1,
        ),
    )

    # Self-contained: include plotly.js inline so the file works offline
    # and on GitHub previewers without an internet connection.
    fig.write_html(out_path, include_plotlyjs=True, full_html=True)

    return {"countries_in_map": len(df_plot)}


def main() -> int:
    project_root = find_project_root(SCRIPT_DIR)
    std_path = project_root / "data" / "processed" / "country_features_standardised.csv"
    cluster_path = (
        project_root / "outputs" / "tables" / "phase04_s04_cluster_assignments.csv"
    )
    panel_path = project_root / "data" / "processed" / "panel.csv"

    out_pca = project_root / "outputs" / "figures" / "phase04_s06_pca_scatter.png"
    out_dendro = project_root / "outputs" / "figures" / "phase04_s06_dendrogram.png"
    out_choropleth = (
        project_root / "outputs" / "figures" / "phase04_s06_choropleth_clusters.html"
    )

    for p in (std_path, cluster_path, panel_path):
        if not p.exists():
            print(f"ERROR: required input not found at {p}")
            return 1

    std = pd.read_csv(std_path)
    clusters = pd.read_csv(cluster_path)
    panel = pd.read_csv(panel_path)

    feature_cols = [c for c in std.columns if c not in METADATA_COLS]

    # Inner-join cluster columns onto std (167 rows, all metadata + 7 features
    # + cluster columns)
    cluster_only_cols = ["iso3"] + [
        c for c in clusters.columns if c not in METADATA_COLS
    ]
    df = std.merge(clusters[cluster_only_cols], on="iso3", how="inner")
    print(f"Joined frame: {df.shape}")
    print(f"  features (used for PCA + Ward): {feature_cols}")

    X_std = df[feature_cols].values.astype(float)

    out_pca.parent.mkdir(parents=True, exist_ok=True)

    print("\n[1/3] PCA scatter ...")
    pca_diag = make_pca_scatter(df, X_std, feature_cols, out_pca)
    print(
        f"  PC1 variance: {pca_diag['pc1_var']:.1f}%  "
        f"PC2 variance: {pca_diag['pc2_var']:.1f}%  "
        f"cumulative: {pca_diag['cumulative_var']:.1f}%"
    )
    print(
        f"  watchlist annotated: "
        f"{pca_diag['watchlist_annotated']}/{pca_diag['watchlist_target']}"
    )
    print(f"  Wrote: {out_pca.relative_to(project_root)}")

    print("\n[2/3] Ward dendrogram ...")
    dendro_diag = make_dendrogram(X_std, len(df), out_dendro)
    print(
        f"  Z[-3] (3-cluster merge): {dendro_diag['k3_height']:.3f}; "
        f"Z[-2] (2-cluster merge): {dendro_diag['k2_height']:.3f}; "
        f"cut at: {dendro_diag['cut_height']:.3f}"
    )
    print(f"  Wrote: {out_dendro.relative_to(project_root)}")

    print("\n[3/3] Plotly choropleth ...")
    chor_diag = make_choropleth(df, panel, out_choropleth)
    print(f"  Countries in map: {chor_diag['countries_in_map']}")
    print(f"  Wrote: {out_choropleth.relative_to(project_root)}")
    print(
        f"  (HTML is self-contained with embedded plotly.js; "
        f"works offline and in GitHub preview)"
    )

    return 0


if __name__ == "__main__":
    sys.exit(main())
