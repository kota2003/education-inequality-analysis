"""
Phase 04 - Step 04: K-means and Ward clustering with robustness checks

Purpose:
    Fit clustering models on the 167-country standardised feature
    matrix and persist their cluster assignments for downstream steps.

    Per Step 03b, the primary clustering K is 3 (overriding the
    mechanical 2/4 consensus that picked K=2). K=2 and K=4 K-means
    are also fit as robustness comparators. Ward-linkage hierarchical
    clustering is fit at K=3 as a second-algorithm robustness check
    (Step 01 Decision 4).

    All cluster labels are canonicalised at output time by ascending
    mean of `mean_years_schooling` so that across-method comparisons
    have a consistent ordering: cluster 0 = lowest education,
    cluster K-1 = highest.

Inputs:
    data/processed/country_features_standardised.csv (167 rows;
    metadata + 7 standardised features)

Outputs:
    outputs/tables/phase04_s04_cluster_assignments.csv
        167 rows x 8 columns: iso3, country_name, region_name,
        income_level_name, cluster_kmeans_k3 (primary),
        cluster_kmeans_k2, cluster_kmeans_k4, cluster_ward_k3

    stdout: per-model cluster sizes, silhouette scores, K-means/Ward
    agreement (Adjusted Rand Index) at K=3 with confusion matrix.

Reproducibility:
    SEED = 42; KMeans n_init = 50 per Step 01 engineering decision.
    Ward linkage is deterministic given the input.
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.cluster.hierarchy import fcluster, linkage
from sklearn.cluster import KMeans
from sklearn.metrics import adjusted_rand_score, silhouette_score

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR.parent))

from src.paths import find_project_root  # noqa: E402

# ---- Configuration ----
SEED = 42
N_INIT = 50

K_PRIMARY = 3
K_ROBUSTNESS = [2, 4]

METADATA_COLS = ["iso3", "country_name", "region_name", "income_level_name"]
ORDERING_FEATURE = "mean_years_schooling"  # used to canonicalise cluster labels


def canonicalise_labels(labels: np.ndarray, ordering_values: np.ndarray) -> np.ndarray:
    """Relabel clusters so that cluster 0 has the smallest mean of
    ``ordering_values`` and cluster K-1 has the largest.

    The relabelling is a permutation of the original labels and does
    not change cluster membership; it only enforces a consistent
    semantic ordering across methods.
    """
    df = pd.DataFrame({"label": labels, "v": ordering_values})
    means = df.groupby("label")["v"].mean().sort_values()
    mapping = {original: new for new, original in enumerate(means.index)}
    return np.array([mapping[lbl] for lbl in labels])


def cluster_size_string(labels: np.ndarray) -> str:
    """Compact "{0: n0, 1: n1, ...}" representation of cluster sizes."""
    counts = pd.Series(labels).value_counts().sort_index()
    return "{" + ", ".join(f"{int(k)}: {int(v)}" for k, v in counts.items()) + "}"


def main() -> int:
    project_root = find_project_root(SCRIPT_DIR)
    std_path = project_root / "data" / "processed" / "country_features_standardised.csv"
    out_csv = project_root / "outputs" / "tables" / "phase04_s04_cluster_assignments.csv"

    if not std_path.exists():
        print(f"ERROR: standardised features CSV not found at {std_path}")
        return 1

    df = pd.read_csv(std_path)
    feature_cols = [c for c in df.columns if c not in METADATA_COLS]
    X = df[feature_cols].values.astype(float)

    print(f"Loaded standardised features: {X.shape}")
    print(f"  countries: {len(df)}")
    print(f"  features ({len(feature_cols)}): {feature_cols}")
    print(f"  primary K: {K_PRIMARY}; robustness K: {K_ROBUSTNESS}")
    print(f"  random_state: {SEED}, n_init: {N_INIT}")
    print(f"  cluster ordering feature: {ORDERING_FEATURE}")

    ordering_vals = df[ORDERING_FEATURE].values

    # ---- K-means at all three K ----
    print("\n--- K-means clustering ---")
    kmeans_labels: dict[int, np.ndarray] = {}
    kmeans_silhouette: dict[int, float] = {}
    for k in sorted([K_PRIMARY] + K_ROBUSTNESS):
        km = KMeans(n_clusters=k, n_init=N_INIT, random_state=SEED).fit(X)
        labels = canonicalise_labels(km.labels_, ordering_vals)
        kmeans_labels[k] = labels
        sil = silhouette_score(X, labels)
        kmeans_silhouette[k] = sil
        primary_tag = " <-- PRIMARY" if k == K_PRIMARY else ""
        print(
            f"  K={k}: cluster sizes = {cluster_size_string(labels)}, "
            f"silhouette = {sil:.3f}{primary_tag}"
        )

    # ---- Ward hierarchical at K_PRIMARY ----
    print(f"\n--- Ward hierarchical (K={K_PRIMARY}) ---")
    linkage_matrix = linkage(X, method="ward")
    # fcluster returns 1-indexed labels; subtract 1 then canonicalise
    ward_raw = fcluster(linkage_matrix, t=K_PRIMARY, criterion="maxclust") - 1
    ward_labels = canonicalise_labels(ward_raw, ordering_vals)
    ward_silhouette = silhouette_score(X, ward_labels)
    print(
        f"  cluster sizes = {cluster_size_string(ward_labels)}, "
        f"silhouette = {ward_silhouette:.3f}"
    )

    # ---- K-means K=3 vs Ward K=3 agreement ----
    print(f"\n--- K-means K={K_PRIMARY} vs Ward K={K_PRIMARY} agreement ---")
    ari = adjusted_rand_score(kmeans_labels[K_PRIMARY], ward_labels)
    print(f"  Adjusted Rand Index: {ari:.3f}")
    print("  Confusion matrix (rows = K-means, cols = Ward):")
    confusion = pd.crosstab(
        pd.Series(kmeans_labels[K_PRIMARY], name="K-means"),
        pd.Series(ward_labels, name="Ward"),
    )
    # Print with a small indent for readability
    confusion_str = confusion.to_string()
    for line in confusion_str.split("\n"):
        print(f"    {line}")

    # ---- Cross-K K-means agreement (K=2 vs K=3, K=3 vs K=4) ----
    print("\n--- Cross-K K-means agreement ---")
    ari_k2_k3 = adjusted_rand_score(kmeans_labels[2], kmeans_labels[K_PRIMARY])
    ari_k3_k4 = adjusted_rand_score(kmeans_labels[K_PRIMARY], kmeans_labels[4])
    print(f"  K=2 vs K=3 ARI: {ari_k2_k3:.3f}")
    print(f"  K=3 vs K=4 ARI: {ari_k3_k4:.3f}")

    # ---- Silhouette comparison summary ----
    print("\n--- Silhouette comparison summary ---")
    print(f"  K=2 (K-means):              {kmeans_silhouette[2]:.3f}")
    primary_tag = "  <-- PRIMARY"
    print(f"  K={K_PRIMARY} (K-means):              {kmeans_silhouette[K_PRIMARY]:.3f}{primary_tag}")
    print(f"  K={K_PRIMARY} (Ward hierarchical):    {ward_silhouette:.3f}")
    print(f"  K=4 (K-means):              {kmeans_silhouette[4]:.3f}")

    # ---- Save assignments CSV ----
    out_df = df[METADATA_COLS].copy()
    out_df[f"cluster_kmeans_k{K_PRIMARY}"] = kmeans_labels[K_PRIMARY]
    for k in K_ROBUSTNESS:
        out_df[f"cluster_kmeans_k{k}"] = kmeans_labels[k]
    out_df[f"cluster_ward_k{K_PRIMARY}"] = ward_labels

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(out_csv, index=False)
    print(f"\nWrote: {out_csv.relative_to(project_root)}")
    print(f"  shape: {out_df.shape}")
    print(f"  columns: {list(out_df.columns)}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
