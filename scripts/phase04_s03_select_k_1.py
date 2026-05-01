"""
Phase 04 - Step 03: K selection diagnostics

Purpose:
    Compute four K-selection diagnostics on the 167-country
    standardised feature matrix and recommend K via the multi-method
    consensus rule fixed in Phase 04 Step 01 Decision 6:

      - Elbow (within-cluster sum of squares; argmax of second
        difference of log(WCSS))
      - Silhouette (mean silhouette score; argmax)
      - Calinski-Harabasz index (argmax)
      - Gap statistic (Tibshirani; 1-SE rule)

    Decision rule: if at least two of the four diagnostics agree on a
    K, recommend that K. Otherwise, default to the Silhouette pick and
    document the disagreement in stdout.

    The K range searched is 2..10. The gap statistic is computed on
    K=2..11 internally so the 1-SE rule can validate K up to 10.

Inputs:
    data/processed/country_features_standardised.csv

Outputs:
    outputs/figures/phase04_s03_k_selection.png  (2x2 diagnostic panel)
    outputs/tables/phase04_s03_k_diagnostics.csv (K, wcss, silhouette,
                                                  calinski_harabasz,
                                                  gap, gap_se)

Reproducibility:
    SEED = 42; KMeans n_init = 50 on data, 10 on gap references; gap
    references = 50 uniform draws over the data feature ranges.
"""

from __future__ import annotations

import sys
from collections import Counter
from pathlib import Path

import matplotlib

matplotlib.use("Agg")  # non-interactive backend; safe under nbconvert
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.metrics import calinski_harabasz_score, silhouette_score

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR.parent))

from src.paths import find_project_root  # noqa: E402

# ---- Configuration (Step 01 engineering decisions) ----
SEED = 42
N_INIT = 50  # for data K-means; n_init for gap references is 10
N_REFS = 50  # gap statistic reference draws per K

# K=2..10 is the displayed/recommended range.
# The gap statistic needs gap(k+1) for the 1-SE rule, so we compute
# gap on K=2..11 internally.
K_RANGE = list(range(2, 11))
GAP_K_RANGE = list(range(2, 12))

METADATA_COLS = ["iso3", "country_name", "region_name", "income_level_name"]


def compute_gap_statistic(
    X: np.ndarray,
    k_range: list[int],
    n_refs: int,
    random_state: int,
) -> tuple[np.ndarray, np.ndarray]:
    """Tibshirani-style gap statistic with uniform-range reference distribution.

    Returns
    -------
    gaps : ndarray (len(k_range),)
        gap(k) = mean(log W_k_ref) - log W_k_data
    gap_se : ndarray (len(k_range),)
        Tibshirani's adjusted SE: sd(log W_k_ref) * sqrt(1 + 1/n_refs)
    """
    mins = X.min(axis=0)
    maxs = X.max(axis=0)
    n_samples, n_features = X.shape
    rng = np.random.default_rng(random_state)

    log_wks = []
    log_wkbs_mean = []
    log_wkbs_sd = []

    for k in k_range:
        # Data KMeans
        km = KMeans(n_clusters=k, n_init=N_INIT, random_state=random_state).fit(X)
        log_wks.append(np.log(km.inertia_))

        # Reference draws
        ref_log_wks = np.empty(n_refs)
        for i in range(n_refs):
            ref = rng.uniform(mins, maxs, size=(n_samples, n_features))
            km_ref = KMeans(
                n_clusters=k, n_init=10, random_state=random_state + i
            ).fit(ref)
            ref_log_wks[i] = np.log(km_ref.inertia_)
        log_wkbs_mean.append(ref_log_wks.mean())
        log_wkbs_sd.append(ref_log_wks.std(ddof=1))

    log_wks = np.array(log_wks)
    log_wkbs_mean = np.array(log_wkbs_mean)
    log_wkbs_sd = np.array(log_wkbs_sd)

    gaps = log_wkbs_mean - log_wks
    gap_se = log_wkbs_sd * np.sqrt(1 + 1.0 / n_refs)
    return gaps, gap_se


def gap_1se_recommendation(
    gap_k_range: list[int],
    gaps: np.ndarray,
    gap_se: np.ndarray,
    target_range: list[int],
) -> int:
    """Apply Tibshirani's 1-SE rule: smallest k in `target_range` such that
    gap(k) >= gap(k+1) - se(k+1). Falls back to argmax(gap) on `target_range`."""
    for k in target_range:
        if k not in gap_k_range or (k + 1) not in gap_k_range:
            continue
        idx_k = gap_k_range.index(k)
        idx_k1 = gap_k_range.index(k + 1)
        if gaps[idx_k] >= gaps[idx_k1] - gap_se[idx_k1]:
            return k
    # Fallback (no k satisfies the rule): argmax over the displayed range
    target_indices = [gap_k_range.index(k) for k in target_range if k in gap_k_range]
    target_gaps = [gaps[i] for i in target_indices]
    return target_range[int(np.argmax(target_gaps))]


def find_elbow_k(ks: list[int], wcss: list[float]) -> int:
    """Return K at which log(WCSS) bends most (argmax of second difference).

    For ks = [k0, k1, ..., kN], second_diff[i] is computed at the interior
    points i = 1..N-1; the K returned is the one with the largest second
    difference, i.e. the most pronounced "elbow".
    """
    log_w = np.log(np.asarray(wcss))
    second_diff = log_w[:-2] - 2.0 * log_w[1:-1] + log_w[2:]
    interior_idx = int(np.argmax(second_diff))
    return ks[interior_idx + 1]


def main() -> int:
    project_root = find_project_root(SCRIPT_DIR)
    std_path = project_root / "data" / "processed" / "country_features_standardised.csv"
    out_fig = project_root / "outputs" / "figures" / "phase04_s03_k_selection.png"
    out_csv = project_root / "outputs" / "tables" / "phase04_s03_k_diagnostics.csv"

    if not std_path.exists():
        print(f"ERROR: standardised features CSV not found at {std_path}")
        return 1

    df = pd.read_csv(std_path)
    feature_cols = [c for c in df.columns if c not in METADATA_COLS]
    X = df[feature_cols].values.astype(float)

    print(f"Loaded standardised features: {X.shape}")
    print(f"  countries: {len(df)}")
    print(f"  features ({len(feature_cols)}): {feature_cols}")
    print(f"  K range: {K_RANGE[0]}..{K_RANGE[-1]} (gap internally to {GAP_K_RANGE[-1]})")
    print(f"  random_state: {SEED}, n_init data: {N_INIT}, gap refs: {N_REFS}")

    # ---- 1. WCSS / Silhouette / Calinski-Harabasz on K_RANGE ----
    print("\n[1/2] Computing WCSS, Silhouette, Calinski-Harabasz over K_RANGE...")
    wcss: list[float] = []
    sils: list[float] = []
    chs: list[float] = []
    for k in K_RANGE:
        km = KMeans(n_clusters=k, n_init=N_INIT, random_state=SEED).fit(X)
        wcss.append(km.inertia_)
        sils.append(silhouette_score(X, km.labels_))
        chs.append(calinski_harabasz_score(X, km.labels_))
        print(
            f"  K={k:2d}  WCSS={km.inertia_:8.2f}  "
            f"silhouette={sils[-1]:6.3f}  CH={chs[-1]:8.2f}"
        )

    # ---- 2. Gap statistic on GAP_K_RANGE ----
    print(
        f"\n[2/2] Computing Gap statistic ({N_REFS} refs per K) "
        f"over K=2..{GAP_K_RANGE[-1]}..."
    )
    gaps, gap_se = compute_gap_statistic(X, GAP_K_RANGE, N_REFS, SEED)
    for i, k in enumerate(GAP_K_RANGE):
        print(f"  K={k:2d}  gap={gaps[i]:6.3f}  gap_se={gap_se[i]:6.3f}")

    # ---- 3. Per-method preferred K ----
    elbow_k = find_elbow_k(K_RANGE, wcss)
    sil_k = K_RANGE[int(np.argmax(sils))]
    ch_k = K_RANGE[int(np.argmax(chs))]
    gap_k = gap_1se_recommendation(GAP_K_RANGE, gaps, gap_se, K_RANGE)

    print("\nPer-method preferred K:")
    print(f"  Elbow             : K = {elbow_k}")
    print(f"  Silhouette        : K = {sil_k}")
    print(f"  Calinski-Harabasz : K = {ch_k}")
    print(f"  Gap (1-SE rule)   : K = {gap_k}")

    # ---- 4. Final recommendation ----
    prefs = [elbow_k, sil_k, ch_k, gap_k]
    counts = Counter(prefs)
    most_common_k, count = counts.most_common(1)[0]
    if count >= 2:
        rec_k = most_common_k
        rec_reason = f"consensus: {count}/4 diagnostics agree on K={rec_k}"
    else:
        rec_k = sil_k
        rec_reason = (
            "no 2-method consensus; defaulting to Silhouette pick "
            f"(K={sil_k}) per Step 01 Decision 6"
        )

    print(f"\nRECOMMENDED K = {rec_k}")
    print(f"  ({rec_reason})")

    # ---- 5. Save diagnostics CSV ----
    diag_rows = []
    for i, k in enumerate(K_RANGE):
        diag_rows.append(
            {
                "K": k,
                "wcss": wcss[i],
                "silhouette": sils[i],
                "calinski_harabasz": chs[i],
                "gap": gaps[GAP_K_RANGE.index(k)],
                "gap_se": gap_se[GAP_K_RANGE.index(k)],
            }
        )
    diag_df = pd.DataFrame(diag_rows)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    diag_df.to_csv(out_csv, index=False)
    print(f"\nWrote: {out_csv.relative_to(project_root)}")

    # ---- 6. 2x2 figure ----
    fig, axes = plt.subplots(2, 2, figsize=(12, 10))

    # Panel: Elbow (WCSS)
    ax = axes[0, 0]
    ax.plot(K_RANGE, wcss, "o-", color="C0")
    ax.axvline(
        elbow_k, color="C0", linestyle="--", alpha=0.6,
        label=f"Elbow pick: K = {elbow_k}",
    )
    ax.set_xlabel("K")
    ax.set_ylabel("Within-cluster sum of squares (inertia)")
    ax.set_title("Elbow (WCSS)")
    ax.set_xticks(K_RANGE)
    ax.legend()
    ax.grid(alpha=0.3)

    # Panel: Silhouette
    ax = axes[0, 1]
    ax.plot(K_RANGE, sils, "o-", color="C1")
    ax.axvline(
        sil_k, color="C1", linestyle="--", alpha=0.6,
        label=f"Silhouette pick: K = {sil_k}",
    )
    ax.set_xlabel("K")
    ax.set_ylabel("Mean silhouette score")
    ax.set_title("Silhouette")
    ax.set_xticks(K_RANGE)
    ax.legend()
    ax.grid(alpha=0.3)

    # Panel: Calinski-Harabasz
    ax = axes[1, 0]
    ax.plot(K_RANGE, chs, "o-", color="C2")
    ax.axvline(
        ch_k, color="C2", linestyle="--", alpha=0.6,
        label=f"CH pick: K = {ch_k}",
    )
    ax.set_xlabel("K")
    ax.set_ylabel("Calinski-Harabasz index")
    ax.set_title("Calinski-Harabasz")
    ax.set_xticks(K_RANGE)
    ax.legend()
    ax.grid(alpha=0.3)

    # Panel: Gap statistic with error bars
    ax = axes[1, 1]
    gap_vals_disp = [gaps[GAP_K_RANGE.index(k)] for k in K_RANGE]
    gap_se_vals_disp = [gap_se[GAP_K_RANGE.index(k)] for k in K_RANGE]
    ax.errorbar(
        K_RANGE, gap_vals_disp, yerr=gap_se_vals_disp,
        fmt="o-", color="C3", capsize=3,
    )
    ax.axvline(
        gap_k, color="C3", linestyle="--", alpha=0.6,
        label=f"Gap (1-SE rule) pick: K = {gap_k}",
    )
    ax.set_xlabel("K")
    ax.set_ylabel("Gap statistic")
    ax.set_title(f"Gap statistic ({N_REFS} reference draws; error bars = 1 SE)")
    ax.set_xticks(K_RANGE)
    ax.legend()
    ax.grid(alpha=0.3)

    fig.suptitle(
        f"K selection diagnostics  "
        f"(n = {len(X)} countries, {X.shape[1]} standardised features)\n"
        f"Recommended K = {rec_k}  ({rec_reason})",
        fontsize=12,
    )
    fig.tight_layout()
    out_fig.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_fig, dpi=300, bbox_inches="tight")
    plt.close(fig)
    print(f"Wrote: {out_fig.relative_to(project_root)}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
