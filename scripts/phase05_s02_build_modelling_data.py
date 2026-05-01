"""
Phase 05 - Step 02: Build modelling-ready panel.

Purpose:
    Construct `data/processed/panel_modelling.csv` from the analytical
    panel and the Phase 04 cluster assignments. Two operations:

    1. Apply log transforms to `gdp_per_capita_ppp` and `population`
       (Phase 02 Decision 4: panel stores raw values; modelling-time
       phase owns np.log). Skewness reference (Phase 03 Step 02 /
       kickoff §2.2): gdp_per_capita_ppp raw skew 1.87 -> log skew
       -0.16; population raw skew 9.03 -> log skew -0.40.
    2. Left-join the four Phase 04 cluster columns (cluster_kmeans_k3
       primary, plus k2/k4/ward_k3 as Phase 04 §Handoff robustness
       comparators) onto each country-year row.

    The panel itself is unchanged in shape (7,378 rows). Six new columns
    are added (2 log + 4 cluster). 167 of 217 countries receive a
    cluster assignment; the remaining 50 stay NaN per Phase 04
    listwise-drop typology (Phase 04 §Known Issues - 50-country MNAR
    list).

Inputs:
    data/processed/panel.csv
    outputs/tables/phase04_s04_cluster_assignments.csv

Outputs:
    data/processed/panel_modelling.csv
"""

from __future__ import annotations

import random
import sys
from pathlib import Path

import numpy as np
import pandas as pd

# Reproducibility
SEED = 42
random.seed(SEED)
np.random.seed(SEED)

# Make `src` importable regardless of where the script is invoked from.
_SCRIPT_DIR = Path(__file__).resolve().parent
_PROJECT_ROOT_GUESS = _SCRIPT_DIR.parent
if str(_PROJECT_ROOT_GUESS) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT_GUESS))

from src.io_utils import read_csv_with_encoding_fallback  # noqa: E402
from src.paths import find_project_root  # noqa: E402


# Variables that Phase 05 log-transforms at modelling time per Phase 02
# Decision 4. Both raw distributions are strictly positive in the panel.
LOG_TRANSFORM_VARS: tuple[str, ...] = (
    "gdp_per_capita_ppp",
    "population",
)

# Cluster columns to bring across from Phase 04. k3 is the headline
# (Spec C interaction term); the rest are retained as Phase 04 §Handoff
# robustness comparators. Marginal storage cost is negligible and
# avoids re-joining downstream.
CLUSTER_COLUMNS: tuple[str, ...] = (
    "cluster_kmeans_k3",
    "cluster_kmeans_k2",
    "cluster_kmeans_k4",
    "cluster_ward_k3",
)

# Spec A RHS variables (post-log-transform names). Used only for the
# DoD listwise-sample sanity check against the Phase 02 anchor
# (~1,423 country-years, ~140 countries).
SPEC_A_RHS: tuple[str, ...] = (
    "mean_years_schooling",
    "enrol_secondary",
    "log_gdp_per_capita_ppp",
    "log_population",
    "urban_population_pct",
)


def _format_int(n: int) -> str:
    """Format int with thousands separator, right-aligned width 6."""
    return f"{n:>6,}"


def main() -> None:
    project_root = find_project_root()
    panel_path = project_root / "data" / "processed" / "panel.csv"
    cluster_path = (
        project_root / "outputs" / "tables" / "phase04_s04_cluster_assignments.csv"
    )
    output_path = project_root / "data" / "processed" / "panel_modelling.csv"

    print(f"[INFO] Project root:   {project_root}")
    print(f"[INFO] Panel input:    {panel_path}")
    print(f"[INFO] Cluster input:  {cluster_path}")
    print(f"[INFO] Output:         {output_path}")
    print()

    # ---------- 1. Load inputs --------------------------------------------------

    panel, panel_enc = read_csv_with_encoding_fallback(panel_path)
    print(
        f"[LOAD] panel.csv ({panel_enc}): "
        f"{panel.shape[0]:,} rows x {panel.shape[1]} cols"
    )
    if panel.shape != (7378, 24):
        print(f"[WARN] Expected (7378, 24); got {panel.shape}")

    clusters, cluster_enc = read_csv_with_encoding_fallback(cluster_path)
    print(
        f"[LOAD] phase04_s04_cluster_assignments.csv ({cluster_enc}): "
        f"{clusters.shape[0]:,} rows x {clusters.shape[1]} cols"
    )
    if clusters.shape[0] != 167:
        print(f"[WARN] Expected 167 cluster rows; got {clusters.shape[0]}")

    missing_cluster_cols = [c for c in CLUSTER_COLUMNS if c not in clusters.columns]
    if missing_cluster_cols:
        raise KeyError(
            f"Cluster file missing expected columns: {missing_cluster_cols}. "
            f"Available columns: {list(clusters.columns)}"
        )

    # Phase 04 wrap finding: K-means K=3 sizes are 40 / 59 / 68 (canonicalised
    # by ascending mean_years_schooling).
    k3_source_counts = clusters["cluster_kmeans_k3"].value_counts().sort_index()
    print(f"[CHECK] Source cluster_kmeans_k3 distribution (N countries):")
    for k, n in k3_source_counts.items():
        print(f"          cluster {int(k)}: {int(n)}")
    expected_k3 = {0: 40, 1: 59, 2: 68}
    actual_k3 = {int(k): int(n) for k, n in k3_source_counts.items()}
    if actual_k3 != expected_k3:
        print(f"[WARN] cluster_kmeans_k3 source distribution {actual_k3} != "
              f"expected {expected_k3}")

    # ---------- 2. Apply log transforms ----------------------------------------

    print()
    print("[STEP] Applying log transforms (Phase 02 Decision 4)...")
    for var in LOG_TRANSFORM_VARS:
        if var not in panel.columns:
            raise KeyError(f"Panel missing variable: {var}")
        log_col = f"log_{var}"
        # Validate strict positivity before transforming - log of zero or
        # negative would silently produce -inf or NaN and propagate downstream.
        nonpos_mask = panel[var].dropna() <= 0
        if nonpos_mask.any():
            raise ValueError(
                f"{var} contains {int(nonpos_mask.sum())} non-positive values; "
                f"cannot apply log transform."
            )
        n_raw_nan = int(panel[var].isna().sum())
        panel[log_col] = np.log(panel[var])
        n_log_nan = int(panel[log_col].isna().sum())
        print(
            f"  {var:21s} -> {log_col:25s} "
            f"NaN: raw={n_raw_nan:>5,}  log={n_log_nan:>5,}"
        )
        if n_log_nan != n_raw_nan:
            print(f"[WARN] log transform changed NaN count for {var}")

    # ---------- 3. Left-join cluster columns ----------------------------------

    print()
    print("[STEP] Attaching cluster columns (left-join on iso3, many_to_one)...")
    cluster_subset = clusters[["iso3", *CLUSTER_COLUMNS]].copy()
    n_panel_pre = len(panel)
    panel_columns_pre = list(panel.columns)
    panel_modelling = panel.merge(
        cluster_subset,
        on="iso3",
        how="left",
        validate="many_to_one",
    )
    n_panel_post = len(panel_modelling)
    if n_panel_pre != n_panel_post:
        raise RuntimeError(
            f"Row count changed during merge: {n_panel_pre:,} -> {n_panel_post:,}. "
            f"Many-to-one left-join must preserve panel rows."
        )
    print(
        f"  rows before: {n_panel_pre:,}  rows after: {n_panel_post:,}  "
        f"(preserved)"
    )

    # ---------- 4. DoD: no NaN introduced beyond panel.csv -------------------

    print()
    print("[CHECK] NaN parity for original panel columns (DoD requirement):")
    nan_diffs: list[tuple[str, int, int]] = []
    for col in panel_columns_pre:
        pre = int(panel[col].isna().sum())
        post = int(panel_modelling[col].isna().sum())
        if pre != post:
            nan_diffs.append((col, pre, post))
    if nan_diffs:
        print(f"  [WARN] {len(nan_diffs)} columns saw NaN count change:")
        for col, pre, post in nan_diffs:
            print(f"            {col}: {pre} -> {post}")
    else:
        print(f"  [OK] All {len(panel_columns_pre)} original columns preserved NaN counts.")

    # ---------- 5. DoD: cluster_kmeans_k3 panel-space distribution -----------

    print()
    print("[CHECK] cluster_kmeans_k3 distribution in panel rows:")
    cy_counts = (
        panel_modelling["cluster_kmeans_k3"]
        .value_counts(dropna=False)
        .sort_index(na_position="last")
    )
    expected_panel = {0: 40 * 34, 1: 59 * 34, 2: 68 * 34}
    expected_nan = 50 * 34
    for k, n in cy_counts.items():
        n = int(n)
        if pd.isna(k):
            label = "NaN"
            expected = expected_nan
        else:
            label = f"cluster {int(k)}"
            expected = expected_panel[int(k)]
        flag = "OK" if n == expected else "WARN"
        print(
            f"  {label:11s}: {n:>5,} country-years  "
            f"(expected {expected:>5,}, {flag})"
        )

    countries_with_cluster = int(
        panel_modelling.dropna(subset=["cluster_kmeans_k3"])["iso3"].nunique()
    )
    countries_without_cluster = int(
        panel_modelling[panel_modelling["cluster_kmeans_k3"].isna()]["iso3"].nunique()
    )
    print(f"  Countries with cluster:    {countries_with_cluster}  (expected 167)")
    print(f"  Countries without cluster: {countries_without_cluster}  (expected 50)")
    if countries_with_cluster != 167 or countries_without_cluster != 50:
        print(
            f"[WARN] Country counts off: with={countries_with_cluster}, "
            f"without={countries_without_cluster}"
        )

    # ---------- 6. Spec A listwise sample sanity check -----------------------

    print()
    print(f"[CHECK] Spec A listwise sample (gini + {len(SPEC_A_RHS)} RHS):")
    spec_a_cols = ["gini", *SPEC_A_RHS]
    missing_spec_cols = [c for c in spec_a_cols if c not in panel_modelling.columns]
    if missing_spec_cols:
        raise KeyError(f"Spec A columns missing from panel_modelling: {missing_spec_cols}")
    spec_a_complete = panel_modelling.dropna(subset=spec_a_cols)
    print(
        f"  Country-years: {_format_int(len(spec_a_complete))}  "
        f"(Phase 02 anchor: ~1,423)"
    )
    print(
        f"  Countries:     {_format_int(spec_a_complete['iso3'].nunique())}  "
        f"(Phase 02 anchor: ~140)"
    )

    # ---------- 7. Write output ---------------------------------------------

    print()
    print(f"[STEP] Writing {output_path}...")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    panel_modelling.to_csv(output_path, index=False, encoding="utf-8")
    out_size_mb = output_path.stat().st_size / (1024 * 1024)
    print(
        f"[OK] panel_modelling.csv: "
        f"{panel_modelling.shape[0]:,} rows x {panel_modelling.shape[1]} cols  "
        f"({out_size_mb:.2f} MB)"
    )

    new_cols = [c for c in panel_modelling.columns if c not in panel_columns_pre]
    print(f"[OK] New columns added ({len(new_cols)}): {new_cols}")


if __name__ == "__main__":
    main()
