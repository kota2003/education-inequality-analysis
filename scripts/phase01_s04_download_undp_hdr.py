"""
Phase 01 - Step 04: Download UNDP HDR composite indices time series.

Purpose:
    Download the UNDP Human Development Report composite indices CSV
    (HDR 2025 vintage, covering 1990–2023) from the URL declared in
    `data/raw/manifest.yaml`. Save the raw file unchanged, then inspect
    the structure: confirm the presence of key columns (iso3, country,
    region) and that mean-years-of-schooling columns (mys_YYYY) exist
    for every year in the project's Scope coverage.

    Wide-to-long reshaping and canonicalisation to ISO-3 are deferred to
    Phase 02 per the Q4 decision (raw layer preserves source-native shape).

    (Step 03 was absorbed into Step 02 — UNESCO UIS variables are taken
    from the WB mirror, so there is no separate Step 03 script.)

Inputs:
    data/raw/manifest.yaml   (URL + output path)

Outputs:
    data/raw/undp_hdr/hdr_composite_indices.csv
    outputs/tables/phase01_s04_undp_hdr_report.csv

Notes:
    - No stochastic components, so no random seed is set.
    - HDR CSVs are historically published in Windows-1252 / Latin-1, not
      UTF-8. The inspection step tries encodings in order and records
      which one succeeded. The raw file is stored as-received; encoding
      normalisation to UTF-8 is deferred to Phase 02.
    - Transient network errors (Timeout, ConnectionError) are retried
      with exponential backoff. Non-network errors are terminal.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

import pandas as pd
import requests
import yaml
from requests.exceptions import ConnectionError as ReqConnectionError
from requests.exceptions import Timeout

PROJECT_ROOT = Path(__file__).resolve().parents[1]
MANIFEST_PATH = PROJECT_ROOT / "data" / "raw" / "manifest.yaml"
REPORT_DIR = PROJECT_ROOT / "outputs" / "tables"
REPORT_OUT = REPORT_DIR / "phase01_s04_undp_hdr_report.csv"

REQUEST_TIMEOUT_S = 60.0
MAX_RETRIES = 3
RETRY_BACKOFF_S = 3.0

START_YEAR = 1990
END_YEAR = 2023

# Order matters. utf-8-sig handles a possible BOM; cp1252 is a superset of
# Latin-1 that covers Windows-exported CSVs; latin-1 is a guaranteed fallback
# (every byte maps to a valid char, though possibly not the intended one).
ENCODING_CANDIDATES = ("utf-8", "utf-8-sig", "cp1252", "latin-1")


# ---------------------------------------------------------------------------
# Manifest
# ---------------------------------------------------------------------------

def load_manifest(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------

def download_file(url: str, dest: Path) -> bool:
    """Stream-download a file with retry on transient network errors."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    last_error: str | None = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with requests.get(url, timeout=REQUEST_TIMEOUT_S, stream=True) as resp:
                resp.raise_for_status()
                total_bytes = 0
                with dest.open("wb") as f:
                    for chunk in resp.iter_content(chunk_size=65536):
                        if chunk:
                            f.write(chunk)
                            total_bytes += len(chunk)
                print(f"    downloaded {total_bytes:,} bytes (attempt {attempt})")
                return True
        except (Timeout, ReqConnectionError) as exc:
            last_error = f"{type(exc).__name__}: {exc}"
            if attempt < MAX_RETRIES:
                print(f"    attempt {attempt} transient error — retrying")
                time.sleep(RETRY_BACKOFF_S * attempt)
                continue
            print(f"    FAILED after {MAX_RETRIES} attempts — {last_error}")
            return False
        except Exception as exc:
            print(f"    terminal error: {type(exc).__name__}: {exc}")
            return False
    return False


# ---------------------------------------------------------------------------
# Robust CSV reader (encoding fallback)
# ---------------------------------------------------------------------------

def read_csv_with_encoding_fallback(path: Path) -> tuple[pd.DataFrame, str]:
    """
    Try reading the CSV with a sequence of likely encodings. Return the
    DataFrame and the encoding that succeeded.
    """
    last_error: Exception | None = None
    for enc in ENCODING_CANDIDATES:
        try:
            df = pd.read_csv(path, encoding=enc)
            return df, enc
        except UnicodeDecodeError as exc:
            last_error = exc
            continue
    # latin-1 cannot raise UnicodeDecodeError, so we should never reach here
    raise RuntimeError(
        f"Could not decode {path} with any of {ENCODING_CANDIDATES}. "
        f"Last error: {last_error!r}"
    )


# ---------------------------------------------------------------------------
# Structure inspection
# ---------------------------------------------------------------------------

def inspect(csv_path: Path) -> pd.DataFrame:
    """Read the CSV and print a structural summary. Returns the DataFrame."""
    print(f"\nReading back: {csv_path}")
    df, used_encoding = read_csv_with_encoding_fallback(csv_path)
    print(f"  encoding used : {used_encoding}")
    print(f"  rows          : {len(df):,}")
    print(f"  cols          : {len(df.columns)}")

    first_cols = list(df.columns[:10])
    print(f"  first 10 columns: {first_cols}")

    for key in ("iso3", "country", "hdicode", "region"):
        flag = "present" if key in df.columns else "MISSING"
        print(f"  column '{key}': {flag}")

    prefixes = ("hdi", "le", "eys", "mys", "gnipc", "gdi", "gii", "ihdi", "phdi")
    print("\n  Detected time-series column families:")
    for prefix in prefixes:
        ys = sorted(
            int(c.split("_", 1)[1])
            for c in df.columns
            if c.startswith(f"{prefix}_") and c.split("_", 1)[1].isdigit()
        )
        if ys:
            print(f"    {prefix:7s} : {len(ys)} cols, years {min(ys)}–{max(ys)}")

    return df


def build_mys_report(df: pd.DataFrame) -> pd.DataFrame:
    """Year-by-year non-null count for mean-years-of-schooling."""
    rows = []
    expected_years = list(range(START_YEAR, END_YEAR + 1))
    actual_mys_years = set()
    for col in df.columns:
        if col.startswith("mys_"):
            suffix = col.split("_", 1)[1]
            if suffix.isdigit():
                actual_mys_years.add(int(suffix))

    for year in expected_years:
        col = f"mys_{year}"
        present = col in df.columns
        non_null = int(df[col].notna().sum()) if present else 0
        rows.append(
            {
                "year": year,
                "column": col,
                "present": present,
                "non_null": non_null,
            }
        )
    report = pd.DataFrame(rows)

    missing_years = sorted(set(expected_years) - actual_mys_years)
    extra_years = sorted(actual_mys_years - set(expected_years))
    print("\n  Mean Years of Schooling (mys) coverage check:")
    print(f"    expected years       : {START_YEAR}–{END_YEAR} ({len(expected_years)} years)")
    print(f"    mys_* columns found  : {len(actual_mys_years)}")
    if missing_years:
        print(f"    MISSING years        : {missing_years}")
    else:
        print("    all expected years present ✓")
    if extra_years:
        print(f"    extra years (beyond Scope) : {extra_years}")

    non_null_total = int(report["non_null"].sum())
    print(f"    total mys non-null country-years (1990–2023): {non_null_total:,}")

    if "iso3" in df.columns:
        uniq = df["iso3"].dropna().nunique()
        print(f"    unique iso3 in file  : {uniq}")

    return report


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    print(f"Loading manifest: {MANIFEST_PATH}")
    manifest = load_manifest(MANIFEST_PATH)

    undp = manifest["sources"].get("undp_hdr")
    if undp is None:
        print("ERROR: manifest missing sources.undp_hdr")
        return 1

    url = undp["url"]
    output_rel = undp["output_file"]
    dest = PROJECT_ROOT / output_rel

    print(f"\nSource URL : {url}")
    print(f"Destination: {dest}")

    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    ok = download_file(url, dest)
    if not ok:
        print("\nDownload failed — leaving previous file (if any) in place.")
        return 2

    try:
        df = inspect(dest)
    except Exception as exc:
        print(f"\nFailed to read downloaded CSV: {type(exc).__name__}: {exc}")
        return 3

    mys_report = build_mys_report(df)
    mys_report.to_csv(REPORT_OUT, index=False)
    print(f"\nCoverage report written to: {REPORT_OUT}")

    all_years_present = bool(mys_report["present"].all())
    any_data = int(mys_report["non_null"].sum()) > 0
    if all_years_present and any_data:
        print("\n=== Step 04 summary: OK ===")
        return 0
    else:
        print("\n=== Step 04 summary: issues detected ===")
        if not all_years_present:
            print("  - Some mys_YYYY columns missing for Scope years")
            print("    (consider updating manifest URL to the latest HDR vintage)")
        if not any_data:
            print("  - No non-null mys data found at all")
        return 2


if __name__ == "__main__":
    sys.exit(main())
