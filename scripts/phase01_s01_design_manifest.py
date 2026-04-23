"""
Phase 01 - Step 01: Design and validate the data-source manifest.

Purpose:
    Parse `data/raw/manifest.yaml` and verify that every World Bank indicator
    code listed in it resolves via the World Bank REST API. Catching typos or
    renamed codes here avoids failures during the bulk downloads in Step 02.

Inputs:
    data/raw/manifest.yaml

Outputs:
    Console report: structural checks + per-indicator availability
    outputs/tables/phase01_s01_manifest_validation.csv

Notes:
    - No stochastic components in this step, so no random seed is set.
    - UNDP HDR URL is not fetched here; Step 04 is responsible for that.
    - Uses `requests` directly (rather than `wbdata`) so the check does not
      depend on library API drift.
    - Transient network failures (Timeout, ConnectionError) are retried up to
      MAX_RETRIES times with exponential backoff. Non-network errors (HTTP
      4xx/5xx, malformed JSON) are treated as immediate failures.
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

# Project root = parent of scripts/
PROJECT_ROOT = Path(__file__).resolve().parents[1]
MANIFEST_PATH = PROJECT_ROOT / "data" / "raw" / "manifest.yaml"
REPORT_DIR = PROJECT_ROOT / "outputs" / "tables"
REPORT_PATH = REPORT_DIR / "phase01_s01_manifest_validation.csv"

WB_API_BASE = "https://api.worldbank.org/v2"
REQUEST_TIMEOUT_S = 20.0
REQUEST_SLEEP_S = 0.1   # gentle pacing between successful calls
MAX_RETRIES = 3         # total attempts for transient network errors
RETRY_BACKOFF_S = 2.0   # waits: 2s, 4s between attempts


# ---------------------------------------------------------------------------
# Manifest loading & structural checks
# ---------------------------------------------------------------------------

def load_manifest(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"Manifest not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        manifest = yaml.safe_load(f)
    if not isinstance(manifest, dict):
        raise ValueError("Manifest root must be a mapping.")
    return manifest


def check_manifest_structure(manifest: dict) -> list[str]:
    """Return a list of structural problems. Empty list = OK."""
    problems: list[str] = []

    required_top = {"schema_version", "project", "coverage", "sources"}
    missing = required_top - set(manifest.keys())
    if missing:
        problems.append(f"missing top-level keys: {sorted(missing)}")
        return problems

    coverage = manifest["coverage"]
    if not all(k in coverage for k in ("start_year", "end_year")):
        problems.append("coverage must contain start_year and end_year")
    elif coverage["start_year"] > coverage["end_year"]:
        problems.append("coverage.start_year > coverage.end_year")

    sources = manifest["sources"]
    if not isinstance(sources, dict) or not sources:
        problems.append("sources must be a non-empty mapping")
        return problems

    wb = sources.get("world_bank_wdi")
    if wb is None:
        problems.append("world_bank_wdi source is required")
    else:
        inds = wb.get("indicators", [])
        if not inds:
            problems.append("world_bank_wdi.indicators is empty")
        codes = [i.get("code") for i in inds]
        names = [i.get("name") for i in inds]
        if any(c is None for c in codes):
            problems.append("some WB indicators are missing 'code'")
        if any(n is None for n in names):
            problems.append("some WB indicators are missing 'name'")
        if len(set(codes)) != len(codes):
            problems.append("duplicate WB indicator codes detected")
        if len(set(names)) != len(names):
            problems.append("duplicate WB variable names detected")

    return problems


# ---------------------------------------------------------------------------
# World Bank indicator probe (with retry on transient network errors)
# ---------------------------------------------------------------------------

def probe_wb_indicator(code: str) -> dict:
    """
    Query the WB REST API for a single indicator's metadata.

    Retries up to MAX_RETRIES times on Timeout / ConnectionError with
    exponential backoff. Other errors are treated as terminal.

    Returns a record with keys:
        resolved, official_name, source_org, attempts, error
    """
    url = f"{WB_API_BASE}/indicator/{code}?format=json"
    last_error: str | None = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, timeout=REQUEST_TIMEOUT_S)
            resp.raise_for_status()
            payload = resp.json()
        except (Timeout, ReqConnectionError) as exc:
            last_error = f"{type(exc).__name__}: {exc}"
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF_S * attempt)
                continue
            return {
                "resolved": False,
                "official_name": None,
                "source_org": None,
                "attempts": attempt,
                "error": f"after {MAX_RETRIES} attempts — {last_error}",
            }
        except Exception as exc:
            # Non-retryable: HTTP 4xx/5xx, JSON decode errors, etc.
            return {
                "resolved": False,
                "official_name": None,
                "source_org": None,
                "attempts": attempt,
                "error": f"{type(exc).__name__}: {exc}",
            }

        # Response parsing (only runs if the request succeeded)
        if not isinstance(payload, list) or len(payload) < 2:
            return {
                "resolved": False,
                "official_name": None,
                "source_org": None,
                "attempts": attempt,
                "error": "unexpected response shape",
            }

        header = payload[0]
        if isinstance(header, dict) and header.get("message"):
            return {
                "resolved": False,
                "official_name": None,
                "source_org": None,
                "attempts": attempt,
                "error": f"API message: {header.get('message')}",
            }

        records = payload[1] or []
        if not records:
            return {
                "resolved": False,
                "official_name": None,
                "source_org": None,
                "attempts": attempt,
                "error": "no records returned",
            }

        rec = records[0]
        source_val = None
        src = rec.get("source")
        if isinstance(src, dict):
            source_val = src.get("value")
        return {
            "resolved": True,
            "official_name": rec.get("name"),
            "source_org": source_val,
            "attempts": attempt,
            "error": None,
        }

    # Unreachable given MAX_RETRIES >= 1, but keeps the type checker happy.
    return {
        "resolved": False,
        "official_name": None,
        "source_org": None,
        "attempts": MAX_RETRIES,
        "error": f"after {MAX_RETRIES} attempts — {last_error}",
    }


def probe_all(codes: list[str]) -> pd.DataFrame:
    records = []
    for i, code in enumerate(codes, start=1):
        print(f"  [{i:2d}/{len(codes)}] {code} ...", end=" ", flush=True)
        rec = probe_wb_indicator(code)
        rec["code"] = code
        records.append(rec)

        status = "OK" if rec["resolved"] else f"FAIL ({rec['error']})"
        if rec.get("attempts", 1) > 1:
            status += f"  [attempts={rec['attempts']}]"
        print(status)

        time.sleep(REQUEST_SLEEP_S)
    return pd.DataFrame.from_records(records)


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def print_role_breakdown(indicators: list[dict]) -> None:
    roles = [ind.get("role", "unspecified") for ind in indicators]
    counts = pd.Series(roles).value_counts()
    print("\nRole breakdown (WB indicators):")
    for role, count in counts.items():
        print(f"  {role:22s} {count}")


def print_summary(report: pd.DataFrame) -> None:
    n_total = len(report)
    n_ok = int(report["resolved"].sum())
    n_fail = n_total - n_ok
    n_retried = int((report["attempts"] > 1).sum())
    print("\n=== Validation summary ===")
    print(f"  Indicators checked : {n_total}")
    print(f"  Resolved OK        : {n_ok}")
    print(f"  Failed             : {n_fail}")
    print(f"  Needed retry       : {n_retried}")
    if n_fail:
        print("\n  Failed indicators:")
        failed = report.loc[~report["resolved"], ["code", "error"]]
        for _, row in failed.iterrows():
            print(f"    - {row['code']}: {row['error']}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    print(f"Loading manifest: {MANIFEST_PATH}")
    manifest = load_manifest(MANIFEST_PATH)

    problems = check_manifest_structure(manifest)
    if problems:
        print("\nManifest structural problems:")
        for p in problems:
            print(f"  - {p}")
        return 1

    coverage = manifest["coverage"]
    sources = manifest["sources"]
    print(f"  Project  : {manifest['project']}")
    print(f"  Schema   : v{manifest['schema_version']}")
    print(f"  Coverage : {coverage['start_year']}–{coverage['end_year']}")
    print(f"  Sources  : {list(sources.keys())}")

    wb = sources["world_bank_wdi"]
    wb_indicators = wb["indicators"]
    print(f"\nWorld Bank indicators declared: {len(wb_indicators)}")
    print_role_breakdown(wb_indicators)

    print(
        f"\nProbing World Bank API for indicator availability "
        f"(timeout={REQUEST_TIMEOUT_S:g}s, retries={MAX_RETRIES}) ..."
    )
    codes = [ind["code"] for ind in wb_indicators]
    probe_df = probe_all(codes)

    meta_df = pd.DataFrame(
        {
            "code": [ind["code"] for ind in wb_indicators],
            "declared_name": [ind["name"] for ind in wb_indicators],
            "role": [ind.get("role", "unspecified") for ind in wb_indicators],
        }
    )
    report = meta_df.merge(probe_df, on="code", how="left")
    report = report[
        [
            "code",
            "declared_name",
            "role",
            "resolved",
            "attempts",
            "official_name",
            "source_org",
            "error",
        ]
    ]

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    report.to_csv(REPORT_PATH, index=False)
    print(f"\nValidation report written to: {REPORT_PATH}")

    print_summary(report)

    undp = sources.get("undp_hdr")
    if undp:
        print(f"\nUNDP HDR URL declared: {undp.get('url')}")
        print("  (not fetched here — Step 04 will download and verify)")

    return 0 if bool(report["resolved"].all()) else 2


if __name__ == "__main__":
    sys.exit(main())
