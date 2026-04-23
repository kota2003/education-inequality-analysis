"""
Phase 01 - Step 02: Download World Bank WDI indicators and country metadata.

Purpose:
    Fetch every World Bank indicator declared in data/raw/manifest.yaml for
    years 1990–2023 across all entities (countries + aggregates), plus the
    full country metadata list. Save to data/raw/world_bank/ in long format.
    UNESCO UIS variables are covered here via the WB mirror (SE.* series),
    so no separate UNESCO download step is needed (see PROJECT_LOG 2026-04-23
    Phase 01 Step 01 — rationale for Q3 decision).

Inputs:
    data/raw/manifest.yaml

Outputs:
    data/raw/world_bank/wb_wdi.csv              (long: one row per indicator × entity × year)
    data/raw/world_bank/wb_country_metadata.csv (one row per entity)
    outputs/tables/phase01_s02_download_report.csv (per-indicator summary)

Notes:
    - No stochastic components, so no random seed is set.
    - Aggregates (World, income groups, regional aggregates) are kept in the
      raw files. Filtering to countries-only is deferred to Phase 02.
    - Uses requests directly against the WB REST API for audit traceability
      and immunity to wbdata library API drift.
    - Transient network errors (Timeout, ConnectionError) retried with
      exponential backoff; HTTP and parse errors are terminal.
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
OUT_DIR = PROJECT_ROOT / "data" / "raw" / "world_bank"
REPORT_DIR = PROJECT_ROOT / "outputs" / "tables"
WDI_OUT = OUT_DIR / "wb_wdi.csv"
META_OUT = OUT_DIR / "wb_country_metadata.csv"
REPORT_OUT = REPORT_DIR / "phase01_s02_download_report.csv"

WB_API_BASE = "https://api.worldbank.org/v2"
REQUEST_TIMEOUT_S = 60.0
MAX_RETRIES = 3
RETRY_BACKOFF_S = 3.0   # waits: 3s, 6s between attempts
PER_PAGE = 20000        # WB API hard cap is typically 32500; 20k is a safe default
REQUEST_SLEEP_S = 0.2   # between successful calls

START_YEAR = 1990
END_YEAR = 2023


# ---------------------------------------------------------------------------
# Manifest
# ---------------------------------------------------------------------------

def load_manifest(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# ---------------------------------------------------------------------------
# HTTP with retry
# ---------------------------------------------------------------------------

def fetch_json_with_retry(url: str, what: str) -> list | dict | None:
    last_error: str | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, timeout=REQUEST_TIMEOUT_S)
            resp.raise_for_status()
            return resp.json()
        except (Timeout, ReqConnectionError) as exc:
            last_error = f"{type(exc).__name__}: {exc}"
            if attempt < MAX_RETRIES:
                print(f"    [{what}] attempt {attempt} transient error — retrying")
                time.sleep(RETRY_BACKOFF_S * attempt)
                continue
            print(f"    [{what}] FAILED after {MAX_RETRIES} attempts — {last_error}")
            return None
        except Exception as exc:
            print(f"    [{what}] terminal error: {type(exc).__name__}: {exc}")
            return None
    return None


def fetch_all_pages(url_base: str, what: str) -> list[dict]:
    """Fetch all pages for a WB endpoint. Returns flat list of record dicts."""
    records: list[dict] = []
    page = 1
    while True:
        sep = "&" if "?" in url_base else "?"
        url = f"{url_base}{sep}page={page}"
        payload = fetch_json_with_retry(url, f"{what} p{page}")
        if payload is None:
            raise RuntimeError(f"Failed to fetch {what} page {page}")
        if not isinstance(payload, list) or len(payload) < 2:
            # WB returns {"message": ...} as a dict when the endpoint errors
            raise RuntimeError(f"Unexpected response shape for {what}: {payload!r}"[:400])

        header = payload[0] if isinstance(payload[0], dict) else {}
        page_records = payload[1] or []
        records.extend(page_records)

        total_pages = int(header.get("pages", 1) or 1)
        if page >= total_pages:
            break
        page += 1
        time.sleep(REQUEST_SLEEP_S)
    return records


# ---------------------------------------------------------------------------
# Indicator download
# ---------------------------------------------------------------------------

def download_indicator(code: str, declared_name: str) -> pd.DataFrame:
    url = (
        f"{WB_API_BASE}/country/all/indicator/{code}"
        f"?date={START_YEAR}:{END_YEAR}&format=json&per_page={PER_PAGE}"
    )
    records = fetch_all_pages(url, f"ind {code}")

    rows = []
    for r in records:
        country = r.get("country") or {}
        indicator = r.get("indicator") or {}
        iso3 = r.get("countryiso3code")
        iso2 = country.get("id")  # WB puts ISO-2 here
        year_str = r.get("date")
        try:
            year = int(year_str) if year_str is not None else None
        except (ValueError, TypeError):
            year = None
        rows.append(
            {
                "indicator_code": indicator.get("id"),
                "indicator_name": indicator.get("value"),
                "declared_name": declared_name,
                "country_iso3": iso3 if iso3 else None,
                "country_iso2": iso2,
                "country_name": country.get("value"),
                "year": year,
                "value": r.get("value"),
            }
        )
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Country metadata download
# ---------------------------------------------------------------------------

def download_country_metadata() -> pd.DataFrame:
    url = f"{WB_API_BASE}/country?format=json&per_page=500"
    records = fetch_all_pages(url, "country metadata")
    rows = []
    for r in records:
        region = r.get("region") or {}
        income = r.get("incomeLevel") or {}
        lending = r.get("lendingType") or {}
        rows.append(
            {
                "country_iso3": r.get("id"),
                "country_iso2": r.get("iso2Code"),
                "country_name": r.get("name"),
                "region_id": region.get("id"),
                "region_name": region.get("value"),
                "income_level_id": income.get("id"),
                "income_level_name": income.get("value"),
                "lending_type_id": lending.get("id"),
                "lending_type_name": lending.get("value"),
                "capital_city": r.get("capitalCity"),
                "longitude": r.get("longitude"),
                "latitude": r.get("latitude"),
            }
        )
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    print(f"Loading manifest: {MANIFEST_PATH}")
    manifest = load_manifest(MANIFEST_PATH)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    # ---- Download indicators ----
    wb = manifest["sources"]["world_bank_wdi"]
    indicators = wb["indicators"]
    print(
        f"\nDownloading {len(indicators)} WB indicators "
        f"for {START_YEAR}–{END_YEAR} (timeout={REQUEST_TIMEOUT_S:g}s, retries={MAX_RETRIES}) ..."
    )

    all_frames: list[pd.DataFrame] = []
    report_rows: list[dict] = []

    for i, ind in enumerate(indicators, start=1):
        code = ind["code"]
        name = ind["name"]
        label = f"[{i:2d}/{len(indicators)}] {code} ({name})"
        print(f"  {label} ...")
        try:
            df = download_indicator(code, name)
        except Exception as exc:
            print(f"    FAIL: {exc}")
            report_rows.append(
                {
                    "code": code,
                    "declared_name": name,
                    "rows": 0,
                    "non_null_rows": 0,
                    "unique_entities": 0,
                    "unique_countries_iso3": 0,
                    "year_min": None,
                    "year_max": None,
                    "status": f"FAIL: {exc}",
                }
            )
            continue

        rows = len(df)
        non_null = int(df["value"].notna().sum())
        uniq_entities = df["country_iso2"].nunique(dropna=True)
        uniq_countries = df["country_iso3"].nunique(dropna=True)
        years = df["year"].dropna()
        y_min = int(years.min()) if len(years) else None
        y_max = int(years.max()) if len(years) else None

        print(
            f"    OK — {rows} rows ({non_null} non-null), "
            f"{uniq_countries} ISO-3 entities, years {y_min}–{y_max}"
        )

        all_frames.append(df)
        report_rows.append(
            {
                "code": code,
                "declared_name": name,
                "rows": rows,
                "non_null_rows": non_null,
                "unique_entities": uniq_entities,
                "unique_countries_iso3": uniq_countries,
                "year_min": y_min,
                "year_max": y_max,
                "status": "OK",
            }
        )
        time.sleep(REQUEST_SLEEP_S)

    if all_frames:
        wdi = pd.concat(all_frames, ignore_index=True)
        wdi.to_csv(WDI_OUT, index=False)
        print(f"\nSaved WDI long-format data: {WDI_OUT}  ({len(wdi):,} rows)")
    else:
        print("\nNo indicator data downloaded; skipping WDI file write.")

    # ---- Download country metadata ----
    print("\nDownloading country metadata ...")
    meta_ok = False
    try:
        meta = download_country_metadata()
        meta.to_csv(META_OUT, index=False)
        meta_ok = True
        # count real countries vs aggregates: aggregates have region_id == "NA"
        n_countries = int((meta["region_id"] != "NA").sum())
        n_agg = int((meta["region_id"] == "NA").sum())
        print(
            f"Saved country metadata: {META_OUT}  "
            f"({len(meta)} entities: {n_countries} countries, {n_agg} aggregates)"
        )
    except Exception as exc:
        print(f"FAILED to download country metadata: {exc}")

    # ---- Download report ----
    report = pd.DataFrame(report_rows)
    report.to_csv(REPORT_OUT, index=False)
    print(f"\nDownload report written to: {REPORT_OUT}")

    # ---- Summary ----
    n_ok = sum(1 for r in report_rows if r["status"] == "OK")
    n_fail = len(report_rows) - n_ok
    print("\n=== Download summary ===")
    print(f"  Indicators OK          : {n_ok}/{len(report_rows)}")
    print(f"  Indicators failed      : {n_fail}")
    print(f"  Country metadata OK    : {meta_ok}")
    if n_fail:
        print("\n  Failed indicators:")
        for r in report_rows:
            if r["status"] != "OK":
                print(f"    - {r['code']}: {r['status']}")

    return 0 if (n_fail == 0 and meta_ok) else 2


if __name__ == "__main__":
    sys.exit(main())
