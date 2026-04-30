"""
Manifest accessors for the data source registry.

The project's data sources are declared in `data/raw/manifest.yaml`
(schema v1). This module provides typed accessors for the parts of the
manifest that step scripts and notebooks consume — the WB declared
names, the HDR target names, and the canonical variable order used to
keep column conventions consistent across phases.

The manifest is the single source of truth for:
- Which variables are declared as part of the project
- The canonical variable name (declared_name) used across the panel
- The presentation order of variables in the wide panel
"""

from __future__ import annotations

from pathlib import Path

import yaml


def load_manifest(path: Path) -> dict:
    """Load and parse the YAML manifest at `path`."""
    with Path(path).open(encoding="utf-8") as f:
        return yaml.safe_load(f)


def wb_declared_names(manifest: dict) -> list[str]:
    """Return WB WDI declared variable names in manifest declaration order."""
    return [ind["name"] for ind in manifest["sources"]["world_bank_wdi"]["indicators"]]


def hdr_target_names(manifest: dict) -> list[str]:
    """Return UNDP HDR target variable names in manifest declaration order."""
    return [var["target_name"] for var in manifest["sources"]["undp_hdr"]["variables"]]


def manifest_variable_order(manifest: dict) -> list[str]:
    """Return all declared variable names in canonical order: WB first, then HDR.

    This is the column order used for the wide panel
    (`data/processed/panel.csv`) and any per-variable summary table.
    """
    return wb_declared_names(manifest) + hdr_target_names(manifest)
