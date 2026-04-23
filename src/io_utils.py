"""
I/O utilities for the Education–Inequality analysis project.

Currently hosts one helper — a CSV reader that tries a sequence of encodings
and returns the DataFrame together with the encoding that succeeded. This is
needed because UNDP HDR CSVs are published in Windows-1252 / Latin-1 rather
than UTF-8, and World Bank and other sources are UTF-8. A single call covers
both cases without the caller needing to care about which source it is.

Promoted from Phase 01 Step 04 / Step 05 scripts per PROJECT_WORKFLOW.md §6.2
(promote functions that are called more than once or are non-trivial).
"""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd

DEFAULT_ENCODING_CANDIDATES: tuple[str, ...] = (
    "utf-8",
    "utf-8-sig",
    "cp1252",
    "latin-1",
)


def read_csv_with_encoding_fallback(
    path: str | Path,
    encodings: Iterable[str] = DEFAULT_ENCODING_CANDIDATES,
    **read_csv_kwargs,
) -> tuple[pd.DataFrame, str]:
    """Read a CSV by trying each encoding in turn.

    Parameters
    ----------
    path
        Path to the CSV file.
    encodings
        Encodings to try in order. The default covers UTF-8 (with and without
        BOM), Windows-1252, and Latin-1 as a guaranteed fallback — every byte
        value maps to a valid Latin-1 character, so a Latin-1 decode can
        never raise ``UnicodeDecodeError``.
    **read_csv_kwargs
        Additional keyword arguments passed through to :func:`pandas.read_csv`.

    Returns
    -------
    df : pandas.DataFrame
        The decoded table.
    encoding_used : str
        The encoding that successfully decoded the file.

    Raises
    ------
    RuntimeError
        If every encoding in *encodings* fails with ``UnicodeDecodeError``.
        This should be impossible when the default list is used, because
        Latin-1 cannot raise that error.
    """
    last_error: UnicodeDecodeError | None = None
    tried: list[str] = []
    for enc in encodings:
        tried.append(enc)
        try:
            df = pd.read_csv(path, encoding=enc, **read_csv_kwargs)
            return df, enc
        except UnicodeDecodeError as exc:
            last_error = exc
            continue
    raise RuntimeError(
        f"Could not decode {path!s} with any of {tuple(tried)}. "
        f"Last error: {last_error!r}"
    )
