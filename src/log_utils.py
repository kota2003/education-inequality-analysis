"""
PROJECT_LOG.md helpers.

Hosts a single function - an idempotent append for PROJECT_LOG.md
entries. The pattern has been duplicated five times across Phases
02 and 03 (s01/s07 in Phase 02, s01/s08-wrap/s08-correction in
Phase 03), exceeding the PROJECT_WORKFLOW.md §6.2 promotion
threshold.

The helper is intentionally minimal: it takes a marker string that
must appear in `entry_text`, looks for that marker in the existing
file, and either appends the entry or no-ops. The marker convention
mirrors the existing log entries' header lines, e.g.
"## 2026-05-01 - Phase 04, Step 08".

Promoted in Phase 04 Step 01 but first exercised in Phase 04 Step 08
wrap, by design - see the Phase 04 Step 01 PROJECT_LOG entry for
the rationale on the deferred first-use.
"""

from __future__ import annotations

from pathlib import Path


def append_log_entry(
    log_path: Path,
    entry_text: str,
    marker: str,
) -> bool:
    """Idempotently append `entry_text` to PROJECT_LOG.md.

    The function checks whether `marker` already appears anywhere in
    the file. If it does, the file is left unchanged and the function
    returns False. Otherwise `entry_text` is appended (preceded by a
    single newline if the file does not already end in one) and the
    function returns True.

    Parameters
    ----------
    log_path
        Path to PROJECT_LOG.md (or any append-only log file following
        the same convention).
    entry_text
        The full entry to append, including its header line. The
        caller is responsible for the format - this function does not
        inject blank lines, headers, or separators beyond ensuring
        the file ends with a newline before the append.
    marker
        A string that uniquely identifies this entry. Conventionally
        the header line itself (e.g.
        ``"## 2026-05-01 - Phase 04, Step 08"``). `marker` MUST appear
        somewhere in `entry_text`; if it does not, a ValueError is
        raised, because the marker would never be found on a rerun
        and the function could not be idempotent.

    Returns
    -------
    appended : bool
        True if the entry was newly appended, False if `marker` was
        already present and the file was left unchanged.

    Raises
    ------
    FileNotFoundError
        If `log_path` does not exist. (We deliberately do not create
        the file - PROJECT_LOG.md exists from project kickoff and a
        missing log indicates a wrong path or wrong working
        directory, not a state to silently recover from.)
    ValueError
        If `marker` is not a substring of `entry_text`.
    """
    log_path = Path(log_path)

    if not log_path.exists():
        raise FileNotFoundError(
            f"Log file not found: {log_path}. "
            f"Refusing to create it - check the path."
        )

    if marker not in entry_text:
        raise ValueError(
            "marker must appear in entry_text so that the append is "
            "idempotent on rerun. "
            f"marker={marker!r} was not found in entry_text."
        )

    existing = log_path.read_text(encoding="utf-8")

    if marker in existing:
        return False

    if not existing.endswith("\n"):
        existing = existing + "\n"

    log_path.write_text(existing + entry_text, encoding="utf-8")
    return True
