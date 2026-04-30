"""
Project path utilities.

A single canonical way to locate the project root from anywhere inside
the project tree. Step scripts and notebooks use this to run correctly
whether invoked from the project root or from a subdirectory like
`scripts/` or `notebooks/`.
"""

from __future__ import annotations

from pathlib import Path


PROJECT_LOG_FILENAME = "PROJECT_LOG.md"


def find_project_root(start: Path | None = None) -> Path:
    """Locate the project root by walking upward from `start`.

    The project root is identified as the nearest ancestor directory
    containing PROJECT_LOG.md, which is guaranteed to exist at the root
    and only at the root.

    Parameters
    ----------
    start : Path, optional
        Directory to start searching from. Defaults to the current
        working directory.

    Returns
    -------
    Path
        Absolute path of the project root.

    Raises
    ------
    FileNotFoundError
        If no ancestor directory contains PROJECT_LOG.md.
    """
    if start is None:
        start = Path.cwd()
    start = Path(start).resolve()

    for candidate in [start, *start.parents]:
        if (candidate / PROJECT_LOG_FILENAME).exists():
            return candidate

    raise FileNotFoundError(
        f"Could not locate {PROJECT_LOG_FILENAME} by walking up from "
        f"{start}. Are you running from inside the project tree?"
    )
