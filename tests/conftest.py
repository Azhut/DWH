from __future__ import annotations

import os
import sys


def pytest_configure(config) -> None:
    """
    Best-effort fix for Windows console encodings (Cyrillic output).
    This keeps test output readable when running under PowerShell/IDE runners.
    """
    os.environ.setdefault("PYTHONUTF8", "1")
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")

    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass

