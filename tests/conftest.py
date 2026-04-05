from __future__ import annotations

import os
import sys

import pytest

from config.config import config


@pytest.fixture(autouse=True)
def _disable_mongo_transactions_in_tests(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "MONGO_USE_TRANSACTIONS", False)


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

