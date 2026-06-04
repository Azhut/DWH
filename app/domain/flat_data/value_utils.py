"""Нормализация поля value для хранения в FlatData (MongoDB / DuckDB)."""

from __future__ import annotations

import math
from typing import Any


def normalize_flat_value(value: Any) -> float:
    """
    Приводит value к числу для колонки DOUBLE.

    - числа сохраняются как float;
    - None, NaN, пустая строка → 0.0;
    - нечисловые строки (например 'X') → 0.0.
    """
    if value is None:
        return 0.0

    if isinstance(value, bool):
        return float(int(value))

    if hasattr(value, "item") and not isinstance(value, (str, bytes)):
        try:
            return normalize_flat_value(value.item())
        except Exception:
            return 0.0

    if isinstance(value, int):
        return float(value)

    if isinstance(value, float):
        if math.isnan(value):
            return 0.0
        return value

    if isinstance(value, str):
        stripped = value.strip()
        if stripped.lower() in ("", "nan", "none", "null"):
            return 0.0
        normalized = stripped.replace(",", ".")
        try:
            parsed = float(normalized)
            if math.isnan(parsed):
                return 0.0
            return parsed
        except ValueError:
            return 0.0

    try:
        parsed = float(value)
        if math.isnan(parsed):
            return 0.0
        return parsed
    except (TypeError, ValueError):
        return 0.0
