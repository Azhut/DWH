"""Преобразование MongoDB-фильтра FlatDataService в SQL WHERE для DuckDB."""

from __future__ import annotations

from typing import Any, Dict, List, Tuple


def mongo_filter_to_sql(
    query: Dict[str, Any],
) -> Tuple[str, List[Any]]:
    """
    Строит фрагмент WHERE и список параметров.

    Поддерживает подмножество, используемое FlatDataService:
    - равенство поля
    - $in
    - $and
    - $regex с $options: i -> ILIKE '%pattern%'
    """
    if not query:
        return "1=1", []

    if "$and" in query:
        parts: List[str] = []
        params: List[Any] = []
        for sub in query["$and"]:
            frag, sub_params = mongo_filter_to_sql(sub)
            parts.append(f"({frag})")
            params.extend(sub_params)
        return " AND ".join(parts), params

    parts = []
    params: List[Any] = []
    for key, value in query.items():
        if key.startswith("$"):
            continue
        col = _quote_column(key)
        if isinstance(value, dict):
            if "$in" in value:
                vals = list(value["$in"])
                if not vals:
                    parts.append("1=0")
                    continue
                placeholders = ", ".join("?" for _ in vals)
                parts.append(f"{col} IN ({placeholders})")
                params.extend(vals)
            elif "$regex" in value:
                pattern = str(value["$regex"])
                parts.append(f"{col} ILIKE ?")
                params.append(f"%{pattern}%")
            else:
                raise ValueError(f"Unsupported query operator for field {key}: {value}")
        else:
            parts.append(f"{col} = ?")
            params.append(value)

    if not parts:
        return "1=1", []
    return " AND ".join(parts), params


def _quote_column(name: str) -> str:
    if name == "column":
        return '"column"'
    if name == "row":
        return '"row"'
    return name
