"""
Настройка эвристик вертикальной иерархии (боковик Excel).

Вынесено в отдельный модуль: список триггеров и лимиты глубины — критичны для бизнеса
и должны настраиваться без правок основного парсера.
"""
from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any, Mapping

# Разделитель сегментов итогового заголовка строки (совпадает с горизонтальными заголовками).
PATH_SEPARATOR = " | "


@dataclass(frozen=True)
class VerticalHierarchyHeuristicConfig:
    """
    Префиксы сравниваются с началом строки после lstrip пробелов, в lower-case.

    primary_* / compound_* — префиксы «витков» иерархии: строка может углублять путь.
    Несколько таких строк подряд (например «в том числе» внутри «в том числе») наращивают
    глубину рекурсивно. Пробелы в начале и тире дополнительно сдвигают уровень.

    По умолчанию лимита длины пути нет (см. vertical_hierarchy_max_path_segments в requisites).
    """

    primary_child_phrase_prefixes: tuple[str, ...] = (
        "из них",
        "в т.ч.",
        "т.ч.",
        "включая",
        "включительно",
    )
    compound_child_phrase_prefixes: tuple[str, ...] = (
        "в том числе",
    )
    dash_prefixes: tuple[str, ...] = ("-", "–", "—")
    """Символы тире в начале строки (после lstrip пробелов) — первичный триггер."""

    min_leading_spaces_for_child_hint: int = 2
    leading_spaces_per_level: int = 2

    # Завершение «подблока» после явной дочерней строки: следующие строки без тире/из них
    # остаются на уровне потомка того же корня, пока не похоже на новый крупный заголовок.
    subblock_exit_markers: tuple[str, ...] = ("всего",)
    subblock_exit_min_line_length: int = 48


DEFAULT_VERTICAL_HIERARCHY_HEURISTICS = VerticalHierarchyHeuristicConfig()

# None — без ограничения числа сегментов в вертикальном пути.
DEFAULT_MAX_VERTICAL_PATH_SEGMENTS: int | None = None


def heuristic_config_from_requisites(requisites: Mapping[str, Any] | None) -> VerticalHierarchyHeuristicConfig:
    """
    Собирает конфиг эвристик из requisites формы (опционально).

    Поддерживаемые ключи (все необязательны):
      - vertical_hierarchy_primary_prefixes: list[str]  — полная замена primary
      - vertical_hierarchy_compound_prefixes: list[str] — полная замена compound
      - vertical_hierarchy_dash_prefixes: list[str]     — полная замена dash
      - vertical_hierarchy_min_leading_spaces: int
      - vertical_hierarchy_spaces_per_level: int
    """
    if not requisites:
        return DEFAULT_VERTICAL_HIERARCHY_HEURISTICS

    r = dict(requisites)
    cfg = DEFAULT_VERTICAL_HIERARCHY_HEURISTICS

    if "vertical_hierarchy_primary_prefixes" in r:
        p = r["vertical_hierarchy_primary_prefixes"]
        cfg = replace(cfg, primary_child_phrase_prefixes=tuple(str(x).lower() for x in p))
    if "vertical_hierarchy_compound_prefixes" in r:
        p = r["vertical_hierarchy_compound_prefixes"]
        cfg = replace(cfg, compound_child_phrase_prefixes=tuple(str(x).lower() for x in p))
    if "vertical_hierarchy_dash_prefixes" in r:
        p = r["vertical_hierarchy_dash_prefixes"]
        cfg = replace(cfg, dash_prefixes=tuple(str(x) for x in p))
    if "vertical_hierarchy_min_leading_spaces" in r:
        cfg = replace(cfg, min_leading_spaces_for_child_hint=int(r["vertical_hierarchy_min_leading_spaces"]))
    if "vertical_hierarchy_spaces_per_level" in r:
        cfg = replace(cfg, leading_spaces_per_level=int(r["vertical_hierarchy_spaces_per_level"]))
    if "vertical_hierarchy_subblock_exit_markers" in r:
        p = r["vertical_hierarchy_subblock_exit_markers"]
        cfg = replace(cfg, subblock_exit_markers=tuple(str(x).lower() for x in p))
    if "vertical_hierarchy_subblock_exit_min_line_length" in r:
        cfg = replace(
            cfg,
            subblock_exit_min_line_length=int(r["vertical_hierarchy_subblock_exit_min_line_length"]),
        )

    return cfg


def max_vertical_path_segments_from_requisites(requisites: Mapping[str, Any] | None) -> int | None:
    """
    Ограничение числа узлов в вертикальном пути (сколько сегментов через PATH_SEPARATOR).

    None — без лимита. Положительное число — обрезка до «корень + последние (n-1) сегментов».
    0 в реквизитах трактуем как «без лимита», как None.
    """
    if not requisites or "vertical_hierarchy_max_path_segments" not in requisites:
        return DEFAULT_MAX_VERTICAL_PATH_SEGMENTS
    raw = int(requisites["vertical_hierarchy_max_path_segments"])
    if raw <= 0:
        return None
    return raw
