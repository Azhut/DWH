"""
Универсальное определение структуры таблицы (заголовки, начало данных).
Общая логика для 5ФК и универсального парсера; 1ФК использует фиксированную стратегию.
"""
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional, Tuple

import pandas as pd

from app.domain.parsing.models import TableStructure

logger = logging.getLogger(__name__)


def _is_empty_or_nan(value: object) -> bool:
    """Проверяет, является ли значение пустым или NaN."""
    if pd.isna(value) or value is None:
        return True
    s = str(value).strip().lower()
    return s in ("", "nan", "none", "null", "nat", "_x0000_", "_x000d_")


def _is_numeric_value(value: object) -> bool:
    """
    Проверяет, является ли значение числом (в т.ч. строка с числом).

    Используется при приведении значений ячеек к int/float на этапе извлечения данных.
    """
    if _is_empty_or_nan(value):
        return False
    s = str(value).strip().replace(" ", "").replace(",", ".")
    if not s:
        return False
    try:
        float(s)
        return True
    except (ValueError, TypeError):
        return False


def _to_positive_int(value: object) -> Optional[int]:
    """
    Пытается привести значение ячейки к натуральному числу (1, 2, 3, ...).

    Возвращает:
    - int > 0, если значение похоже на натуральное число
    - None, если значение пустое/NaN/не число/не натуральное
    """
    if _is_empty_or_nan(value) or isinstance(value, bool):
        return None

    if isinstance(value, int):
        return value if value > 0 else None

    if isinstance(value, float):
        if value > 0 and value.is_integer():
            return int(value)
        return None

    s = str(value).strip().lower().replace("\xa0", " ")
    if not s:
        return None

    s_compact = s.replace(" ", "")
    if s_compact.isdigit():
        parsed = int(s_compact)
        return parsed if parsed > 0 else None

    #  "12.0" / "12,00" → 12
    if any(ch in s_compact for ch in (".", ",")):
        try:
            as_float = float(s_compact.replace(",", "."))
        except ValueError:
            return None
        if as_float > 0 and as_float.is_integer():
            return int(as_float)

    return None


def _find_1_to_n_run(row: pd.Series) -> Optional[Tuple[int, int, int]]:
    """
    Ищет в строке самый длинный подряд идущий прогон 1..n.

    Возвращает кортеж (start_col, end_col, n), где:
    - start_col/end_col — границы прогона по индексам столбцов
    - n — длина прогона (последнее ожидаемое число)
    """
    best_start = -1
    best_end = -1
    best_len = 0

    run_start = -1
    expected = 1

    for col_idx, raw_value in enumerate(row.tolist()):
        parsed = _to_positive_int(raw_value)

        if parsed == expected:
            if expected == 1:
                run_start = col_idx
            expected += 1
            continue

        run_len = expected - 1
        if run_len > best_len:
            best_len = run_len
            best_start = run_start
            best_end = col_idx - 1

        if parsed == 1:
            run_start = col_idx
            expected = 2
        else:
            run_start = -1
            expected = 1

    tail_len = expected - 1
    if tail_len > best_len:
        best_len = tail_len
        best_start = run_start
        best_end = len(row) - 1

    if best_len <= 0 or best_start < 0 or best_end < best_start:
        return None

    return best_start, best_end, best_len


def _find_numbering_row_and_bounds(
    df: pd.DataFrame,
    *,
    max_rows_to_check: int = 80,
    min_sequence_len: int = 8,
) -> Optional[Tuple[int, int, int, int]]:
    """
    Находит строку нумерации столбцов и границы валидных столбцов.

    Идея: ищем строку, в которой присутствует прогон натуральных чисел 1..n.
    Всё, что вне прогона, считаем невалидными столбцами и отрезаем.

    Возвращает (numbering_row, first_col, last_col, n) или None.
    """
    if df.empty:
        return None

    search_rows = min(max_rows_to_check, len(df))
    best: Optional[Tuple[int, int, int, int]] = None

    for row_idx in range(search_rows):
        run = _find_1_to_n_run(df.iloc[row_idx])
        if run is None:
            continue

        start_col, end_col, seq_len = run
        if seq_len < min_sequence_len:
            continue

        if best is None:
            best = (row_idx, start_col, end_col, seq_len)
            continue

        _, best_start, _, best_len = best
        if seq_len > best_len or (seq_len == best_len and start_col < best_start):
            best = (row_idx, start_col, end_col, seq_len)

    return best


def _find_header_start_row(
    df: pd.DataFrame,
    *,
    header_end_row: int,
    first_col: int,
    last_col: int,
) -> int:
    """
    Определяет первую строку горизонтальных заголовков.

    Правило: по валидным столбцам [first_col:last_col] ищем первую строку
    (начиная с 0), где есть хоть что-то кроме NaN/пустоты.
    """
    if header_end_row < 0:
        return 0

    for row_idx in range(min(header_end_row + 1, len(df))):
        row = df.iloc[row_idx, first_col : last_col + 1]
        if int(row.notna().sum()) > 0:
            return row_idx

    return 0


@dataclass(frozen=True)
class AutoDetectedTableLayout:
    """Результат авто-детекции: структура + границы валидных столбцов."""

    structure: TableStructure
    first_col: int
    last_col: int
    numbering_row: int
    sequence_len: int


def auto_detect_table_layout(
    df: pd.DataFrame,
    *,
    sheet_name: str = "",
    max_rows_to_check: int = 80,
    min_sequence_len: int = 8,
) -> Optional[AutoDetectedTableLayout]:
    """
    Автоматически определяет структуру таблицы по строке нумерации столбцов.

    Алгоритм:
    - находим строку, где в валидных столбцах есть прогон 1..n
    - всё, что выше строки нумерации — зона заголовков
    - всё, что ниже — данные (data_start_row = numbering_row + 1)
    - валидные столбцы: от столбца с "1" до столбца с "n" включительно
    - vertical_header_column всегда 0 (после обрезки к валидным столбцам)
    """
    numbering = _find_numbering_row_and_bounds(
        df,
        max_rows_to_check=max_rows_to_check,
        min_sequence_len=min_sequence_len,
    )
    if numbering is None:
        logger.warning("Не нашли строку нумерации столбцов для листа '%s'", sheet_name)
        return None

    numbering_row, first_col, last_col, seq_len = numbering

    header_end_row = numbering_row - 1
    header_start_row = _find_header_start_row(
        df,
        header_end_row=header_end_row,
        first_col=first_col,
        last_col=last_col,
    )
    data_start_row = min(numbering_row + 1, len(df))

    structure = TableStructure(
        header_start_row=header_start_row,
        header_end_row=header_end_row,
        data_start_row=data_start_row,
        vertical_header_column=0,
    )

    logger.info(
        "Авто-детекция для '%s': numbering_row=%d, 1..%d, cols=[%d:%d], headers=[%d:%d], data_start=%d",
        sheet_name,
        numbering_row,
        seq_len,
        first_col,
        last_col,
        structure.header_start_row,
        structure.header_end_row,
        structure.data_start_row,
    )

    return AutoDetectedTableLayout(
        structure=structure,
        first_col=first_col,
        last_col=last_col,
        numbering_row=numbering_row,
        sequence_len=seq_len,
    )


class StructureDetectionStrategy(ABC):
    """Стратегия определения структуры таблицы."""

    @abstractmethod
    def detect(self, df: pd.DataFrame, sheet_name: str) -> TableStructure:
        pass


class FixedStructureStrategy(StructureDetectionStrategy):
    """Фиксированная структура (1ФК)."""

    def __init__(
        self,
        header_start_row: int,
        header_end_row: int,
        data_start_row: int,
        vertical_header_column: int = 0,
    ):
        self.header_start_row = header_start_row
        self.header_end_row = header_end_row
        self.data_start_row = data_start_row
        self.vertical_header_column = vertical_header_column

    def detect(self, df: pd.DataFrame, sheet_name: str = "") -> TableStructure:
        return TableStructure(
            header_start_row=self.header_start_row,
            header_end_row=self.header_end_row,
            data_start_row=self.data_start_row,
            vertical_header_column=self.vertical_header_column,
        )


class AutoDetectStructureStrategy(StructureDetectionStrategy):
    """Автоматическое определение структуры (5ФК, универсальный)."""

    def detect(self, df: pd.DataFrame, sheet_name: str = "") -> TableStructure:
        layout = auto_detect_table_layout(df, sheet_name=sheet_name)
        if layout is not None:
            return layout.structure

        # Fallback: если строка нумерации не найдена — считаем, что заголовок в первых строках.
        # Это "best-effort" поведение, чтобы не падать на нестандартных листах.
        header_start_row = 0
        header_end_row = min(2, max(0, len(df) - 1))
        data_start_row = min(header_end_row + 1, len(df))
        return TableStructure(
            header_start_row=header_start_row,
            header_end_row=header_end_row,
            data_start_row=data_start_row,
            vertical_header_column=0,
        )


def detect_table_structure(
    df: pd.DataFrame,
    strategy: StructureDetectionStrategy,
    sheet_name: str = "",
) -> TableStructure:
    """Определяет структуру таблицы по выбранной стратегии."""
    return strategy.detect(df, sheet_name)
