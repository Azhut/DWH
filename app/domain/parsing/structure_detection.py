"""
Универсальное определение структуры таблицы (заголовки, начало данных).
Общая логика для 5ФК и универсального парсера; 1ФК использует фиксированную стратегию.
"""
import logging
from abc import ABC, abstractmethod
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
    """Проверяет, является ли значение числом (в т.ч. строка с числом)."""
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


def _find_first_non_empty_column_index(df: pd.DataFrame) -> int:
    """
    Возвращает индекс первого столбца с непустыми значениями.
    Пропускает столбцы с именами Unnamed_Column_*.
    """
    for col_idx, col_name in enumerate(df.columns):
        if isinstance(col_name, str) and col_name.startswith("Unnamed_Column_"):
            continue
        col = df.iloc[:, col_idx]
        if sum(1 for v in col if not _is_empty_or_nan(v)) > 0:
            return col_idx
    return 0


def _auto_detect_table_structure(raw_df: pd.DataFrame, sheet_name: str = "") -> Tuple[int, int, int]:
    """
    Автоматически определяет границы таблицы по эвристикам:
    - header_start_row: первая строка заголовков
    - header_end_row: последняя строка заголовков
    - data_start_row: первая строка данных

    Эвристика: после заголовков идёт строка с номерами столбцов (натуральные числа).
    """
    analysis_df = raw_df.copy()
    max_rows_to_check = min(50, len(analysis_df))

    # Шаг 1: первая непустая строка с подтверждением (вертикальная + горизонтальная проверка)
    first_non_empty_row: Optional[int] = None
    for i in range(max_rows_to_check):
        row = analysis_df.iloc[i]
        non_empty_count = sum(1 for val in row if not _is_empty_or_nan(val))
        if non_empty_count < 2:
            continue

        valid_column_found = False
        total_non_empty_cols = 0
        for col_idx in range(len(row)):
            if _is_empty_or_nan(row.iloc[col_idx]):
                continue
            total_non_empty_cols += 1
            consecutive_nans = 0
            for j in range(1, min(11, len(analysis_df) - i)):
                if _is_empty_or_nan(analysis_df.iloc[i + j, col_idx]):
                    consecutive_nans += 1
                else:
                    break
            if consecutive_nans < 6:
                valid_column_found = True
        if not valid_column_found and total_non_empty_cols > 0:
            continue

        look_ahead = min(12, len(analysis_df) - i - 1)
        sustained = sum(
            1
            for j in range(1, look_ahead + 1)
            if sum(1 for val in analysis_df.iloc[i + j] if not _is_empty_or_nan(val)) >= 3
        )
        if sustained >= 6:
            first_non_empty_row = i
            break

    if first_non_empty_row is None:
        best = max(range(max_rows_to_check), key=lambda i: sum(1 for v in analysis_df.iloc[i] if not _is_empty_or_nan(v)))
        first_non_empty_row = best

    # Шаг 2: строка с номерами столбцов (натуральные числа)
    column_numbers_row: Optional[int] = None
    for r in range(first_non_empty_row, min(first_non_empty_row + 15, len(analysis_df))):
        row = analysis_df.iloc[r]
        numeric_count = sum(
            1
            for v in row
            if not _is_empty_or_nan(v) and str(v).strip().isdigit() and int(str(v).strip()) > 0
        )
        if numeric_count >= 8:
            column_numbers_row = r
            break

    if column_numbers_row is None:
        for r in range(first_non_empty_row, min(first_non_empty_row + 20, len(analysis_df))):
            if sum(1 for v in analysis_df.iloc[r] if _is_numeric_value(v)) >= 3:
                column_numbers_row = r - 1
                break
    if column_numbers_row is None:
        column_numbers_row = first_non_empty_row + 2

    header_end_row = column_numbers_row - 1
    data_start_row = column_numbers_row + 1

    if first_non_empty_row < 0 or header_end_row < first_non_empty_row:
        logger.warning("Некорректная структура для листа %s, используем fallback", sheet_name)
        return 0, 0, len(raw_df)

    data_start_row = min(data_start_row, len(raw_df))
    return first_non_empty_row, header_end_row, data_start_row


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
        header_start, header_end, data_start = _auto_detect_table_structure(df, sheet_name)
        # Вертикальная колонка: первый непустой в зоне данных
        data_zone = df.iloc[data_start:]
        vertical_col = _find_first_non_empty_column_index(data_zone)
        return TableStructure(
            header_start_row=header_start,
            header_end_row=header_end,
            data_start_row=data_start,
            vertical_header_column=vertical_col,
        )


def detect_table_structure(
    df: pd.DataFrame,
    strategy: StructureDetectionStrategy,
    sheet_name: str = "",
) -> TableStructure:
    """Определяет структуру таблицы по выбранной стратегии."""
    return strategy.detect(df, sheet_name)
