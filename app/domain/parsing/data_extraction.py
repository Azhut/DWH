"""
Универсальное извлечение данных из листа по структуре и заголовкам.
Поддерживает простой режим (1ФК) и режим с дедупликацией колонок (5ФК).
"""
import logging
from typing import List

import pandas as pd

from app.domain.parsing.models import (
    CellValue,
    ExtractedColumn,
    ExtractedSheetData,
    ParsedHeaders,
    TableStructure,
)
from app.domain.parsing.structure_detection import _is_empty_or_nan, _is_numeric_value
from app.parsers.header_fixer import fix_header

logger = logging.getLogger(__name__)


def extract_sheet_data(
    sheet: pd.DataFrame,
    structure: TableStructure,
    headers: ParsedHeaders,
    sheet_name: str = "",
    deduplicate_columns: bool = False,
) -> ExtractedSheetData:
    """
    Извлекает данные в структурированном виде.

    Args:
        sheet: DataFrame листа
        structure: Структура таблицы
        headers: Распарсенные заголовки
        sheet_name: Имя листа (для логов)
        deduplicate_columns: Если True, оставлять только первое вхождение каждой колонки (5ФК)
    """
    if deduplicate_columns:
        return _extract_with_column_dedup(sheet, structure, headers, sheet_name)
    return _extract_simple(sheet, structure, headers, sheet_name)


def _extract_simple(
    sheet: pd.DataFrame,
    structure: TableStructure,
    headers: ParsedHeaders,
    sheet_name: str,
) -> ExtractedSheetData:
    """Простое извлечение по позициям (1ФК)."""
    columns: List[ExtractedColumn] = []
    start_row = structure.data_start_row
    v_col = structure.vertical_header_column

    for col_idx, col_header in enumerate(headers.horizontal, start=1):
        values: List[CellValue] = []
        for row_idx, row_header in enumerate(headers.vertical):
            try:
                cell_val = sheet.iloc[start_row + row_idx, col_idx]
            except IndexError:
                cell_val = None
            values.append(CellValue(row_header=row_header, value=cell_val))
        columns.append(ExtractedColumn(column_header=col_header, values=values))

    return ExtractedSheetData(columns=columns)


def _extract_with_column_dedup(
    sheet: pd.DataFrame,
    structure: TableStructure,
    headers: ParsedHeaders,
    sheet_name: str,
) -> ExtractedSheetData:
    """
    Извлечение с дедупликацией колонок: только первое вхождение каждого имени колонки.
    Используется для 5ФК (дубликаты типа «из них крытые»).
    """
    # Строим таблицу: колонки по позиции, имена из headers.horizontal
    start_row = structure.data_start_row
    vertical_headers = list(headers.vertical)
    horizontal_headers = list(headers.horizontal)

    # Маппинг: имя колонки -> позиция первого вхождения
    column_map: dict[str, int] = {}
    for pos in range(len(horizontal_headers)):
        name = horizontal_headers[pos]
        if name not in column_map:
            column_map[name] = pos

    columns: List[ExtractedColumn] = []
    for col_name, col_pos in column_map.items():
        values: List[CellValue] = []
        for row_idx in range(len(vertical_headers)):
            try:
                cell_val = sheet.iloc[start_row + row_idx, col_pos + 1]  # +1 т.к. колонка 0 — вертикальные заголовки
            except IndexError:
                cell_val = None

            if not _is_empty_or_nan(cell_val) and _is_numeric_value(cell_val):
                try:
                    s = str(cell_val).strip().replace(",", ".").replace(" ", "")
                    num = float(s)
                    cell_val = int(num) if num == int(num) else num
                except (ValueError, TypeError):
                    pass
            elif _is_empty_or_nan(cell_val):
                cell_val = None

            values.append(CellValue(row_header=vertical_headers[row_idx], value=cell_val))
        columns.append(ExtractedColumn(column_header=col_name.strip(), values=values))

    return ExtractedSheetData(columns=columns)
