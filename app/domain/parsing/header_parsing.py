"""
Универсальный парсинг заголовков листа (горизонтальные и вертикальные).
Общая логика из BaseSheetParser; используется для 1ФК и 5ФК.
"""
import logging
from typing import List, Tuple

import pandas as pd

from app.domain.parsing.models import ParsedHeaders, TableStructure
from app.parsers.header_fixer import fix_header, finalize_header_fixing

logger = logging.getLogger(__name__)


def _get_header_rows(sheet: pd.DataFrame, structure: TableStructure) -> pd.DataFrame:
    """Срезы строк заголовков по структуре."""
    return sheet.iloc[structure.header_start_row : structure.header_end_row + 1].fillna("")


def _fill_empty_cells_in_headers(header_rows: pd.DataFrame, structure: TableStructure) -> None:
    """Заполняет пустые ячейки в заголовках (проброс сверху и слева)."""
    n = len(header_rows)
    for row_idx in range(n - 1, 0, -1):
        for col_idx in range(header_rows.shape[1]):
            if header_rows.iloc[row_idx, col_idx] == "":
                for search_row in range(row_idx - 1, -1, -1):
                    if header_rows.iloc[search_row, col_idx] != "":
                        header_rows.iloc[row_idx, col_idx] = header_rows.iloc[search_row, col_idx]
                        break
    for row_idx in range(n):
        for col_idx in range(1, header_rows.shape[1]):
            if header_rows.iloc[row_idx, col_idx] == "":
                header_rows.iloc[row_idx, col_idx] = header_rows.iloc[row_idx, col_idx - 1]


def _get_horizontal_headers(header_rows: pd.DataFrame) -> List[str]:
    """Формирует горизонтальные заголовки (колонки) из многоуровневых строк."""
    horizontal = []
    n = len(header_rows)
    for col_idx in range(1, header_rows.shape[1]):
        path = []
        current = header_rows.iloc[n - 1, col_idx]
        path.append(current)
        for row_idx in range(n - 2, -1, -1):
            val = header_rows.iloc[row_idx, col_idx]
            if val != current:
                path.insert(0, val)
                current = val
        horizontal.append(" | ".join(str(p) for p in path))
    return horizontal


def _get_vertical_headers(sheet: pd.DataFrame, structure: TableStructure) -> List[str]:
    """Вертикальные заголовки (боковые) из колонки по структуре."""
    col_idx = structure.vertical_header_column
    return sheet.iloc[structure.data_start_row :, col_idx].dropna().astype(str).tolist()


def _normalize_headers(headers: List[str]) -> List[str]:
    """Удаление переносов и артефактов в заголовках."""
    return [fix_header(h).replace("_x000D_", "").strip() for h in headers]


def parse_headers(sheet: pd.DataFrame, structure: TableStructure) -> ParsedHeaders:
    """
    Парсит горизонтальные и вертикальные заголовки по заданной структуре.
    Вызов finalize_header_fixing() — ответственность вызывающего (один раз после всех листов).
    """
    header_rows = _get_header_rows(sheet, structure)
    _fill_empty_cells_in_headers(header_rows, structure)

    horizontal = _get_horizontal_headers(header_rows)
    vertical = _get_vertical_headers(sheet, structure)

    horizontal = _normalize_headers(horizontal)
    vertical = _normalize_headers(vertical)

    finalize_header_fixing()
    return ParsedHeaders(horizontal=horizontal, vertical=vertical)
