"""
Контракты агрегата Parsing: типизированные модели для парсинга листов Excel.
"""
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Union


@dataclass(frozen=True)
class TableStructure:
    """Структура таблицы: границы заголовков и данных."""

    header_start_row: int
    header_end_row: int
    data_start_row: int
    vertical_header_column: int  # индекс колонки с вертикальными заголовками

    @property
    def num_header_levels(self) -> int:
        return max(1, self.header_end_row - self.header_start_row + 1)


@dataclass
class ParsedHeaders:
    """Распарсенные заголовки листа."""

    horizontal: List[str]  # заголовки колонок
    vertical: List[str]   # заголовки строк (боковые)


@dataclass
class CellValue:
    """Значение ячейки с контекстом строки."""

    row_header: str
    row_number: Optional[Union[int, str]]
    value: Union[str, int, float, None]


@dataclass
class ExtractedColumn:
    """Одна колонка извлечённых данных."""

    column_header: str
    values: List[CellValue]


@dataclass
class ExtractedSheetData:
    """Извлечённые данные листа в структурированном виде."""
    columns: List[ExtractedColumn]


# Служебное значение для пустых ячеек (1ФК, notes)
SERVICE_EMPTY = "__EMPTY__"
