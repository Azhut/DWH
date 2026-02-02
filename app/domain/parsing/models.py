"""
Контракты агрегата Parsing: типизированные модели для парсинга листов Excel.
"""
from dataclasses import dataclass, field
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

    def to_legacy_format(self) -> List[Dict[str, Any]]:
        """Формат, совместимый со старым API (data для SheetModel)."""
        return [
            {
                "column_header": col.column_header,
                "values": [{"row_header": c.row_header, "value": c.value} for c in col.values],
            }
            for col in self.columns
        ]


@dataclass
class SheetParseResult:
    """Полный результат парсинга одного листа."""

    headers: ParsedHeaders
    data: ExtractedSheetData
    structure: TableStructure

    def to_legacy_dict(self) -> Dict[str, Any]:
        """Словарь в формате старого API парсеров."""
        return {
            "headers": {
                "horizontal": self.headers.horizontal,
                "vertical": self.headers.vertical,
            },
            "data": self.data.to_legacy_format(),
        }


# Служебное значение для пустых ячеек (1ФК, notes)
SERVICE_EMPTY = "__EMPTY__"
