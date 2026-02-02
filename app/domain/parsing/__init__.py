"""
Агрегат Parsing: контракты и универсальная логика парсинга листов Excel.

Общая логика (универсальный парсер) используется для 1ФК и 5ФК.
Частные случаи задаются только конфигурацией pipeline (фиксированная vs авто-структура, notes да/нет).
"""
from app.domain.parsing.models import (
    TableStructure,
    ParsedHeaders,
    CellValue,
    ExtractedColumn,
    ExtractedSheetData,
    SheetParseResult,
    SERVICE_EMPTY,
)
from app.domain.parsing.structure_detection import (
    detect_table_structure,
    StructureDetectionStrategy,
    FixedStructureStrategy,
    AutoDetectStructureStrategy,
)
from app.domain.parsing.header_parsing import parse_headers
from app.domain.parsing.data_extraction import extract_sheet_data
from app.domain.parsing.flat_data_builder import build_flat_data_records
from app.domain.parsing.notes import process_notes_1fk

__all__ = [
    "TableStructure",
    "ParsedHeaders",
    "CellValue",
    "ExtractedColumn",
    "ExtractedSheetData",
    "SheetParseResult",
    "SERVICE_EMPTY",
    "detect_table_structure",
    "StructureDetectionStrategy",
    "FixedStructureStrategy",
    "AutoDetectStructureStrategy",
    "parse_headers",
    "extract_sheet_data",
    "build_flat_data_records",
    "process_notes_1fk",
]
