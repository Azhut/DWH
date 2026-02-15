"""Контекст parsing pipeline: передаётся между шагами парсинга одного листа."""
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import pandas as pd

from app.domain.flat_data.models import FlatDataRecord
from app.domain.form.models import FormInfo, FormType
from app.domain.parsing.models import ExtractedSheetData, TableStructure


@dataclass
class ParsingPipelineContext:
    """Контекст обработки одного листа в рамках parsing pipeline."""

    # Входные данные
    sheet_name: str
    raw_dataframe: pd.DataFrame
    form_info: FormInfo
    file_year: Optional[int]
    file_reporter: Optional[str]
    file_id: Optional[str]
    form_id: Optional[str]

    # Конфигурация pipeline (зависит от формы)
    apply_notes: bool = False  # True только для 1ФК
    deduplicate_columns: bool = False  # True для 5ФК

    # Промежуточные результаты (domain/parsing)
    table_structure: Optional[TableStructure] = None
    processed_dataframe: Optional[pd.DataFrame] = None
    header_start_row: Optional[int] = None
    header_end_row: Optional[int] = None
    data_start_row: Optional[int] = None
    vertical_header_column: Optional[int] = None
    horizontal_headers: List[str] = field(default_factory=list)
    vertical_headers: List[str] = field(default_factory=list)
    extracted_data: Optional[ExtractedSheetData] = None
    parsed_data: Optional[Dict] = None  # legacy-формат для совместимости

    # Финальные результаты
    flat_data_records: List[FlatDataRecord] = field(default_factory=list)
    sheet_model_data: Optional[Dict] = None  # Данные для SheetModel

    # Ошибки и статус
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    failed: bool = False

    def add_error(self, message: str) -> None:
        """Добавляет ошибку в контекст."""
        self.errors.append(message)
        self.failed = True

    def add_warning(self, message: str) -> None:
        """Добавляет предупреждение в контекст."""
        self.warnings.append(message)
