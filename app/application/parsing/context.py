"""Контекст parsing pipeline: передаётся между шагами парсинга одного листа."""
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import pandas as pd

from app.domain.flat_data.models import FlatDataRecord
from app.domain.form.models import FormInfo
from app.domain.parsing.models import ExtractedSheetData, TableStructure


@dataclass
class ParsingPipelineContext:
    """
    Контекст обработки одного листа в рамках parsing pipeline.

    Нейтральный носитель данных — не знает о конкретных формах.
    Форма-специфичная логика полностью вынесена в стратегии.

    Жизненный цикл полей:
    - Входные данные: устанавливаются при создании, не меняются.
    - Промежуточные результаты: заполняются шагами последовательно.
    - Финальные результаты: заполняются последними шагами pipeline.
    - Ошибки/статус: накапливаются по ходу выполнения.
    """

    # --- Входные данные (устанавливаются при создании) ---
    sheet_name: str
    raw_dataframe: pd.DataFrame
    form_info: FormInfo
    file_year: Optional[int]
    file_reporter: Optional[str]
    file_id: Optional[str]
    form_id: Optional[str]

    # --- Промежуточные результаты (заполняются шагами) ---
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

    # --- Финальные результаты ---
    flat_data_records: List[FlatDataRecord] = field(default_factory=list)
    sheet_model_data: Optional[Dict] = None

    # --- Ошибки и статус ---
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    failed: bool = False

    def add_warning(self, message: str) -> None:
        """Добавляет предупреждение. Не останавливает pipeline."""
        self.warnings.append(message)