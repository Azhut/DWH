"""Контекст parsing pipeline: передаётся между шагами парсинга одного листа."""
from dataclasses import dataclass, field
from typing import List, Optional

import pandas as pd

from app.domain.form.models import FormInfo
from app.domain.parsing.models import ExtractedSheetData, TableStructure
from app.domain.parsing.workbook_source import ParsingWorkbookSource
from app.domain.sheet.models import SheetModel


@dataclass
class ParsingPipelineContext:
    """
    Контекст обработки одного листа в рамках parsing pipeline.

    Два уровня данных:
    - Рабочие/промежуточные: живут только здесь, нужны шагам в процессе парсинга,
      отбрасываются после завершения pipeline.
    - Финальные результаты: пишутся в sheet_model — единственный источник правды
      о результатах парсинга листа. После завершения pipeline ProcessSheetsStep
      забирает sheet_model и больше не смотрит в ctx.

    ЕДИНСТВЕННЫЙ ИСТОЧНИК ПРАВДЫ:
    - sheet_model.sheet_name           — нормализованное имя листа
    - sheet_model.horizontal_headers   — горизонтальные заголовки
    - sheet_model.vertical_headers     — вертикальные заголовки
    - sheet_model.flat_data_records    — плоские данные
    - sheet_model.warnings / .errors   — диагностика

    Нейтральный носитель — не знает о конкретных формах.
    """

    # --- Входные данные (устанавливаются при создании, не меняются) ---
    sheet_model: SheetModel
    raw_dataframe: pd.DataFrame
    form_info: FormInfo

    # Снимок файла для доменного парсинга (openpyxl indent и т.п.).
    # Заполняется в ProcessSheetsStep из UploadPipelineContext одним объектом — без
    # дублирования отдельных полей вроде file_content + extension.
    workbook_source: ParsingWorkbookSource | None = None

    # --- Рабочие/промежуточные данные шагов ---
    table_structure: Optional[TableStructure] = None
    processed_dataframe: Optional[pd.DataFrame] = None
    extracted_data: Optional[ExtractedSheetData] = None

    # --- Ошибки и статус pipeline ---
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    failed: bool = False

    @property
    def sheet_name(self) -> str:
        """
        Имя листа для использования в шагах и логах.
        До DetectTableStructureStep возвращает оригинальное имя (sheet_fullname).
        После — нормализованное (sheet_name из sheet_model).
        """
        return self.sheet_model.sheet_name or self.sheet_model.sheet_fullname

    def add_warning(self, message: str) -> None:
        """Добавляет предупреждение в ctx и в sheet_model. Не останавливает pipeline."""
        self.warnings.append(message)
        self.sheet_model.warnings.append(message)