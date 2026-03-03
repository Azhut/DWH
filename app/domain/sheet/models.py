"""Модели агрегата Sheet: представление листа (SheetModel)."""
from dataclasses import dataclass, field
from typing import List, Optional

from app.domain.flat_data.models import FlatDataRecord


@dataclass
class SheetModel:
    """
    Результат парсинга одного листа Excel.

    Жизненный цикл:
    - Создаётся в ProcessSheetsStep с единственным заполненным полем sheet_fullname.
    - Передаётся в ParsingPipelineContext — шаги постепенно заполняют поля.
    - DetectTableStructureStep нормализует sheet_fullname → sheet_name.
    - ParseHeadersStep заполняет horizontal_headers / vertical_headers.
    - GenerateFlatDataStep заполняет flat_data_records.
    - После завершения pipeline — единственный источник правды о листе.

    Не содержит raw/промежуточных данных (DataFrame, TableStructure и т.п.) —
    они живут в ParsingPipelineContext и отбрасываются после парсинга.
    """

    sheet_fullname: str                                                   # оригинальное имя из Excel
    sheet_name: Optional[str] = None                                      # нормализованное имя
    horizontal_headers: List[str] = field(default_factory=list)           # Горизонтальные (боковые) заголовоки
    vertical_headers: List[str] = field(default_factory=list)             # Вертикальные (верхние) заголовоки
    flat_data_records: List[FlatDataRecord] = field(default_factory=list) # Плоские данные для листа

    # Статус и диагностика (накапливаются по ходу pipeline)
    warnings: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    failed: bool = False