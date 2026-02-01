"""
Контекст, передаваемый между шагами upload pipeline.
Все шаги получают один и тот же контекст и могут его дополнять.
Реквизиты формы (skip_sheets и др.) — часть сущности Form (form_info.requisites), не контекста.
"""
from dataclasses import dataclass, field
from typing import Optional, List, Any

from fastapi import UploadFile


@dataclass
class UploadPipelineContext:
    """Контекст обработки одного файла в рамках upload pipeline."""

    # Входные данные (заполняются до запуска pipeline)
    file: UploadFile
    form_id: str

    # Результаты шагов (заполняются по ходу выполнения)
    # file_info — из сервиса по работе с файлами (FileHandlingService)
    file_info: Any = None  # FileInfo
    # form_info — из сервиса по работе с формами (FormService); реквизиты (skip_sheets) в form_info.requisites
    form_info: Any = None  # FormInfo
    file_model: Any = None  # FileModel
    sheet_models: Optional[List[Any]] = None  # List[SheetModel]
    flat_data: Optional[List[dict]] = None

    # При ошибке: сообщение и флаг
    error: Optional[str] = None
    failed: bool = False

    # Итоговый ответ по файлу (заполняется pipeline runner)
    file_response: Any = None  # FileResponse
