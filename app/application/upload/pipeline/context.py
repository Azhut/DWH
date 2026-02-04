from dataclasses import dataclass, field
from typing import List, Optional
from fastapi import UploadFile
from app.domain.file.models import FileInfo, FileModel
from app.domain.flat_data.models import FlatDataRecord
from app.domain.form.models import FormInfo
from app.domain.sheet.models import SheetModel


@dataclass
class UploadPipelineContext:
    """
    Контекст обработки одного файла в рамках upload pipeline.
    
    Поля для передачи данных между шагами:
    - file: загружаемый файл
    - form_id: ID формы
    - file_info: метаданные файла (год, город)
    - form_info: информация о форме из БД
    - file_model: модель файла для сохранения
    - sheet_models: модели листов
    - flat_data: плоские данные для сохранения
    
    Поля для обработки ошибок:
    - error: сообщение критической ошибки (если произошла)
    - failed: флаг критической ошибки
    - warnings: список некритических ошибок (для отладки)
    """

    file: UploadFile
    form_id: str

    file_info: Optional[FileInfo] = None
    form_info: Optional[FormInfo] = None
    file_model: Optional[FileModel] = None
    sheet_models: Optional[List[SheetModel]] = None
    flat_data: Optional[List[FlatDataRecord]] = None

    error: Optional[str] = None
    failed: bool = False
    warnings: List[str] = field(default_factory=list)