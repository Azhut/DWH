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

    ЖИЗНЕННЫЙ ЦИКЛ ДАННЫХ:

    1. Инициализация (endpoint):
       - file: UploadFile - весь объект FastAPI
       - form_id: ID формы

    2. ReadFileContentStep (шаг 1):
       - Читает file → file_content (bytes)
       - Извлекает filename для дальнейшего использования
       - После этого шага file больше НЕ используется

    3. ExtractMetadataStep (шаг 2):
       - Использует только filename → создаёт file_info

    4. CheckUniquenessStep, CreateFileModelStep (шаги 3-4):
       - Работают с file_info → создают file_model

    5. ProcessSheetsStep (шаг 5):
       - Использует file_content для чтения Excel
       - Создаёт sheet_models и flat_data

    6. EnrichFlatDataStep, PersistStep (шаги 6-7):
       - Работают с финальными данными

    ПРИНЦИП: Данные трансформируются по мере прохождения шагов,
    старые данные не удаляются для возможности error handling.
    """

    form_id: str
    file: UploadFile
    file_content: Optional[bytes] = None  # Содержимое файла в памяти
    file_info: Optional[FileInfo] = None  # Метаданные: reporter, year, extension
    form_info: Optional[FormInfo] = None  # Информация о форме из БД
    file_model: Optional[FileModel] = None  # Модель файла для сохранения
    sheet_models: Optional[List[SheetModel]] = None  # Модели листов
    flat_data: Optional[List[FlatDataRecord]] = None  # Плоские данные


    error: Optional[str] = None
    failed: bool = False
    warnings: List[str] = field(default_factory=list)