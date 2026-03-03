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
       - file: UploadFile — весь объект FastAPI
       - form_id: ID формы
       - form_info: информация о форме (загружается до pipeline)

    2. ReadFileContentStep (шаг 1):
       - Читает file → file_content (bytes)
       - После этого шага file больше НЕ используется

    3. ExtractMetadataStep (шаг 2):
       - filename → file_info

    4. CheckUniquenessStep, CreateFileModelStep (шаги 3-4):
       - file_info → file_model

    5. ProcessSheetsStep (шаг 5):
       - file_content → запускает parsing pipeline для каждого листа
       - Результат: sheets (список SheetModel с заполненными данными)

    6. EnrichFlatDataStep, PersistStep (шаги 6-7):
       - Работают с flat_data (агрегация из sheets) и file_model

    ЕДИНСТВЕННЫЙ ИСТОЧНИК ПРАВДЫ:
    - sheets — список SheetModel с результатами парсинга каждого листа
    - flat_data — производное (property), агрегация flat_data_records из всех листов
    """

    form_id: str
    file: UploadFile
    form_info: Optional[FormInfo] = None       # информация о форме из БД
    file_content: Optional[bytes] = None       # содержимое файла в памяти
    file_info: Optional[FileInfo] = None       # метаданные: reporter, year, extension
    file_model: Optional[FileModel] = None     # модель файла для сохранения
    sheets: List[SheetModel] = field(default_factory=list)  # результаты парсинга листов

    error: Optional[str] = None
    failed: bool = False
    warnings: List[str] = field(default_factory=list)

    @property
    def flat_data(self) -> List[FlatDataRecord]:
        """
        Агрегация всех FlatDataRecord из всех листов.

        Единственный источник flat_data — SheetModel.flat_data_records.
        Не хранится отдельно, всегда вычисляется из sheets.
        """
        return [record for sheet in self.sheets for record in sheet.flat_data_records]