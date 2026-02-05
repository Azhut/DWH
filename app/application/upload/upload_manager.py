import logging
from typing import List
from fastapi import UploadFile
from app.api.v2.schemas.upload import UploadResponse
from app.application.upload.pipeline import build_default_pipeline
from app.application.upload.request_validator import RequestValidator
from app.application.upload.form_loader import FormLoader
from app.application.upload.file_processor import FileProcessor
from app.application.upload.response_builder import UploadResponseBuilder
from app.application.data import DataSaveService
from app.domain.file.service import FileService
from app.domain.form.service import FormService
from app.domain.sheet.service import SheetService

logger = logging.getLogger(__name__)

class UploadManager:
    """
    Управляет загрузкой файлов через upload pipeline.

    Ответственность:
    1. Оркестрация процесса загрузки
    2. Делегирование валидации, загрузки формы, обработки и формирования ответа

    Компоненты:
    - RequestValidator: валидация запроса
    - FormLoader: загрузка формы из БД (один раз для всех файлов)
    - FileProcessor: обработка каждого файла
    - UploadResponseBuilder: формирование ответа
    """

    def __init__(
        self,
        file_service: FileService,
        form_service: FormService,
        sheet_service: SheetService,
        data_save_service: DataSaveService,
    ):
        self._file_service = file_service
        self._form_service = form_service
        self._sheet_service = sheet_service
        self._data_save_service = data_save_service

        self._validator = RequestValidator()
        self._form_loader = FormLoader(form_service=form_service)
        self._pipeline = build_default_pipeline(
            file_service=file_service,
            sheet_service=sheet_service,
            data_save_service=data_save_service,
        )
        self._file_processor = FileProcessor(pipeline=self._pipeline)

    async def upload_files(
        self,
        files: List[UploadFile],
        form_id: str,
    ) -> UploadResponse:
        """
        Обрабатывает загрузку файлов.

        Этапы:
        1. Валидация запроса (формат form_id, наличие файлов)
        2. Загрузка формы из БД (один раз для всех файлов)
        3. Обработка каждого файла через pipeline
        4. Формирование ответа

        Raises:
            RequestValidationError: При ошибках валидации запроса

        Returns:
            UploadResponse: Всегда возвращает 200 OK со списком результатов по файлам
        """
        # ============= Валидация запроса =============
        form_id = self._validator.validate_request(files, form_id)

        logger.info(
            "Начало обработки %d файл(ов) для формы '%s'",
            len(files),
            form_id,
        )

        # ============= Загрузка формы из БД  =============
        form_info = await self._form_loader.load_form(form_id)

        # ============= Обработка файлов =============
        file_responses = []
        for file in files:
            response = await self._file_processor.process_file(file, form_id, form_info)
            file_responses.append(response)

        # ============= Формирование ответа =============
        return UploadResponseBuilder.build_response(file_responses)