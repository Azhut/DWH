import logging
from typing import List, Dict
from uuid import uuid4

from fastapi import UploadFile

from app.api.v2.schemas.upload import UploadResponse
from app.application.data import DataSaveService
from app.application.parsing.registry import ParsingStrategyRegistry
from app.application.upload.file_processor import FileProcessor
from app.application.upload.form_loader import FormLoader
from app.application.upload.pipeline import build_default_pipeline
from app.application.upload.request_validator import RequestValidator
from app.application.upload.response_builder import UploadResponseBuilder
from app.application.upload.upload_progress import UploadProgress
from app.domain.file.service import FileService
from app.domain.form.service import FormService

logger = logging.getLogger(__name__)


class UploadManager:
    """Оркестратор всего пайплана обработки"""

    def __init__(
        self,
        file_service: FileService,
        form_service: FormService,
        data_save_service: DataSaveService,
        parsing_registry: ParsingStrategyRegistry | None = None,
    ):
        self._validator = RequestValidator()
        self._form_loader = FormLoader(form_service=form_service)
        self._pipeline = build_default_pipeline(
            file_service=file_service,
            data_save_service=data_save_service,
            parsing_registry=parsing_registry,
        )
        self._file_processor = FileProcessor(pipeline=self._pipeline)
        # Хранилище прогресса в памяти
        self._upload_progress: Dict[str, UploadProgress] = {}

    async def upload_files(
        self,
        files: List[UploadFile],
        form_id: str,
    ) -> UploadResponse:

        form_id = self._validator.validate_request(files, form_id)
        logger.info("Upload started for form '%s' with %d file(s)", form_id, len(files))

        # Генерируем upload_id и создаем прогресс
        upload_id = str(uuid4())
        progress = UploadProgress(
            upload_id=upload_id,
            total_files=len(files),
            form_id=form_id
        )
        self._upload_progress[upload_id] = progress

        form_info = await self._form_loader.load_form(form_id)

        file_responses = []
        try:
            for file in files:
                response = await self._file_processor.process_file(file, form_id, form_info)
                file_responses.append(response)
                
                # Обновляем прогресс
                success = response.status == "success"
                error = response.error if not success else None
                progress.add_processed_file(response.filename, success, error)

            # Завершаем прогресс
            if all(file.status == "success" for file in file_responses):
                progress.complete()
            else:
                progress.fail()

        except Exception as e:
            logger.error(f"Upload failed for {upload_id}: {e}")
            progress.fail()
            raise

        # Возвращаем ответ с upload_id для обратной совместимости
        return UploadResponseBuilder.build_response(file_responses, upload_id)

    def get_upload_progress(self, upload_id: str) -> UploadProgress | None:
        """Возвращает прогресс загрузки по upload_id"""
        return self._upload_progress.get(upload_id)

    def cleanup_upload_progress(self, upload_id: str):
        """Удаляет прогресс из памяти (опционально для очистки)"""
        self._upload_progress.pop(upload_id, None)
