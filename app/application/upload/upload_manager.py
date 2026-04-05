# app/application/upload/upload_manager.py
import asyncio
import io
import logging
from typing import Dict, List
from uuid import uuid4

from fastapi import UploadFile
from starlette.datastructures import UploadFile as StarletteUploadFile

from app.api.v2.schemas.files import FileResponse
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
    """
    Оркестратор пайплайна обработки файлов.

    Контракт:
        upload_files()           → 202 Accepted + upload_id (немедленно)
        get_upload_progress()    → текущее состояние задачи
        cleanup_upload_progress() → освобождение памяти после завершения

    Финальный UploadResponse формируется внутри фоновой задачи и
    сохраняется в UploadProgress.file_responses — SSE-эндпоинт
    забирает его оттуда и отдаёт клиенту последним событием.
    """

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
        self._upload_progress: Dict[str, UploadProgress] = {}

    # ------------------------------------------------------------------
    # Публичный API
    # ------------------------------------------------------------------

    async def upload_files(
        self,
        files: List[UploadFile],
        form_id: str,
    ) -> UploadResponse:
        """
        Валидирует запрос, читает файлы в память, регистрирует задачу
        и немедленно возвращает 202 с upload_id.

        Raises:
            RequestValidationError — если запрос невалиден (нет файлов,
                                     некорректный form_id и т.д.)
        """
        form_id = self._validator.validate_request(files, form_id)
        logger.info("Upload started: form=%s, files=%d", form_id, len(files))

        # Читаем содержимое файлов сразу — FastAPI закрывает объекты
        # UploadFile после возврата ответа из эндпоинта.
        buffered: List[tuple[str, str, bytes]] = []
        for file in files:
            content = await file.read()
            buffered.append((file.filename or "", file.content_type or "", content))

        upload_id = str(uuid4())
        progress = UploadProgress(
            upload_id=upload_id,
            total_files=len(buffered),
            form_id=form_id,
        )
        self._upload_progress[upload_id] = progress

        asyncio.create_task(
            self._process_files_background(buffered, form_id, upload_id, progress)
        )

        return UploadResponseBuilder.build_accepted_response(upload_id)

    def get_upload_progress(self, upload_id: str) -> UploadProgress | None:
        return self._upload_progress.get(upload_id)

    def cleanup_upload_progress(self, upload_id: str) -> None:
        self._upload_progress.pop(upload_id, None)

    # ------------------------------------------------------------------
    # Фоновая обработка
    # ------------------------------------------------------------------

    async def _process_files_background(
        self,
        buffered: List[tuple[str, str, bytes]],
        form_id: str,
        upload_id: str,
        progress: UploadProgress,
    ) -> None:
        file_responses: List[FileResponse] = []
        try:
            form_info = await self._form_loader.load_form(form_id)

            for filename, content_type, content in buffered:
                upload_file = StarletteUploadFile(
                    file=io.BytesIO(content),
                    filename=filename,
                    headers={"content-type": content_type},
                )
                response = await self._file_processor.process_file(
                    upload_file, form_id, form_info
                )
                file_responses.append(response)

                success = response.status == "success"
                progress.add_processed_file(
                    response.filename,
                    success,
                    response.error if not success else None,
                )

            # Фиксируем итог — complete/fail сохраняют file_responses
            # внутри progress, SSE-эндпоинт заберёт их для финального события.
            if all(r.status == "success" for r in file_responses):
                progress.complete(file_responses)
            else:
                progress.fail(file_responses)

            logger.info("Background processing done: upload_id=%s", upload_id)

        except Exception as e:
            logger.error(
                "Background processing failed: upload_id=%s, error=%s",
                upload_id,
                e,
            )
            # Частичные результаты сохраняем, чтобы клиент получил хоть что-то
            progress.fail(file_responses or [])