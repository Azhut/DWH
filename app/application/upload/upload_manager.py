import asyncio
import io
import logging
from typing import List, Dict
from uuid import uuid4

from fastapi import UploadFile
from starlette.datastructures import UploadFile as StarletteUploadFile

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
        self._upload_progress: Dict[str, UploadProgress] = {}

    async def upload_files(
        self,
        files: List[UploadFile],
        form_id: str,
    ) -> UploadResponse:

        form_id = self._validator.validate_request(files, form_id)
        logger.info("Upload started for form '%s' with %d file(s)", form_id, len(files))

        # Читаем файлы в память сразу — FastAPI закроет их после возврата ответа
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

        # Запускаем обработку в фоне и сразу возвращаем upload_id
        asyncio.create_task(
            self._process_files_background(buffered, form_id, upload_id, progress)
        )

        return UploadResponseBuilder.build_pending_response(upload_id)

    async def _process_files_background(
        self,
        buffered: List[tuple[str, str, bytes]],
        form_id: str,
        upload_id: str,
        progress: UploadProgress,
    ) -> None:
        file_responses = []
        try:
            form_info = await self._form_loader.load_form(form_id)

            for filename, content_type, content in buffered:
                upload_file = StarletteUploadFile(
                    file=io.BytesIO(content),
                    filename=filename,
                    headers={"content-type": content_type},
                )
                response = await self._file_processor.process_file(upload_file, form_id, form_info)
                file_responses.append(response)

                success = response.status == "success"
                progress.add_processed_file(response.filename, success, response.error if not success else None)

            if all(r.status == "success" for r in file_responses):
                progress.complete()
            else:
                progress.fail()

            logger.info("Background processing done for upload_id=%s", upload_id)

        except Exception as e:
            logger.error("Background processing failed for upload_id=%s: %s", upload_id, e)
            progress.fail()

    def get_upload_progress(self, upload_id: str) -> UploadProgress | None:
        return self._upload_progress.get(upload_id)

    def cleanup_upload_progress(self, upload_id: str) -> None:
        self._upload_progress.pop(upload_id, None)