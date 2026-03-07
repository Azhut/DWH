import logging
from typing import List

from fastapi import UploadFile

from app.api.v2.schemas.upload import UploadResponse
from app.application.data import DataSaveService
from app.application.parsing.registry import ParsingStrategyRegistry
from app.application.upload.file_processor import FileProcessor
from app.application.upload.form_loader import FormLoader
from app.application.upload.pipeline import build_default_pipeline
from app.application.upload.request_validator import RequestValidator
from app.application.upload.response_builder import UploadResponseBuilder
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

    async def upload_files(
        self,
        files: List[UploadFile],
        form_id: str,
    ) -> UploadResponse:

        form_id = self._validator.validate_request(files, form_id)
        logger.info("Upload started for form '%s' with %d file(s)", form_id, len(files))

        form_info = await self._form_loader.load_form(form_id)

        file_responses = []
        for file in files:
            response = await self._file_processor.process_file(file, form_id, form_info)
            file_responses.append(response)

        return UploadResponseBuilder.build_response(file_responses)
