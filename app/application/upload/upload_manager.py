"""Оркестратор эндпоинта upload: запускает pipeline для каждого файла и собирает ответ."""
import logging
from typing import List

from fastapi import UploadFile

from app.api.v2.schemas.upload import UploadResponse, FileResponse
from app.application.upload.pipeline import UploadPipelineContext, build_default_pipeline
from app.application.upload.data_save import DataSaveService
from app.domain.file.models import FileStatus
from app.domain.file.service import FileService
from app.domain.form.service import FormService
from app.domain.sheet.service import SheetService

logger = logging.getLogger(__name__)


class UploadManager:
    """Оркестратор эндпоинта upload: делегирует обработку pipeline, агрегаты — domain (file, form, sheet)."""

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
        self._pipeline = build_default_pipeline(
            file_service=file_service,
            form_service=form_service,
            sheet_service=sheet_service,
            data_save_service=data_save_service,
        )

    async def process_files(self, files: List[UploadFile], form_id: str) -> UploadResponse:
        file_responses: List[FileResponse] = []

        for file in files:
            ctx = UploadPipelineContext(file=file, form_id=form_id)
            await self._pipeline.run_for_file(ctx)

            if ctx.failed and ctx.file_response:
                file_responses.append(ctx.file_response)
                logger.error("Ошибка при обработке файла '%s': %s", file.filename, ctx.file_response.error)
            else:
                file_responses.append(
                    FileResponse(filename=file.filename, status=FileStatus.SUCCESS.value, error="")
                )
                form_type = getattr(getattr(ctx.form_info, "type", None), "value", "?")
                logger.info(
                    "Файл '%s' успешно обработан. Тип формы: %s, листов: %d, записей: %d",
                    file.filename,
                    form_type,
                    len(ctx.sheet_models or []),
                    len(ctx.flat_data or []),
                )

        success_count = sum(1 for r in file_responses if r.status == FileStatus.SUCCESS.value)
        failure_count = len(file_responses) - success_count
        logger.info("Обработка завершена. Успешно: %d, с ошибками: %d", success_count, failure_count)

        return UploadResponse(
            message=f"{success_count} files processed successfully, {failure_count} failed.",
            details=file_responses,
        )
