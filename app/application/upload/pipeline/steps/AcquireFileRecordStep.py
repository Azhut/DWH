"""Шаг: получить или переиспользовать запись файла для жизненного цикла загрузки."""

import logging
from datetime import datetime

from app.application.upload.pipeline.context import UploadPipelineContext
from app.core.exceptions import CriticalUploadError, DuplicateFileError
from app.domain.file.models import FileModel, FileStatus
from app.domain.file.service import FileService

logger = logging.getLogger(__name__)


class AcquireFileRecordStep:
    """Получает запись файла и переводит её в статус PROCESSING."""

    def __init__(self, file_service: FileService):
        self._file_service = file_service

    async def execute(self, ctx: UploadPipelineContext) -> None:
        if not ctx.filename:
            raise CriticalUploadError(
                message="Filename is not set in upload context",
                domain="upload.acquire_file_record",
                http_status=500,
                meta={"context_state": "filename is empty"},
            )

        try:
            existing_success = await self._file_service.get_by_filename_and_status(
                ctx.filename,
                FileStatus.SUCCESS,
                ctx.form_id,
            )
            if existing_success:
                raise DuplicateFileError(
                    message=f"File '{ctx.filename}' has already been uploaded successfully.",
                    domain="upload.acquire_file_record",
                    http_status=409,
                    meta={
                        "file_name": ctx.filename,
                        "form_id": ctx.form_id,
                        "existing_file_id": existing_success.file_id,
                    },
                )

            file_model = await self._file_service.get_by_filename(ctx.filename, ctx.form_id)
            if not file_model:
                file_model = FileModel.create_processing(
                    filename=ctx.filename,
                    form_id=ctx.form_id,
                )

            file_model.form_id = ctx.form_id
            file_model.year = None
            file_model.reporter = None
            file_model.status = FileStatus.PROCESSING
            file_model.error = None
            file_model.sheets = []
            file_model.size = 0
            file_model.updated_at = datetime.now()

            await self._file_service.update_or_create(file_model)
            ctx.file_model = file_model
            logger.debug(
                "Acquired file record file_id=%s for filename=%s form_id=%s",
                file_model.file_id,
                ctx.filename,
                ctx.form_id,
            )

        except DuplicateFileError:
            raise
        except Exception as exc:
            raise CriticalUploadError(
                message=f"Failed to acquire file record: {exc}",
                domain="upload.acquire_file_record",
                http_status=500,
                meta={
                    "file_name": ctx.filename,
                    "form_id": ctx.form_id,
                    "error": str(exc),
                },
            ) from exc
