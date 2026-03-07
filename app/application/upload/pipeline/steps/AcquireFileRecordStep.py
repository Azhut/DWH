"""Step: acquire or reuse file record for upload lifecycle."""

import logging
from datetime import datetime

from app.application.upload.pipeline.context import UploadPipelineContext
from app.core.exceptions import CriticalUploadError, DuplicateFileError
from app.domain.file.models import FileModel, FileStatus
from app.domain.file.service import FileService

logger = logging.getLogger(__name__)


class AcquireFileRecordStep:
    """Acquire a single file record and move it to PROCESSING state."""

    def __init__(self, file_service: FileService):
        self._file_service = file_service

    async def execute(self, ctx: UploadPipelineContext) -> None:
        filename = getattr(ctx.file, "filename", None)
        if not filename:
            raise CriticalUploadError(
                message="File name is not set in upload context",
                domain="upload.acquire_file_record",
                http_status=500,
                meta={"context_state": "filename is None"},
            )

        try:
            existing_success = await self._file_service.get_by_filename_and_status(
                filename,
                FileStatus.SUCCESS,
                ctx.form_id,
            )
            if existing_success:
                raise DuplicateFileError(
                    message=f"File '{filename}' has already been uploaded successfully.",
                    domain="upload.acquire_file_record",
                    http_status=409,
                    meta={
                        "file_name": filename,
                        "form_id": ctx.form_id,
                        "existing_file_id": existing_success.file_id,
                    },
                )

            file_model = await self._file_service.get_by_filename(filename, ctx.form_id)
            if not file_model:
                file_model = FileModel.create_processing(
                    filename=filename,
                    form_id=ctx.form_id,
                )

            file_model.form_id = ctx.form_id
            file_model.status = FileStatus.PROCESSING
            file_model.error = None
            file_model.updated_at = datetime.now()

            await self._file_service.update_or_create(file_model)
            ctx.file_model = file_model
            logger.debug(
                "Acquired file record file_id=%s for filename=%s form_id=%s",
                file_model.file_id,
                filename,
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
                    "file_name": filename,
                    "form_id": ctx.form_id,
                    "error": str(exc),
                },
            ) from exc
