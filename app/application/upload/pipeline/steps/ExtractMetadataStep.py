"""Шаг: извлечь метаданные из имени файла."""

import logging

from app.application.upload.pipeline.context import UploadPipelineContext
from app.core.exceptions import CriticalUploadError, FileValidationError
from app.core.profiling import profile_step
from app.domain.file.service import FileService

logger = logging.getLogger(__name__)


class ExtractMetadataStep:
    """Извлекает метаданные reporter/year из имени файла в контексте."""

    def __init__(self, file_service: FileService):
        self._file_service = file_service

    @profile_step()
    async def execute(self, ctx: UploadPipelineContext) -> None:
        if not ctx.filename:
            raise CriticalUploadError(
                message="Filename is not set in upload context",
                domain="upload.extract_metadata",
                http_status=500,
                meta={"context_state": "filename is empty"},
            )

        try:
            file_info = self._file_service.validate_and_extract_metadata_from_filename(
                ctx.filename,
            )
        except FileValidationError as exc:
            raise CriticalUploadError(
                message=exc.message,
                domain="upload.extract_metadata",
                http_status=400,
                meta={"file_name": ctx.filename},
            ) from exc
        except Exception as exc:
            raise CriticalUploadError(
                message=f"Failed to extract metadata from filename: {exc}",
                domain="upload.extract_metadata",
                http_status=500,
                meta={"file_name": ctx.filename, "error": str(exc)},
            ) from exc

        ctx.file_info = file_info
        if ctx.file_model:
            ctx.file_model.year = file_info.year
            ctx.file_model.reporter = file_info.reporter

        logger.debug(
            "Metadata extracted: filename=%s, reporter=%s, year=%s",
            ctx.filename,
            file_info.reporter,
            file_info.year,
        )
