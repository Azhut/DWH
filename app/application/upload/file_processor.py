"""Process one uploaded file through upload pipeline."""

import logging

from fastapi import UploadFile

from app.api.v2.schemas.files import FileResponse
from app.application.upload.pipeline import UploadPipelineContext, UploadPipelineRunner
from app.core.exceptions import CriticalUploadError, log_app_error
from app.domain.file.models import FileStatus
from app.domain.form.models import FormInfo

logger = logging.getLogger(__name__)


class FileProcessor:
    """Runs upload pipeline for a single file and maps result to API response."""

    def __init__(self, pipeline: UploadPipelineRunner):
        self._pipeline = pipeline

    async def process_file(
        self,
        file: UploadFile,
        form_id: str,
        form_info: FormInfo,
    ) -> FileResponse:
        filename = (file.filename or "").strip()
        logger.info("Processing file: '%s'", filename)

        ctx = UploadPipelineContext(
            file=file,
            filename=filename,
            form_id=form_id,
            form_info=form_info,
        )

        try:
            await self._pipeline.run_for_file(ctx)

            if ctx.failed:
                return FileResponse(
                    filename=ctx.filename,
                    status=FileStatus.FAILED,
                    error=ctx.error or "Unknown upload error",
                )

            form_type = getattr(getattr(ctx.form_info, "type", None), "value", "?")
            logger.info(
                "File '%s' processed successfully. form_type=%s, sheets=%d, records=%d",
                ctx.filename,
                form_type,
                len(ctx.sheets),
                len(ctx.flat_data),
            )
            return FileResponse(
                filename=ctx.filename,
                status=FileStatus.SUCCESS,
                error="",
            )
        except Exception as exc:
            error = CriticalUploadError(
                message=f"Internal file processing error: {exc}",
                domain="upload.file_processor",
                http_status=500,
                meta={"file_name": ctx.filename, "form_id": form_id, "error": str(exc)},
            )
            log_app_error(error, exc_info=True)
            return FileResponse(
                filename=ctx.filename,
                status=FileStatus.FAILED,
                error=error.message,
            )
