import logging
from typing import List

from app.application.upload.pipeline.context import UploadPipelineContext
from app.application.upload.pipeline.steps.AcquireFileRecordStep import AcquireFileRecordStep
from app.application.upload.pipeline.steps.BaseUploadPipelineStep import UploadPipelineStep
from app.application.upload.pipeline.steps.EnrichFlatDataStep import EnrichFlatDataStep
from app.application.upload.pipeline.steps.ExtractMetadataStep import ExtractMetadataStep
from app.application.upload.pipeline.steps.PersistStep import PersistStep
from app.application.upload.pipeline.steps.ProcessSheetsStep import ProcessSheetsStep
from app.application.upload.pipeline.steps.ReadFileContentStep import ReadFileContentStep
from app.application.upload.pipeline.steps.SyncFileMetadataStep import SyncFileMetadataStep
from app.core.exceptions import (
    CriticalParsingError,
    CriticalUploadError,
    DuplicateFileError,
    NonCriticalParsingError,
    NonCriticalUploadError,
    log_app_error,
)
from config import config

logger = logging.getLogger(__name__)


class UploadPipelineRunner:
    """Run upload steps and map all failures into context state."""

    def __init__(self, steps: List[UploadPipelineStep], data_save_service):
        self.steps = steps
        self.data_save_service = data_save_service

    async def run_for_file(self, ctx: UploadPipelineContext) -> None:
        for step in self.steps:
            try:
                await step.execute(ctx)

            except (CriticalUploadError, CriticalParsingError) as error:
                log_app_error(error)
                await self._handle_critical_error(ctx, error)
                return

            except DuplicateFileError as error:
                log_app_error(error)
                ctx.failed = True
                ctx.error = error.message
                return

            except (NonCriticalUploadError, NonCriticalParsingError) as error:
                if config.DEBUG:
                    log_app_error(error)
                continue

            except Exception as exc:
                error = CriticalUploadError(
                    message=f"Unexpected pipeline error on step {step.__class__.__name__}: {exc}",
                    domain="upload.pipeline",
                    http_status=500,
                    meta={
                        "step": step.__class__.__name__,
                        "file_name": getattr(ctx.file, "filename", None),
                        "error": str(exc),
                    },
                )
                log_app_error(error, exc_info=True)
                await self._handle_critical_error(ctx, error)
                return

    async def _handle_critical_error(self, ctx: UploadPipelineContext, error: Exception) -> None:
        """Mark context as failed and rollback the single acquired file record."""
        ctx.failed = True
        ctx.error = error.message if hasattr(error, "message") else str(error)

        if not ctx.file_model or not getattr(ctx.file_model, "file_id", None):
            logger.error(
                "Critical error for '%s', but file record was not acquired; failed state in DB was not persisted",
                getattr(ctx.file, "filename", None),
            )
            return

        try:
            await self.data_save_service.rollback(ctx.file_model, ctx.error)
            logger.info("Rollback completed for file %s", ctx.file_model.file_id)
        except Exception as rollback_err:
            logger.error(
                "Rollback error for file %s: %s",
                ctx.file_model.file_id,
                rollback_err,
            )


def build_default_pipeline(
    file_service,
    data_save_service,
) -> UploadPipelineRunner:
    """Build default upload pipeline."""
    steps: List[UploadPipelineStep] = [
        AcquireFileRecordStep(file_service),
        ReadFileContentStep(),
        ExtractMetadataStep(file_service),
        SyncFileMetadataStep(),
        ProcessSheetsStep(),
        EnrichFlatDataStep(),
        PersistStep(data_save_service),
    ]
    return UploadPipelineRunner(steps=steps, data_save_service=data_save_service)
