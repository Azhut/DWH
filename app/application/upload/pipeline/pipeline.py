"""Оркестратор upload pipeline: запускает шаги для каждого файла, обрабатывает ошибки."""
import logging
from typing import List

from fastapi import HTTPException

from app.api.v2.schemas.upload import FileResponse
from app.application.upload.pipeline.context import UploadPipelineContext
from app.application.upload.pipeline.steps import (
    UploadPipelineStep,
    ValidateRequestStep,
    ExtractMetadataStep,
    CheckUniquenessStep,
    LoadFormStep,
    CreateFileModelStep,
    ProcessSheetsStep,
    EnrichFlatDataStep,
    PersistStep,
)
from app.domain.file.models import FileModel, FileStatus

logger = logging.getLogger(__name__)


class UploadPipelineRunner:
    def __init__(self, steps: List[UploadPipelineStep], data_save_service):
        self.steps = steps
        self.data_save_service = data_save_service

    async def run_for_file(self, ctx: UploadPipelineContext) -> None:
        try:
            for step in self.steps:
                await step.execute(ctx)
            return
        except HTTPException as e:
            err_msg = e.detail if isinstance(e.detail, str) else str(e)
            await self._save_stub_and_set_response(ctx, err_msg)
        except Exception as e:
            logger.exception("Непредвиденная ошибка при обработке файла: %s", e)
            err_msg = str(e)
            await self._save_stub_and_set_response(ctx, err_msg)

    async def _save_stub_and_set_response(self, ctx: UploadPipelineContext, err_msg: str) -> None:
        stub = FileModel.create_stub(
            file_id=ctx.file.filename,
            filename=ctx.file.filename,
            form_id=ctx.form_id,
            error_message=err_msg,
            year=ctx.file_info.year if ctx.file_info else None,
            city=ctx.file_info.city if ctx.file_info else None,
        )
        stub.form_id = ctx.form_id
        try:
            await self.data_save_service.save_file(stub)
        except Exception:
            logger.exception("Не удалось сохранить stub запись файла")

        ctx.file_response = FileResponse(
            filename=ctx.file.filename,
            status=FileStatus.FAILED.value,
            error=err_msg,
        )
        ctx.failed = True


def build_default_pipeline(
    file_service,
    form_service,
    sheet_service,
    data_save_service,
) -> UploadPipelineRunner:
    """Собирает pipeline с шагами по умолчанию. Шаги делегируют агрегатам: file, form, sheet."""
    steps: List[UploadPipelineStep] = [
        ValidateRequestStep(),
        ExtractMetadataStep(file_service),
        CheckUniquenessStep(file_service),
        LoadFormStep(form_service),
        CreateFileModelStep(),
        ProcessSheetsStep(sheet_service),
        EnrichFlatDataStep(),
        PersistStep(data_save_service),
    ]
    return UploadPipelineRunner(steps=steps, data_save_service=data_save_service)
