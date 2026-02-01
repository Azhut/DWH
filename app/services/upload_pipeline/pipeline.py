"""
Оркестратор upload pipeline: запускает цепочку шагов для каждого файла,
обрабатывает ошибки и сохраняет stub при сбое.
"""
from datetime import datetime
from typing import List
from uuid import uuid4

from fastapi import HTTPException, UploadFile

from app.api.v2.schemas.upload import UploadResponse, FileResponse
from app.models.file_model import FileModel
from app.models.file_status import FileStatus
from app.services.upload_pipeline.context import UploadPipelineContext
from app.services.upload_pipeline.steps import (
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
from app.core.logger import logger


class UploadPipelineRunner:
    """
    Управляет выполнением pipeline загрузки файлов.
    Принимает список шагов и зависимости для обработки ошибок (data_save_service).
    """

    def __init__(
        self,
        steps: List[UploadPipelineStep],
        data_save_service,
    ):
        self.steps = steps
        self.data_save_service = data_save_service

    async def run_for_file(self, ctx: UploadPipelineContext) -> None:
        """
        Выполняет все шаги по порядку. При исключении сохраняет stub,
        устанавливает ctx.file_response в FAILED и ctx.failed = True.
        Исключение не пробрасывается — обработка остальных файлов продолжается.
        """
        try:
            for step in self.steps:
                await step.execute(ctx)
            # Успех: ответ формируется снаружи (в process_files)
            return
        except HTTPException as e:
            err_msg = e.detail if isinstance(e.detail, str) else str(e)
            await self._save_stub_and_set_response(ctx, err_msg)
        except Exception as e:
            logger.exception("Непредвиденная ошибка при обработке файла: %s", e)
            err_msg = str(e)
            await self._save_stub_and_set_response(ctx, err_msg)

    async def _save_stub_and_set_response(self, ctx: UploadPipelineContext, err_msg: str) -> None:
        """Сохраняет stub-запись файла и выставляет file_response в FAILED."""
        from app.api.v2.schemas.upload import FileResponse

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
    file_handling_service,
    form_service,
    sheet_handling_service,
    data_save_service,
) -> UploadPipelineRunner:
    """
    Собирает pipeline с шагами по умолчанию. Шаги делегируют:
    файлы — FileHandlingService, форма — FormService, листы — SheetHandlingService.
    """
    steps: List[UploadPipelineStep] = [
        ValidateRequestStep(),
        ExtractMetadataStep(file_handling_service),
        CheckUniquenessStep(file_handling_service),
        LoadFormStep(form_service),
        CreateFileModelStep(),
        ProcessSheetsStep(sheet_handling_service),
        EnrichFlatDataStep(),
        PersistStep(data_save_service),
    ]
    return UploadPipelineRunner(steps=steps, data_save_service=data_save_service)
