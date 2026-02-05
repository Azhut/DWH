import logging
from typing import List
from app.application.upload.pipeline.context import UploadPipelineContext
from app.core.exceptions import (
    CriticalUploadError,
    NonCriticalUploadError,
    CriticalParsingError,
    NonCriticalParsingError,
    log_app_error
)
from app.application.upload.pipeline.steps import (
    UploadPipelineStep,
    ExtractMetadataStep,
    CheckUniquenessStep,
    CreateFileModelStep,
    ProcessSheetsStep,
    EnrichFlatDataStep,
    PersistStep,
)
from app.domain.file import FileModel
from config import config

logger = logging.getLogger(__name__)


class UploadPipelineRunner:
    """
    Оркестратор upload pipeline: запускает шаги для каждого файла, обрабатывает ошибки.

    Обработка ошибок:
    - CriticalUploadError / CriticalParsingError: останавливает pipeline, делает rollback, сохраняет stub
    - NonCriticalUploadError / NonCriticalParsingError: логирует в DEBUG режиме, продолжает выполнение
    - Exception: обрабатывается как критическая ошибка

    Никогда не выбрасывает ошибки наружу — все ошибки сохраняются в контексте.
    """

    def __init__(self, steps: List[UploadPipelineStep], data_save_service):
        self.steps = steps
        self.data_save_service = data_save_service

    async def run_for_file(self, ctx: UploadPipelineContext) -> None:
        """
        Запускает все шаги пайплайна для одного файла.
        """

        for step_idx, step in enumerate(self.steps):
            try:
                await step.execute(ctx)

            except CriticalUploadError as e:
                log_app_error(e)
                await self._handle_critical_error(ctx, e)
                return

            except CriticalParsingError as e:
                log_app_error(e)
                await self._handle_critical_error(ctx, e)
                return

            except NonCriticalUploadError as e:
                if config.DEBUG:
                    log_app_error(e)

                continue

            except NonCriticalParsingError as e:
                if config.DEBUG:
                    log_app_error(e)
                continue

            except Exception as e:

                error = CriticalUploadError(
                    message=f"Непредвиденная ошибка на шаге {step.__class__.__name__}: {str(e)}",
                    domain="upload.pipeline",
                    http_status=500,
                    meta={
                        "step": step.__class__.__name__,
                        "file_name": ctx.file.filename,
                        "error": str(e),
                    },
                )
                log_app_error(error, exc_info=True)
                await self._handle_critical_error(ctx, error)
                return

    async def _handle_critical_error(self, ctx: UploadPipelineContext, error: Exception) -> None:
        """
        Обрабатывает критическую ошибку:
        1. Помечает контекст как failed
        2. Делает rollback плоских данных (если были сохранены)
        3. Сохраняет stub файл с информацией об ошибке
        """
        ctx.failed = True
        ctx.error = error.message if hasattr(error, 'message') else str(error)

        # Rollback плоских данных, если они были сохранены
        if ctx.file_model and getattr(ctx.file_model, "file_id", None):
            try:
                await self.data_save_service.rollback(ctx.file_model, ctx.error)
                logger.info("Rollback плоских данных выполнен для файла %s", ctx.file_model.file_id)
            except Exception as rollback_err:
                logger.error(
                    "Ошибка при rollback для файла %s: %s",
                    ctx.file_model.file_id,
                    rollback_err,
                )

        # Сохранение stub файла с информацией об ошибке
        await self._save_stub(ctx, ctx.error)

    async def _save_stub(self, ctx: UploadPipelineContext, err_msg: str) -> None:
        """
        Сохраняет stub запись файла с информацией об ошибке.
        Это позволяет клиенту видеть, какие файлы нужно загрузить повторно.
        """
        try:
            stub = FileModel.create_stub(
                file_id=ctx.file.filename,
                filename=ctx.file.filename,
                form_id=ctx.form_id,
                error_message=err_msg,
                year=ctx.file_info.year if ctx.file_info else None,
                city=ctx.file_info.city if ctx.file_info else None,
            )
            stub.form_id = ctx.form_id


            await self.data_save_service.save_file(stub)
            logger.info("Stub запись сохранена для файла %s", ctx.file.filename)
        except Exception:
            logger.exception("Не удалось сохранить stub запись файла %s", ctx.file.filename)


def build_default_pipeline(
        file_service,
        sheet_service,
        data_save_service,
) -> UploadPipelineRunner:
    """
    Собирает pipeline с шагами по умолчанию.
    Шаги делегируют агрегатам: file, form, sheet.
    """
    steps: List[UploadPipelineStep] = [
        ExtractMetadataStep(file_service),
        CheckUniquenessStep(file_service),
        CreateFileModelStep(),
        ProcessSheetsStep(sheet_service),
        EnrichFlatDataStep(),
        PersistStep(data_save_service),
    ]
    return UploadPipelineRunner(steps=steps, data_save_service=data_save_service)