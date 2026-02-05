"""Обработка одного файла через upload pipeline."""
import logging
from fastapi import UploadFile
from app.api.v2.schemas.files import FileResponse
from app.application.upload.pipeline import UploadPipelineContext, UploadPipelineRunner
from app.domain.file.models import FileStatus
from app.domain.form.models import FormInfo
from app.core.exceptions import CriticalUploadError, log_app_error

logger = logging.getLogger(__name__)

class FileProcessor:
    """
    Обрабатывает один файл через upload pipeline.
    Отвечает за:
    - Создание контекста с готовой формой
    - Запуск pipeline
    - Обработку ошибок
    - Формирование результата
    """

    def __init__(self, pipeline: UploadPipelineRunner):
        self._pipeline = pipeline

    async def process_file(
        self,
        file: UploadFile,
        form_id: str,
        form_info: FormInfo
    ) -> FileResponse:
        """
        Обрабатывает один файл через pipeline.

        Args:
            file: Загруженный файл
            form_id: ID формы
            form_info: Информация о форме

        Returns:
            FileResponse с результатом обработки
        """
        logger.info("Обработка файла: '%s'", file.filename)


        ctx = UploadPipelineContext(
            file=file,
            form_id=form_id,
            form_info=form_info,
        )

        try:
            await self._pipeline.run_for_file(ctx)

            if ctx.failed:
                response = FileResponse(
                    filename=file.filename,
                    status=FileStatus.FAILED,
                    error=ctx.error or "Неизвестная ошибка",
                )
            else:
                response = FileResponse(
                    filename=file.filename,
                    status=FileStatus.SUCCESS,
                    error="",
                )

                form_type = getattr(getattr(ctx.form_info, "type", None), "value", "?")
                logger.info(
                    "Файл '%s' успешно обработан. Тип формы: %s, листов: %d, записей: %d",
                    file.filename,
                    form_type,
                    len(ctx.sheet_models or []),
                    len(ctx.flat_data or []),
                )

            return response

        except Exception as e:
            error = CriticalUploadError(
                message=f"Внутренняя ошибка обработки файла: {str(e)}",
                domain="upload.file_processor",
                http_status=500,
                meta={"file_name": file.filename, "form_id": form_id, "error": str(e)},
            )
            log_app_error(error, exc_info=True)

            return FileResponse(
                filename=file.filename,
                status=FileStatus.FAILED,
                error=error.message,
            )