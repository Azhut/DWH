"""
Оркестратор эндпоинта upload: делегирует обработку каждого файла upload pipeline.
Для других эндпоинтов — другие оркестраторы (managers).
"""
from typing import List

from fastapi import UploadFile

from app.api.v2.schemas.upload import UploadResponse, FileResponse
from app.models.file_status import FileStatus
from app.data.services.data_save import DataSaveService
from app.services.file_handling_service import FileHandlingService
from app.services.form_service import FormService
from app.services.sheet_handling_service import SheetHandlingService
from app.core.logger import logger
from app.services.upload_pipeline import UploadPipelineContext, build_default_pipeline


class UploadManager:
    """Оркестратор эндпоинта upload: запускает pipeline для каждого файла и собирает ответ."""

    def __init__(
        self,
        file_handling_service: FileHandlingService,
        form_service: FormService,
        sheet_handling_service: SheetHandlingService,
        data_save_service: DataSaveService,
    ):
        self._file_service = file_handling_service
        self._form_service = form_service
        self._sheet_service = sheet_handling_service
        self._data_save_service = data_save_service
        self._pipeline = build_default_pipeline(
            file_handling_service=file_handling_service,
            form_service=form_service,
            sheet_handling_service=sheet_handling_service,
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
