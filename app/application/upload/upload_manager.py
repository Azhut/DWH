import logging
from typing import List
from fastapi import UploadFile
from app.api.v2.schemas.upload import UploadResponse, FileResponse
from app.application.upload.pipeline import UploadPipelineContext, build_default_pipeline
from app.application.data import DataSaveService
from app.domain.file.models import FileStatus
from app.domain.file.service import FileService
from app.domain.form.service import FormService
from app.domain.sheet.service import SheetService
from app.core.exceptions import CriticalUploadError, log_app_error, RequestValidationError

logger = logging.getLogger(__name__)


class UploadManager:
    """
    Управляет загрузкой файлов через upload pipeline.

    Ответственность:
    1. Валидация запроса (form_id, files, существование формы)
    2. Запуск pipeline для каждого файла
    3. Сбор результатов обработки
    4. Формирование ответа
    """

    def __init__(
        self,
        file_service: FileService,
        form_service: FormService,
        sheet_service: SheetService,
        data_save_service: DataSaveService,
    ):
        self._file_service = file_service
        self._form_service = form_service
        self._sheet_service = sheet_service
        self._data_save_service = data_save_service
        self._pipeline = build_default_pipeline(
            file_service=file_service,
            form_service=form_service,
            sheet_service=sheet_service,
            data_save_service=data_save_service,
        )

    async def upload_files(
            self,
            files: List[UploadFile],
            form_id: str,
    ) -> UploadResponse:
        """
        Обрабатывает загрузку файлов.

        Этапы:
        1. Валидация запроса (может выбросить RequestValidationError)
        2. Обработка каждого файла через pipeline
        3. Возврат результатов (всегда 200 OK с деталями)

        Raises:
            RequestValidationError: При ошибках валидации запроса
                (отсутствие form_id, файлов, несуществующая форма, проблемы с БД)

        Returns:
            UploadResponse: Всегда возвращает 200 OK со списком результатов по файлам
        """

        # ============= Валидация запроса =============
        await self._validate_request(files, form_id)

        logger.info(
            "Начало обработки %d файл(ов) для формы '%s'",
            len(files),
            form_id,
        )

        # ============= Обработка файлов =============
        file_responses = await self._process_files(files, form_id)

        # ============= Формирование ответа =============
        success_count = sum(1 for r in file_responses if r.status == FileStatus.SUCCESS)
        failure_count = len(file_responses) - success_count

        logger.info(
            "Обработка завершена. Успешно: %d, с ошибками: %d",
            success_count,
            failure_count,
        )

        return UploadResponse(
            message=f"{success_count} files processed successfully, {failure_count} failed.",
            details=file_responses,
        )

    async def _validate_request(self, files: List[UploadFile], form_id: str) -> None:
        """
        Валидация входных данных запроса.

        Проверки:
        1. form_id не пустой
        2. Список файлов не пустой
        3. Форма существует в БД

        Raises:
            RequestValidationError: При любой ошибке валидации
        """

        if not form_id or not form_id.strip():
            raise RequestValidationError(
                message="Параметр form_id обязателен",
                http_status=400,
                meta={"form_id": form_id}
            )

        if not files or len(files) == 0:
            raise RequestValidationError(
                message="Не предоставлены файлы для загрузки",
                http_status=400,
                meta={"form_id": form_id}
            )

        try:
            form_info = await self._form_service.get_form_info(form_id)
            if not form_info:
                raise RequestValidationError(
                    message=f"Форма с ID '{form_id}' не найдена",
                    http_status=404,
                    meta={"form_id": form_id}
                )

        except RequestValidationError:
            raise
        except Exception as e:

            raise RequestValidationError(
                message=f"Ошибка при проверке формы: {str(e)}",
                http_status=500,
                meta={"form_id": form_id, "error": str(e)}
            )

    async def _process_files(
            self,
            files: List[UploadFile],
            form_id: str
    ) -> List[FileResponse]:
        """
        Обрабатывает каждый файл через pipeline.

        Для каждого файла:
        1. Создаёт контекст
        2. Запускает pipeline
        3. Проверяет результат (failed или success)
        4. Добавляет результат в список

        Args:
            files: Список загруженных файлов
            form_id: ID формы

        Returns:
            Список FileResponse с результатами обработки каждого файла
        """
        file_responses: List[FileResponse] = []

        for file in files:
            logger.info("Обработка файла: '%s'", file.filename)

            # Создаём контекст для файла
            ctx = UploadPipelineContext(
                file=file,
                form_id=form_id,
            )

            try:
                # Запускаем pipeline для файла
                await self._pipeline.run_for_file(ctx)

                # Проверяем результат
                if ctx.failed:
                    file_responses.append(
                        FileResponse(
                            filename=file.filename,
                            status=FileStatus.FAILED,
                            error=ctx.error or "Неизвестная ошибка",
                        )
                    )
                else:
                    file_responses.append(
                        FileResponse(
                            filename=file.filename,
                            status=FileStatus.SUCCESS,
                            error="",
                        )
                    )
                    form_type = getattr(getattr(ctx.form_info, "type", None), "value", "?")
                    logger.info(
                        "Файл '%s' успешно обработан. Тип формы: %s, листов: %d, записей: %d",
                        file.filename,
                        form_type,
                        len(ctx.sheet_models or []),
                        len(ctx.flat_data or []),
                    )

            except Exception as e:
                error = CriticalUploadError(
                    message=f"Внутренняя ошибка обработки файла: {str(e)}",
                    domain="upload.manager",
                    http_status=500,
                    meta={"file_name": file.filename, "form_id": form_id, "error": str(e)},
                )
                log_app_error(error, exc_info=True)

                file_responses.append(
                    FileResponse(
                        filename=file.filename,
                        status=FileStatus.FAILED,
                        error=error.message,
                    )
                )

        return file_responses