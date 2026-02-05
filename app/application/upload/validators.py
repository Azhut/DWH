import logging
from typing import List
from fastapi import UploadFile
from app.core.exceptions import RequestValidationError
from app.domain.form.service import FormService

logger = logging.getLogger(__name__)


class UploadRequestValidator:
    """
    Валидатор запроса загрузки файлов.
    Проверяет:
    - Наличие и валидность form_id
    - Наличие файлов
    - Существование формы в БД
    """

    def __init__(self, form_service: FormService):
        self._form_service = form_service

    async def validate(
            self,
            files: List[UploadFile],
            form_id: str
    ) -> None:
        """
        Валидация входных данных запроса.

        Raises:
            RequestValidationError: При любой ошибке валидации
        """
        # Проверка form_id в запросе
        if not form_id or not form_id.strip():
            raise RequestValidationError(
                message="Параметр form_id обязателен",
                http_status=400,
                meta={"form_id": form_id}
            )

        # Проверка наличия файлов
        if not files or len(files) == 0:
            raise RequestValidationError(
                message="Не предоставлены файлы для загрузки",
                http_status=400,
                meta={"form_id": form_id}
            )

        # Проверка существования формы
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