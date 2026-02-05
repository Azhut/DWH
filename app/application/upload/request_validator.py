from typing import List
from fastapi import UploadFile
from app.core.exceptions import RequestValidationError, FormValidationError
from app.domain.form import validate_form_id


class RequestValidator:
    """
    Валидатор запроса загрузки файлов.
    Проверяет:
    - Формат form_id (не пустой, строка)
    - Наличие файлов в запросе
    """

    @staticmethod
    def validate_request(
            files: List[UploadFile],
            form_id: str
    ) -> str:
        """
        Валидация входных данных запроса.

        Args:
            files: Список загруженных файлов
            form_id: ID формы из запроса

        Returns:
            Очищенный form_id

        Raises:
            RequestValidationError: При любой ошибке валидации
        """
        try:
            form_id = validate_form_id(form_id)
        except FormValidationError as e:
            raise RequestValidationError(
                message=e.message,
                http_status=400,
                domain="form.validation",
                meta={"form_id": e.form_id}
            )

        if not files or len(files) == 0:
            raise RequestValidationError(
                message="Не предоставлены файлы для загрузки",
                http_status=400,
                meta={"form_id": form_id}
            )

        return form_id.strip()