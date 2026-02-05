import logging
from app.domain.form.service import FormService
from app.domain.form.models import FormInfo
from app.core.exceptions import RequestValidationError

logger = logging.getLogger(__name__)


class FormLoader:
    """
    Загружает форму из БД один раз для всех файлов в запросе.
    Выполняет проверку существования формы.
    """

    def __init__(self, form_service: FormService):
        self._form_service = form_service

    async def load_form(self, form_id: str) -> FormInfo:
        """
        Загружает форму из БД и проверяет её существование.

        Args:
            form_id: ID формы

        Returns:
            FormInfo с информацией о форме

        Raises:
            RequestValidationError: Если форма не найдена или ошибка БД
        """
        try:
            form_info = await self._form_service.get_form_or_raise(form_id)

            logger.info(
                "Форма загружена: ID=%s, имя='%s', тип=%s",
                form_info.id,
                form_info.name,
                form_info.type.value,
            )

            return form_info

        except RequestValidationError:
            raise
        except Exception as e:
            raise RequestValidationError(
                message=f"Ошибка при загрузке формы: {str(e)}",
                http_status=500,
                domain="upload.form_loader",
                meta={"form_id": form_id, "error": str(e)}
            )