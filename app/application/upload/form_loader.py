import logging

from app.core.exceptions import FormValidationError, RequestValidationError
from app.domain.form.models import FormInfo
from app.domain.form.service import FormService

logger = logging.getLogger(__name__)


class FormLoader:
    """Loads and validates form once per upload request."""

    def __init__(self, form_service: FormService):
        self._form_service = form_service

    async def load_form(self, form_id: str) -> FormInfo:
        try:
            form_info = await self._form_service.get_form_or_raise(form_id)
            logger.info(
                "Form loaded: id=%s, name='%s', type=%s",
                form_info.id,
                form_info.name,
                form_info.type.value,
            )
            return form_info

        except FormValidationError as exc:
            raise RequestValidationError(
                message=exc.message,
                http_status=404,
                domain="form.validation",
                meta={"form_id": exc.form_id},
            ) from exc

        except RequestValidationError:
            raise

        except Exception as exc:
            raise RequestValidationError(
                message=f"Failed to load form: {exc}",
                http_status=500,
                domain="upload.form_loader",
                meta={"form_id": form_id, "error": str(exc)},
            ) from exc
