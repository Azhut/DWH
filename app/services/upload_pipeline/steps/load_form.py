"""Шаг: загрузка формы. Делегирует сервису по работе с формами."""
from app.models.form_model import FormInfo
from app.services.upload_pipeline.context import UploadPipelineContext
from app.services.form_service import FormService
from app.core.logger import logger


class LoadFormStep:
    """Загрузка формы и реквизитов (в т.ч. skip_sheets) делегируется FormService."""

    def __init__(self, form_service: FormService):
        self._form_service = form_service

    async def execute(self, ctx: UploadPipelineContext) -> None:
        form_info = await self._form_service.get_form_info_or_raise(ctx.form_id)
        ctx.form_info = form_info
        logger.info(
            "Обработка файла '%s' для формы '%s' (ID: %s, тип: %s)",
            ctx.file.filename,
            form_info.name,
            ctx.form_id,
            form_info.type.value,
        )
