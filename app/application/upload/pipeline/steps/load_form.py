"""Шаг: загрузка формы через сервис агрегата Form."""
import logging

from app.application.upload.pipeline.context import UploadPipelineContext
from app.domain.form.service import FormService

logger = logging.getLogger(__name__)


class LoadFormStep:
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
