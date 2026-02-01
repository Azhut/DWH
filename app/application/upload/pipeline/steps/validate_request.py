"""Шаг: валидация form_id через сервис агрегата Form."""
from app.application.upload.pipeline.context import UploadPipelineContext
from app.domain.form.service import validate_form_id


class ValidateRequestStep:
    async def execute(self, ctx: UploadPipelineContext) -> None:
        validate_form_id(ctx.form_id)
