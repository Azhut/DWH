"""Шаг: валидация входящего запроса (form_id) через единый сервис."""
from app.services.form_validation import validate_form_id
from app.services.upload_pipeline.context import UploadPipelineContext


class ValidateRequestStep:
    """Проверяет form_id через общий сервис валидации (единое место для всех эндпоинтов)."""

    async def execute(self, ctx: UploadPipelineContext) -> None:
        validate_form_id(ctx.form_id)
