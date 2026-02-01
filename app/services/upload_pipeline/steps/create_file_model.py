"""Шаг: создание FileModel до обработки листов. Использует file_info из контекста."""
from app.models.file_model import FileModel
from app.services.upload_pipeline.context import UploadPipelineContext


class CreateFileModelStep:
    """Создаёт FileModel с UUID; данные берёт из file_info (результат FileHandlingService)."""

    async def execute(self, ctx: UploadPipelineContext) -> None:
        file_model = FileModel.create_new(
            filename=ctx.file_info.filename,
            year=ctx.file_info.year,
            city=ctx.file_info.city,
            form_id=ctx.form_id,
        )
        file_model.form_id = ctx.form_id
        ctx.file_model = file_model
