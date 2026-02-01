"""Шаг: создание FileModel через модель агрегата File."""
from app.application.upload.pipeline.context import UploadPipelineContext
from app.domain.file.models import FileModel


class CreateFileModelStep:
    async def execute(self, ctx: UploadPipelineContext) -> None:
        file_model = FileModel.create_new(
            filename=ctx.file_info.filename,
            year=ctx.file_info.year,
            city=ctx.file_info.city,
            form_id=ctx.form_id,
        )
        file_model.form_id = ctx.form_id
        ctx.file_model = file_model
