"""Шаг: сохранение файла и flat_data в БД."""
from app.services.upload_pipeline.context import UploadPipelineContext


class PersistStep:
    """Сохраняет FileModel и flat_data через DataSaveService."""

    def __init__(self, data_save_service):
        self.data_save_service = data_save_service

    async def execute(self, ctx: UploadPipelineContext) -> None:
        await self.data_save_service.process_and_save_all(ctx.file_model, ctx.flat_data or [])
