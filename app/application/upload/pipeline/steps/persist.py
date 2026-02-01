"""Шаг: сохранение файла и flat_data через координатор DataSaveService."""
from app.application.upload.data_save import DataSaveService
from app.application.upload.pipeline.context import UploadPipelineContext


class PersistStep:
    def __init__(self, data_save_service: DataSaveService):
        self.data_save_service = data_save_service

    async def execute(self, ctx: UploadPipelineContext) -> None:
        await self.data_save_service.process_and_save_all(ctx.file_model, ctx.flat_data or [])
