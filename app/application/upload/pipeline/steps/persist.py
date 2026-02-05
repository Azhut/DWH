from app.application.data import DataSaveService
from app.application.upload.pipeline.context import UploadPipelineContext
from app.core.exceptions import CriticalUploadError


class PersistStep:
    """Шаг: сохранение файла и flat_data через координатор DataSaveService."""
    def __init__(self, data_save_service: DataSaveService):
        self.data_save_service = data_save_service

    async def execute(self, ctx: UploadPipelineContext) -> None:
        """
        Делегирует фактическое сохранение в DataSaveService.
        Любая ошибка здесь считается критической для файла — переводим в CriticalUploadError.
        """
        try:
            await self.data_save_service.process_and_save_all(ctx.file_model, ctx.flat_data)
        except Exception as e:
            raise CriticalUploadError(
                message=f"Ошибка при сохранении данных файла: {e}",
                domain="upload.persist",
                http_status=500,
                meta={"file_name": getattr(ctx.file, "filename", None), "error": str(e)},
            )

