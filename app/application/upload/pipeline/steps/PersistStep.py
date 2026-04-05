from app.application.data import DataSaveService
from app.application.upload.pipeline.context import UploadPipelineContext
from app.core.exceptions import CriticalUploadError
from app.core.profiling import profile_step


class PersistStep:
    """Сохраняет файл и flat_data через DataSaveService."""

    def __init__(self, data_save_service: DataSaveService):
        self.data_save_service = data_save_service

    @profile_step()
    async def execute(self, ctx: UploadPipelineContext) -> None:
        if not ctx.file_model:
            raise CriticalUploadError(
                message="file_model is not set before PersistStep",
                domain="upload.persist",
                http_status=500,
                meta={"file_name": ctx.filename},
            )

        try:
            await self.data_save_service.process_and_save_all(ctx.file_model, ctx.flat_data)
        except CriticalUploadError:
            raise
        except Exception as exc:
            raise CriticalUploadError(
                message=f"Failed to persist uploaded data: {exc}",
                domain="upload.persist",
                http_status=500,
                meta={"file_name": ctx.filename, "error": str(exc)},
            ) from exc
