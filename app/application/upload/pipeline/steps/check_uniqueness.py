from app.application.upload.pipeline.context import UploadPipelineContext
from app.core.exceptions import CriticalUploadError
from app.domain.file.service import FileService
import logging


class CheckUniquenessStep:
    """
    Проверяет уникальность имени файла.
    Если файл уже был загружен, выбрасывает CriticalUploadError.
    """

    def __init__(self, file_service: FileService):
        self._file_service = file_service

    async def execute(self, ctx: UploadPipelineContext) -> None:
        try:
            unique = await self._file_service.is_filename_unique(ctx.file.filename)
        except Exception as e:
            error = CriticalUploadError(
                message=f"Ошибка при проверке уникальности файла: {e}",
                domain="upload.uniqueness",
                http_status=500,
                meta={"file_name": ctx.file.filename, "form_id": ctx.form_id, "error": str(e)},
            )
            raise error

        if not unique:
            raise CriticalUploadError(
                message=f"Файл '{ctx.file.filename}' уже был загружен.",
                domain="upload.uniqueness",
                http_status=400,
                meta={"file_name": ctx.file.filename, "form_id": ctx.form_id}
            )
