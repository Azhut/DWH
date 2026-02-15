"""Шаг: проверка уникальности файла."""
import logging
from app.application.upload.pipeline.context import UploadPipelineContext
from app.core.exceptions import CriticalUploadError
from app.domain.file.service import FileService

logger = logging.getLogger(__name__)


class CheckUniquenessStep:
    """
    Проверяет, что файл с таким именем ещё не был загружен.

    ВХОД:
    - ctx.filename (из ReadFileContentStep)

    ИСКЛЮЧЕНИЕ:
    - CriticalUploadError если файл уже существует
    """

    def __init__(self, file_service: FileService):
        self._file_service = file_service

    async def execute(self, ctx: UploadPipelineContext) -> None:
        """Проверяет уникальность имени файла."""
        if not ctx.file.filename:
            raise CriticalUploadError(
                message="Имя файла не установлено в контексте",
                domain="upload.check_uniqueness",
                http_status=500,
                meta={"context_state": "filename is None"},
            )

        try:
            unique = await self._file_service.is_filename_unique(ctx.file.filename)
        except Exception as e:
            raise CriticalUploadError(
                message=f"Ошибка при проверке уникальности файла: {str(e)}",
                domain="upload.check_uniqueness",
                http_status=500,
                meta={"file_name": ctx.file.filename, "form_id": ctx.form_id, "error": str(e)},
            ) from e

        if not unique:
            raise CriticalUploadError(
                message=f"Файл '{ctx.file.filename}' уже был загружен.",
                domain="upload.check_uniqueness",
                http_status=409,
                meta={"file_name": ctx.file.filename, "form_id": ctx.form_id}
            )

        logger.debug("Файл '%s' уникален, продолжаем обработку", ctx.file.filename)