"""Шаг: проверка уникальности файла."""
import logging
from datetime import datetime

from app.application.upload.pipeline.context import UploadPipelineContext
from app.core.exceptions import CriticalUploadError, DuplicateFileError
from app.domain.file import FileStatus
from app.domain.file.service import FileService

logger = logging.getLogger(__name__)


class CheckUniquenessStep:
    """
    Проверяет уникальность имени файла.

    ЛОГИКА:
    1. Если найден файл со статусом SUCCESS → ошибка дубликата (не создаём новую запись)
    2. Если найден файл со статусом FAILED/PROCESSING → переиспользуем file_id (обновляем запись)
    3. Если файл не найден → создаём новую запись с новым file_id
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
            # 1️⃣ Проверяем на SUCCESS (дубликат — ошибка)
            existing_success = await self._file_service.get_by_filename_and_status(
                ctx.file.filename,
                FileStatus.SUCCESS
            )

            if existing_success:
                raise DuplicateFileError(
                    message=f"Файл '{ctx.file.filename}' уже был успешно загружен.",
                    domain="upload.check_uniqueness",
                    http_status=409,
                    meta={
                        "file_name": ctx.file.filename,
                        "form_id": ctx.form_id,
                        "existing_file_id": existing_success.file_id,
                    },
                )

            # 2️⃣ Проверяем на FAILED/PROCESSING (переиспользуем file_id)
            existing_any = await self._file_service.get_by_filename(ctx.file.filename)

            if existing_any:
                # ✅ Переиспользуем существующий file_id
                ctx.file_model = existing_any
                ctx.file_model.status = FileStatus.PROCESSING
                ctx.file_model.error = None
                ctx.file_model.updated_at = datetime.now()
                logger.debug(
                    "Файл '%s' найден со статусом %s, переиспользуем file_id=%s",
                    ctx.file.filename,
                    existing_any.status,
                    existing_any.file_id,
                )
                return

            # 3️⃣ Файл не найден — создадим новый в CreateFileModelStep
            logger.debug("Файл '%s' уникален, создадим новую запись", ctx.file.filename)

        except DuplicateFileError:
            raise
        except Exception as e:
            raise CriticalUploadError(
                message=f"Ошибка при проверке уникальности файла: {str(e)}",
                domain="upload.check_uniqueness",
                http_status=500,
                meta={
                    "file_name": ctx.file.filename,
                    "form_id": ctx.form_id,
                    "error": str(e)
                },
            ) from e