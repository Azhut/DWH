import logging
import uuid

from app.application.upload.pipeline.context import UploadPipelineContext
from app.core.exceptions import CriticalUploadError

logger = logging.getLogger(__name__)


class ReadFileContentStep:
    """
    Читает UploadFile в bytes и сохраняет в контексте.

    ОТВЕТСТВЕННОСТЬ:
    - Однократное чтение файла
    - Кеширование ctx.file_content
    - Фиксация ctx.filename

    ПОСЛЕ ШАГА:
    - ctx.file больше не используется
    - все шаги работают только с bytes
    """

    async def execute(self, ctx: UploadPipelineContext) -> None:
        try:
            # filename нужен только для логов и мета
            ctx.filename = ctx.file.filename or f"{uuid.uuid4()}.bin"

            await ctx.file.seek(0)
            content = await ctx.file.read()

            if not content:
                raise ValueError("Файл пустой")

            ctx.file_content = content

            logger.info(
                "✓ Файл '%s' прочитан в память: %d байт",
                ctx.filename,
                len(content)
            )

        except ValueError as e:
            raise CriticalUploadError(
                message=str(e),
                domain="upload.read_file",
                http_status=400,
                meta={"file_name": ctx.filename},
            ) from e

        except Exception as e:
            logger.exception(
                "Неожиданная ошибка при чтении файла '%s'",
                ctx.filename
            )
            raise CriticalUploadError(
                message=f"Ошибка при чтении файла '{ctx.filename}': {str(e)}",
                domain="upload.read_file",
                http_status=500,
                meta={"file_name": ctx.filename},
            ) from e
