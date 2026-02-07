"""Шаг: чтение содержимого файла в память (один раз)."""
import logging
from app.application.upload.pipeline.context import UploadPipelineContext
from app.core.exceptions import CriticalUploadError

logger = logging.getLogger(__name__)


class ReadFileContentStep:
    """
    Читает содержимое UploadFile в память и кеширует в контексте.

    ОТВЕТСТВЕННОСТЬ:
    1. Надёжное чтение файла (4 метода с fallback)
    2. Кеширование содержимого в ctx.file_content
    3. Извлечение filename в ctx.filename для последующих шагов

    ПОСЛЕ ЭТОГО ШАГА:
    - ctx.file больше НЕ используется в pipeline
    - Все последующие шаги работают с ctx.file_content и ctx.filename

    ЗАЧЕМ:
    - Решает проблему SpooledTemporaryFile (файл может быть уже прочитан)
    - Избегает повторного чтения файла
    - Изолирует логику работы с FastAPI UploadFile в одном месте
    """

    async def execute(self, ctx: UploadPipelineContext) -> None:
        """Читает файл в память и извлекает filename."""
        try:
            # Извлекаем filename (нужен для всех последующих шагов)
            ctx.filename = ctx.file.filename or "unknown"

            # Читаем содержимое файла
            content = await self._read_file_reliably(ctx.file)

            # Кешируем в контексте
            ctx.file_content = content

            logger.info(
                "✓ Файл '%s' прочитан в память: %d байт",
                ctx.filename,
                len(content)
            )

        except ValueError as e:
            # Ошибка чтения файла
            raise CriticalUploadError(
                message=str(e),
                domain="upload.read_file",
                http_status=400,
                meta={"file_name": ctx.file.filename},
            ) from e
        except Exception as e:
            logger.exception("Неожиданная ошибка при чтении файла '%s'", ctx.file.filename)
            raise CriticalUploadError(
                message=f"Ошибка при чтении файла '{ctx.file.filename}': {str(e)}",
                domain="upload.read_file",
                http_status=500,
                meta={"file_name": ctx.file.filename, "error": str(e)},
            ) from e

    async def _read_file_reliably(self, file) -> bytes:
        """
        Надёжное чтение UploadFile с поддержкой SpooledTemporaryFile.

        Пробует 4 метода чтения в порядке приоритета:
        1. UploadFile.read() - стандартный способ FastAPI
        2. file.file.read() - прямое чтение из file объекта
        3. file.file._file.read() - через внутренний объект SpooledTemporaryFile
        4. rollover() + read() - принудительный сброс на диск

        Returns:
            bytes: содержимое файла

        Raises:
            ValueError: если ни один метод не сработал
        """
        filename = file.filename or "unknown"
        file_obj = file.file
        content = None

        # Метод 1: Стандартный способ
        try:
            await file.seek(0)
            content = await file.read()
            if content and len(content) > 0:
                logger.debug("Файл '%s': метод 1 (UploadFile.read) - %d байт", filename, len(content))
                return content
        except Exception as e:
            logger.debug("Метод 1 не сработал для '%s': %s", filename, e)

        # Метод 2: Прямое чтение
        try:
            if hasattr(file_obj, 'seek'):
                file_obj.seek(0)
            if hasattr(file_obj, 'read'):
                content = file_obj.read()
                if isinstance(content, str):
                    content = content.encode('utf-8')
                if content and len(content) > 0:
                    logger.debug("Файл '%s': метод 2 (file.read) - %d байт", filename, len(content))
                    return content
        except Exception as e:
            logger.debug("Метод 2 не сработал для '%s': %s", filename, e)

        # Метод 3: Через _file (SpooledTemporaryFile)
        try:
            if hasattr(file_obj, '_file'):
                inner = file_obj._file
                if hasattr(inner, 'seek'):
                    inner.seek(0)
                if hasattr(inner, 'read'):
                    content = inner.read()
                    if isinstance(content, str):
                        content = content.encode('utf-8')
                    if content and len(content) > 0:
                        logger.debug("Файл '%s': метод 3 (_file.read) - %d байт", filename, len(content))
                        return content
        except Exception as e:
            logger.debug("Метод 3 не сработал для '%s': %s", filename, e)

        # Метод 4: Rollover
        try:
            if hasattr(file_obj, 'rollover'):
                file_obj.rollover()
                file_obj.seek(0)
                content = file_obj.read()
                if isinstance(content, str):
                    content = content.encode('utf-8')
                if content and len(content) > 0:
                    logger.debug("Файл '%s': метод 4 (rollover) - %d байт", filename, len(content))
                    return content
        except Exception as e:
            logger.debug("Метод 4 не сработал для '%s': %s", filename, e)

        # Ничего не сработало - собираем диагностику
        diagnostics = {
            "file_type": type(file_obj).__name__,
            "content_type": file.content_type,
        }

        try:
            if hasattr(file_obj, 'tell'):
                diagnostics["position"] = file_obj.tell()
        except:
            pass

        raise ValueError(
            f"Файл '{filename}' пустой или не может быть прочитан. "
            f"Проверьте, что файл не повреждён и имеет содержимое. "
            f"Диагностика: {diagnostics}"
        )