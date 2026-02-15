"""Шаг: извлечение метаданных из имени файла."""
import logging
from app.application.upload.pipeline.context import UploadPipelineContext
from app.core.exceptions import CriticalUploadError
from app.domain.file.service import FileService

logger = logging.getLogger(__name__)


class ExtractMetadataStep:
    """
    Извлекает метаданные файла (субъект, год) из имени файла.

    ВХОД:
    - ctx.filename (установлен в ReadFileContentStep)

    ВЫХОД:
    - ctx.file_info (FileInfo с reporter, year, extension)

    ЗАВИСИМОСТЬ:
    - FileService.validate_and_extract_metadata()
    """

    def __init__(self, file_service: FileService):
        self._file_service = file_service

    async def execute(self, ctx: UploadPipelineContext) -> None:
        """
        Извлекает из имени файла метаданные (reporter, year и т.п.).
        Если не удалось — бросает CriticalUploadError.
        """
        if not ctx.file.filename:
            raise CriticalUploadError(
                message="Имя файла не установлено в контексте",
                domain="upload.extract_metadata",
                http_status=500,
                meta={"context_state": "filename is None"},
            )

        try:
            file_info = self._file_service.validate_and_extract_metadata_from_filename(
                ctx.file.filename
            )
        except Exception as e:
            raise CriticalUploadError(
                message=f"Ошибка при извлечении метаданных файла: {e}",
                domain="upload.extract_metadata",
                http_status=500,
                meta={"file_name": ctx.file.filename, "error": str(e)},
            ) from e

        if not file_info or not file_info.reporter or not file_info.year:
            raise CriticalUploadError(
                message="Не удалось извлечь субъект или год из имени файла",
                domain="upload.extract_metadata",
                http_status=400,
                meta={"file_name": ctx.file.filename},
            )

        ctx.file_info = file_info

        logger.debug(
            "Метаданные извлечены: файл='%s', субъект='%s', год=%d",
            ctx.file.filename,
            file_info.reporter,
            file_info.year
        )