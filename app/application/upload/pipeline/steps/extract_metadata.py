from app.application.upload.pipeline.context import UploadPipelineContext
from app.core.exceptions import CriticalUploadError
from app.domain.file.service import FileService


class ExtractMetadataStep:
    """Шаг: извлечение метаданных файла через сервис агрегата File."""
    def __init__(self, file_service: FileService):
        self._file_service = file_service

    async def execute(self, ctx: UploadPipelineContext) -> None:
        """
        Извлекает из имени файла метаданные (city, year и т.п.).
        Если не удалось — бросает CriticalUploadError (ошибка клиента / запроса)
        """
        try:
            file_info = self._file_service.validate_and_extract_metadata(ctx.file)
        except Exception as e:
            raise CriticalUploadError(
                message=f"Ошибка при извлечении метаданных файла: {e}",
                domain="upload.extract_metadata",
                http_status=500,
                meta={"file_name": getattr(ctx.file, "filename", None), "error": str(e)},
            )

        if not file_info or not getattr(file_info, "city", None) or not getattr(file_info, "year", None):
            raise CriticalUploadError(
                message="Не удалось извлечь город или год из имени файла",
                domain="upload.extract_metadata",
                http_status=400,
                meta={"file_name": getattr(ctx.file, "filename", None)},
            )

        ctx.file_info = file_info
