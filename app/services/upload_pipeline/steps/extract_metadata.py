"""Шаг: извлечение метаданных файла. Делегирует сервису по работе с файлами."""
from fastapi import HTTPException

from app.services.upload_pipeline.context import UploadPipelineContext
from app.services.file_handling_service import FileHandlingService


class ExtractMetadataStep:
    """Проверка валидации и извлечение city/year делегируются FileHandlingService."""

    def __init__(self, file_handling_service: FileHandlingService):
        self._file_service = file_handling_service

    async def execute(self, ctx: UploadPipelineContext) -> None:
        file_info = self._file_service.validate_and_extract_metadata(ctx.file)
        if not file_info.city or not file_info.year:
            raise HTTPException(
                status_code=400,
                detail="Не удалось извлечь город или год из имени файла",
            )
        ctx.file_info = file_info
