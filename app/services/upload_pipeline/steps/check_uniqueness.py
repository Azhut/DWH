"""Шаг: проверка уникальности файла. Делегирует сервису по работе с файлами."""
from fastapi import HTTPException

from app.services.upload_pipeline.context import UploadPipelineContext
from app.services.file_handling_service import FileHandlingService


class CheckUniquenessStep:
    """Проверка уникальности имени файла делегируется FileHandlingService."""

    def __init__(self, file_handling_service: FileHandlingService):
        self._file_service = file_handling_service

    async def execute(self, ctx: UploadPipelineContext) -> None:
        unique = await self._file_service.is_filename_unique(ctx.file.filename)
        if not unique:
            raise HTTPException(
                status_code=400,
                detail=f"Файл '{ctx.file.filename}' уже был загружен.",
            )
