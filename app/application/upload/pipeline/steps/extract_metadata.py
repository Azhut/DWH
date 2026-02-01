"""Шаг: извлечение метаданных файла через сервис агрегата File."""
from fastapi import HTTPException

from app.application.upload.pipeline.context import UploadPipelineContext
from app.domain.file.service import FileService


class ExtractMetadataStep:
    def __init__(self, file_service: FileService):
        self._file_service = file_service

    async def execute(self, ctx: UploadPipelineContext) -> None:
        file_info = self._file_service.validate_and_extract_metadata(ctx.file)
        if not file_info.city or not file_info.year:
            raise HTTPException(status_code=400, detail="Не удалось извлечь город или год из имени файла")
        ctx.file_info = file_info
