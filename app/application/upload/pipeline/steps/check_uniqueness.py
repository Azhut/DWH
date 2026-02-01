"""Шаг: проверка уникальности имени файла через сервис агрегата File."""
from fastapi import HTTPException

from app.application.upload.pipeline.context import UploadPipelineContext
from app.domain.file.service import FileService


class CheckUniquenessStep:
    def __init__(self, file_service: FileService):
        self._file_service = file_service

    async def execute(self, ctx: UploadPipelineContext) -> None:
        unique = await self._file_service.is_filename_unique(ctx.file.filename)
        if not unique:
            raise HTTPException(status_code=400, detail=f"Файл '{ctx.file.filename}' уже был загружен.")
