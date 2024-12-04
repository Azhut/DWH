# app/features/files/services/ingestion_service.py
from app.models.file_model import FileModel
from app.api.v1.schemas.files import FileResponse, UploadResponse
from typing import List
from fastapi import UploadFile


class IngestionService:

    async def process_files(self, files: List[UploadFile]) -> List[FileModel]:
        """
        Обрабатывает список файлов и возвращает информацию о каждом.
        """
        file_models = []

        for file in files:
            status, error = await self.process_single_file(file)
            file_models.append(FileModel(file_id=file.filename, filename=file.filename, status=status, error=error))

        return file_models

    async def process_single_file(self, file: UploadFile) -> tuple[str, str]:
        """
        Загружает и обрабатывает один файл. Возвращает статус и ошибку, если она была.
        """
        try:
            # Логика обработки файла
            return "Успешно обработан", ""
        except Exception as e:
            return "Ошибка обработки", str(e)

    async def format_upload_response(self, file_models: List[FileModel]) -> UploadResponse:
        """
        Формирует финальный ответ для API, включая статистику.
        """
        success_count = sum("Успешно обработан" in file.status for file in file_models)
        failure_count = len(file_models) - success_count


        responses = [
            FileResponse(filename=file.filename, status=file.status, error=file.error)
            for file in file_models
        ]

        message = (
            f"{success_count} файлов успешно обработано, {failure_count} файлов с ошибками."
            if failure_count else "Все файлы успешно обработаны."
        )

        return UploadResponse(message=message, details=responses)
