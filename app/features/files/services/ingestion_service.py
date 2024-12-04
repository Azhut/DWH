from datetime import datetime
from typing import List, Tuple
from fastapi import HTTPException, UploadFile

from app.api.v1.schemas.files import UploadResponse, FileResponse
from app.features.files.services.file_validation_service import FileValidationService
from app.features.files.services.sheet_extraction import SheetExtractionService
from app.features.files.services.city_and_year_extractor import CityAndYearExtractor
from app.models.file_model import FileModel
from app.models.sheet_model import SheetModel
from app.features.files.sheet_parsers.parsers import get_sheet_parser


class IngestionService:
    def __init__(self):
        self.file_validation_service = FileValidationService()
        self.sheet_extraction_service = SheetExtractionService()
        self.city_and_year_extractor = CityAndYearExtractor()

    async def process_files(self, files: List[UploadFile]) -> List[FileModel]:
        """
        Обрабатывает список файлов и возвращает информацию о каждом.
        """
        file_models = []

        for file in files:
            try:
                # Валидация файла
                self.file_validation_service.validate(file.filename)

                # Извлечение города и года
                city, year = self.city_and_year_extractor.extract(file.filename)

                # Извлечение листов из файла
                sheets = await self.sheet_extraction_service.extract_sheets(file)

                # Парсинг данных с листов
                file_model, sheets_data = await self.parse_sheets(file, sheets, city, year)

                # Сохранение данных
                for sheet_data in sheets_data:
                    await self.save_sheet(sheet_data)

                file_models.append(file_model)
            except HTTPException as e:
                file_models.append(self._create_failed_file_model(file.filename, e.detail, str(e)))
            except Exception as e:
                file_models.append(self._create_failed_file_model(file.filename, "Общая ошибка", str(e)))

        return file_models

    async def parse_sheets(self, file: UploadFile, sheets: List[dict], city: str, year: int) -> Tuple[
        FileModel, List[SheetModel]]:
        sheets_data = []
        file_model = FileModel(
            file_id=file.filename,
            filename=file.filename,
            status="Успешно обработан",
            error="",
            year=year,
            city=city,
            upload_timestamp=datetime.utcnow()
        )

        for sheet in sheets:
            try:
                # Получаем парсер для листа
                parser = get_sheet_parser(sheet["sheet_name"])
                # Парсим данные
                data = parser.parse(sheet["data"])

                # Обработка данных перед сохранением (если нужно)
                processed_data = self.process_data_for_db(data, city, year)  # дополнительная обработка данных

                # Создаем модель для каждого листа
                sheets_data.append(SheetModel(
                    sheet_name=sheet["sheet_name"],
                    data=processed_data,  # уже обработанные данные
                    file_id=file.filename
                ))
            except Exception as e:
                file_model.status = "Ошибка парсинга"
                file_model.error = f"Ошибка на листе {sheet['sheet_name']}: {str(e)}"
                continue

        return file_model, sheets_data

    def process_data_for_db(self, data: dict, city: str, year: int) -> dict:
        """
        Обрабатывает данные, приводя их к формату, пригодному для сохранения в MongoDB.
        """
        # Пример обработки: добавление города и года к данным
        processed_data = {
            "city": city,
            "year": year,
            "data": data
        }
        return processed_data

    def _create_failed_file_model(self, filename: str, status: str, error: str) -> FileModel:
        """
        Создает объект FileModel для файла с ошибкой.
        """
        return FileModel(
            file_id=filename,
            filename=filename,
            status=status,
            error=error,
            year=0,
            city="Неизвестно",
            upload_timestamp=datetime.utcnow()
        )

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
