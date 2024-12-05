from datetime import datetime
from typing import List, Tuple
from fastapi import HTTPException, UploadFile
from app.api.v1.schemas.files import UploadResponse, FileResponse
from app.core.logger import logger
from app.features.files.services.file_validation_service import FileValidationService
from app.features.files.services.sheet_extraction_service import SheetExtractionService
from app.features.files.services.city_and_year_extractor import CityAndYearExtractor
from app.models.file_model import FileModel
from app.features.sheet_parsers.parsers import get_sheet_parser
from app.data_storage.data_management_service import DataManagementService
from app.models.sheet_model import SheetModel

class IngestionService:
    def __init__(self):
        self.file_validator = FileValidationService()
        self.sheet_extractor = SheetExtractionService()
        self.city_year_extractor = CityAndYearExtractor()
        self.data_service = DataManagementService()

    async def process_files(self, files: List[UploadFile]) -> UploadResponse:
        file_responses = []
        sheet_models = []

        for file in files:
            try:
                # Проверка файла
                self.file_validator.validate(file.filename)
                city, year = self.city_year_extractor.extract(file.filename)

                # Извлечение листов
                sheets = await self.sheet_extractor.extract(file)

                # Обработка листов
                processed_sheets = await self._process_sheets(file.filename, sheets, city, year)
                sheet_models.extend(processed_sheets)

                # Успешная обработка файла
                file_responses.append(FileResponse(filename=file.filename, status="Success", error=""))
            except HTTPException as e:
                file_responses.append(FileResponse(filename=file.filename, status="Error", error=str(e)))
            except Exception as e:
                logger.error(f"Error processing file {file.filename}: {str(e)}")
                file_responses.append(
                    FileResponse(filename=file.filename, status="Error", error=f"Unexpected error: {str(e)}"))

        # Сохранение всех обработанных данных
        if sheet_models:
            self.data_service.process_and_save_all(sheet_models, "unique_file_id")

        # Формирование ответа
        success_count = sum(1 for resp in file_responses if resp.status == "Success")
        failure_count = len(file_responses) - success_count
        message = f"{success_count} files processed successfully, {failure_count} failed."

        return UploadResponse(message=message, details=file_responses)

    async def _process_sheets(self, file_id, sheets, city, year) -> List[SheetModel]:
        sheet_models = []

        for sheet in sheets:
            try:
                # Получение подходящего парсера
                parser = get_sheet_parser(sheet["sheet_name"])
                parsed_data = await parser.parse(sheet["data"])

                # Формирование модели листа
                sheet_model = SheetModel(
                    file_id=file_id,
                    sheet_name=sheet["sheet_name"],
                    data={
                        year: {
                            city: {
                                "headers": parsed_data.get("headers"),
                                "rows": parsed_data.get("rows")
                            }
                        }
                    }
                )
                sheet_models.append(sheet_model)
            except Exception as e:
                logger.error(f"Error parsing sheet {sheet['sheet_name']}: {str(e)}")
                raise HTTPException(status_code=400, detail=f"Error processing sheet {sheet['sheet_name']}")

        return sheet_models
