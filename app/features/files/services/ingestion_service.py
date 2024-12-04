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
        all_sheets_data = []

        for file in files:
            try:
                self.file_validator.validate(file.filename)
                city, year = self.city_year_extractor.extract(file.filename)

                sheets = await self.sheet_extractor.extract(file)

                file_model, sheet_data = await self._process_sheets(file, sheets, city, year)
                all_sheets_data.extend(sheet_data)
                file_responses.append(FileResponse(filename=file.filename, status="Success", error=""))
            except HTTPException as e:
                file_responses.append(FileResponse(filename=file.filename, status="Error", error=str(e)))
            except Exception as e:
                logger.error(f"Error processing file {file.filename}: {str(e)}")
                file_responses.append(
                    FileResponse(filename=file.filename, status="Error", error=f"Unexpected error: {str(e)}"))

        # Преобразование all_sheets_data в SheetModel
        sheet_models = [SheetModel(**data) for data in all_sheets_data]

        if sheet_models:
            await self.data_service.save_sheets(sheet_models, 'fdfs')

        success_count = sum(1 for resp in file_responses if resp.status == "Success")
        failure_count = len(file_responses) - success_count
        message = f"{success_count} files processed successfully, {failure_count} failed."

        return UploadResponse(message=message, details=file_responses)

    async def _process_sheets(self, file, sheets, city, year) -> Tuple[FileModel, List[dict]]:
        sheet_data_list = []

        for sheet in sheets:
            try:
                parser = get_sheet_parser(sheet["sheet_name"])
                data = parser.parse(sheet["data"])
                sheet_data_list.append(self._prepare_data_for_db(data, sheet["sheet_name"], city, year))
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"Error processing sheet {sheet['sheet_name']}: {str(e)}")

        file_model = FileModel(
            file_id=file.filename,
            filename=file.filename,
            status="Processed",
            year=year,
            city=city,
            upload_timestamp=datetime.now(),
        )
        return file_model, sheet_data_list

    @staticmethod
    def _prepare_data_for_db(data, sheet_name, city, year):
        # Вместо словаря возвращаем SheetModel
        return SheetModel(
            sheet_name=sheet_name,
            city=city,
            year=year,
            data=data
        )
