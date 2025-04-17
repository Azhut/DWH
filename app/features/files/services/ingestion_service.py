from typing import List

from fastapi import HTTPException, UploadFile

from app.api.v2.schemas.files import UploadResponse, FileResponse
from app.core.logger import logger
from app.data_storage.services.data_save_service import create_data_save_service
from app.features.files.services.FileProcessor import FileProcessor
from app.features.files.services.SheetProcessor import SheetProcessor


class IngestionService:
    def __init__(self):
        self.file_processor = FileProcessor()
        self.sheet_processor = SheetProcessor()
        self.data_service = create_data_save_service()

    async def process_files(self, files: List[UploadFile]) -> UploadResponse:
        file_responses = []
        all_sheet_models = []

        for file in files:
            try:
                city, year = self.file_processor.validate_and_extract_metadata(file)
                sheet_models = await self.sheet_processor.extract_and_process_sheets(file, city, year)

                if sheet_models:
                    flat_data = []  # Assuming flat_data is defined or obtained here
                    await self.data_service.process_and_save_all(sheet_models, file.filename, flat_data)

                all_sheet_models.extend(sheet_models)

                file_responses.append(FileResponse(filename=file.filename, status="Success", error=""))
            except HTTPException as e:
                file_responses.append(FileResponse(filename=file.filename, status="Error", error=str(e)))
            except Exception as e:
                logger.error(f"Error processing file {file.filename}: {str(e)}")
                file_responses.append(
                    FileResponse(filename=file.filename, status="Error", error=f"Unexpected error: {str(e)}"))

        message = self._generate_summary(file_responses)
        return UploadResponse(message=message, details=file_responses)

    @staticmethod
    def _generate_summary(file_responses: List[FileResponse]) -> str:
        success_count = sum(1 for resp in file_responses if resp.status == "Success")
        failure_count = len(file_responses) - success_count
        return f"{success_count} files processed successfully, {failure_count} failed."
