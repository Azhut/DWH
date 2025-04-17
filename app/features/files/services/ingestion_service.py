from typing import List
from fastapi import HTTPException, UploadFile
from datetime import datetime

from app.api.v2.schemas.files import UploadResponse, FileResponse
from app.core.logger import logger
from app.data_storage.data_save_service import create_data_save_service
from app.features.files.services.FileProcessor import FileProcessor
from app.features.files.services.SheetProcessor import SheetProcessor
from app.models.file_model import FileModel


class IngestionService:
    def __init__(self):
        self.file_processor = FileProcessor()
        self.sheet_processor = SheetProcessor()
        self.data_service = create_data_save_service()

    async def process_files(self, files: List[UploadFile]) -> UploadResponse:
        file_responses = []

        for file in files:
            try:
                city, year = self.file_processor.validate_and_extract_metadata(file)
                sheet_models = await self.sheet_processor.extract_and_process_sheets(file, city, year)

                flat_data = []
                file_model = FileModel(
                    file_id=file.filename,
                    filename=file.filename,
                    year=year,
                    city=city,
                    status="success",
                    error=None,
                    upload_timestamp=datetime.now(),
                    size=len(sheet_models) if sheet_models else 0
                )

                await self.data_service.process_and_save_all(file.filename, flat_data, file_model)

                file_responses.append(FileResponse(filename=file.filename, status="Success", error=""))

            except HTTPException as e:
                if "уже был загружен" in str(e):
                    file_responses.append(FileResponse(filename=file.filename, status="Error", error=str(e)))
                else:
                    file_model = FileModel.create_stub(file.filename, file.filename, str(e))
                    await self.data_service.save_file(file_model)
                    file_responses.append(FileResponse(filename=file.filename, status="Error", error=str(e)))

            except Exception as e:
                logger.error(f"Error processing file {file.filename}: {str(e)}")
                file_model = FileModel.create_stub(file.filename, file.filename, f"Unexpected error: {str(e)}")
                await self.data_service.save_file(file_model)
                file_responses.append(
                    FileResponse(filename=file.filename, status="Error", error=f"Unexpected error: {str(e)}")
                )

        message = self._generate_summary(file_responses)
        return UploadResponse(message=message, details=file_responses)

    @staticmethod
    def _generate_summary(file_responses: List[FileResponse]) -> str:
        success_count = sum(1 for resp in file_responses if resp.status == "Success")
        failure_count = len(file_responses) - success_count
        return f"{success_count} files processed successfully, {failure_count} failed."