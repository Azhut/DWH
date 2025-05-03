from typing import List
from fastapi import HTTPException, UploadFile
from datetime import datetime

from app.api.v2.schemas.files import UploadResponse, FileResponse
from app.core.logger import logger
from app.data_storage.data_save_service import create_data_save_service
from app.features.files.services.FileProcessor import FileProcessor
from app.features.files.services.SheetProcessor import SheetProcessor
from app.models.file_model import FileModel
from app.models.file_status import FileStatus


class IngestionService:
    def __init__(self):
        self.file_processor = FileProcessor()
        self.sheet_processor = SheetProcessor()
        self.data_service = create_data_save_service()

    async def process_files(self, files: List[UploadFile]) -> UploadResponse:
        logger.info(f"Начало загрузки {len(files)} файла(ов)")
        file_responses = []

        for file in files:
            logger.info(f"Обработка файла {file.filename}")
            try:
                await file.seek(0)
                metadata = self.file_processor.validate_and_extract_metadata(file)

                await file.seek(0)
                sheet_models, flat_data = await self.sheet_processor.extract_and_process_sheets(file, metadata.city, metadata.year)

                file_model = FileModel(
                    file_id=file.filename,
                    filename=file.filename,
                    year=metadata.year,
                    city=metadata.city,
                    error=None,
                    upload_timestamp=datetime.now(),
                    size=len(sheet_models) if sheet_models else 0
                )

                try:
                    await self.data_service.process_and_save_all(file.filename, flat_data, file_model)
                    file_responses.append(FileResponse(filename=file.filename, status=FileStatus.SUCCESS, error=""))
                except Exception:
                    # await self.data_service.rollback(file_id) TODO # Реализуйте метод отката
                    raise

            except HTTPException as e:
                logger.error(f"HTTP error processing {file.filename}: {e.detail}")
                if "уже был загружен" in str(e):
                    file_responses.append(FileResponse(filename=file.filename, status=FileStatus.DUPLICATE, error=str(e)))
                else:
                    file_model = FileModel.create_stub(file.filename, file.filename, str(e))
                    file_model.status = FileModel.STATUS_FAILED
                    await self.data_service.save_file(file_model)
                    file_responses.append(FileResponse(filename=file.filename, status=FileStatus.FAILED, error=str(e)))

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
        success_count = sum(1 for resp in file_responses if resp.status == FileStatus.SUCCESS)
        failure_count = len(file_responses) - success_count
        return f"{success_count} files processed successfully, {failure_count} failed."