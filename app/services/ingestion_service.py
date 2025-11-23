from typing import List
from fastapi import HTTPException, UploadFile
from app.api.v2.schemas.files import UploadResponse, FileResponse
from app.services.file_processor import FileProcessor
from app.services.sheet_processor import SheetProcessor
from app.models.file_model import FileModel
from app.models.file_status import FileStatus
from app.data.services.data_save import create_data_save_service

class IngestionService:
    def __init__(self):
        self.file_processor = FileProcessor()
        self.sheet_processor = SheetProcessor()
        self.data_service = create_data_save_service()

    async def process_files(self, files: List[UploadFile]) -> UploadResponse:
        file_responses = []
        for file in files:
            metadata = None
            try:
                await file.seek(0)
                metadata = self.file_processor.validate_and_extract_metadata(file)
                if not metadata.city or not metadata.year:
                    raise HTTPException(400, "Не удалось извлечь город или год из имени файла")

                # создаём FileModel с UUID ДО обработки листов
                file_model = FileModel.create_new(filename=file.filename, year=metadata.year, city=metadata.city)

                await file.seek(0)
                # Передаём весь file_model — sheet_processor поставит file_id в flat записи
                sheet_models, flat_data = await self.sheet_processor.extract_and_process_sheets(
                    file, file_model
                )

                # обновляем размер/список листов
                file_model.size = len(sheet_models) if sheet_models else 0
                file_model.sheets = [m.sheet_name for m in sheet_models] if sheet_models else []

                try:
                    await self.data_service.process_and_save_all(file_model, flat_data)
                    file_responses.append(FileResponse(filename=file.filename, status=FileStatus.SUCCESS, error=""))
                except Exception as e:
                    # DataSaveService уже логирует и делает откат
                    file_responses.append(FileResponse(filename=file.filename, status=FileStatus.FAILED, error=str(e)))
            except HTTPException as e:
                # Создаём stub с UUID, если metadata не извлечён — возьмём временный UUID
                temp_id = getattr(metadata, "filename", None) or file.filename
                stub = FileModel.create_stub(
                    file_id=str(temp_id),
                    filename=file.filename,
                    error_message=str(e.detail),
                    year=metadata.year if metadata else None,
                    city=metadata.city if metadata else None
                )
                # Сохраняем запись о неудачной загрузке
                try:
                    await self.data_service.save_file(stub)
                except Exception:
                    pass
                file_responses.append(FileResponse(filename=file.filename, status=FileStatus.FAILED, error=str(e.detail)))
            except Exception as e:
                # Непредвиденная ошибка
                tmp_id = file.filename
                file_model = FileModel.create_stub(
                    file_id=tmp_id,
                    filename=file.filename,
                    error_message=f"Неожиданная ошибка: {str(e)}",
                    year=metadata.year if metadata else None,
                    city=metadata.city if metadata else None
                )
                try:
                    await self.data_service.save_file(file_model)
                except Exception:
                    pass
                file_responses.append(FileResponse(filename=file.filename, status=FileStatus.FAILED, error=str(e)))
        message = self._generate_summary(file_responses)
        return UploadResponse(message=message, details=file_responses)

    @staticmethod
    def _generate_summary(file_responses: List[FileResponse]) -> str:
        success_count = sum(1 for resp in file_responses if resp.status == FileStatus.SUCCESS)
        failure_count = len(file_responses) - success_count
        return f"{success_count} files processed successfully, {failure_count} failed."
