from typing import List, Optional
from fastapi import HTTPException, UploadFile
from app.api.v2.schemas.files import UploadResponse, FileResponse
from app.services.file_processor import FileProcessor
from app.services.sheet_processor import SheetProcessor
from app.models.file_model import FileModel
from app.models.file_status import FileStatus
from app.data.services.data_save import DataSaveService
from app.core.logger import logger


class IngestionService:
    def __init__(
            self,
            file_processor: FileProcessor,
            sheet_processor: SheetProcessor,
            data_save_service: DataSaveService
    ):
        self.file_processor = file_processor
        self.sheet_processor = sheet_processor
        self.data_service = data_save_service

    async def process_files(
            self,
            files: List[UploadFile],
            form_id: str,
            skip_sheets: Optional[List[int]] = None,
            spravochno_keywords: Optional[List[str]] = None
    ) -> UploadResponse:
        """
        form_id: обязательный идентификатор формы (строка), передаётся фронтендом
        skip_sheets, spravochno_keywords: опциональные переопределения от клиента
        """
        file_responses = []
        for file in files:
            metadata = None
            try:
                # извлекаем метаданные
                metadata = self.file_processor.validate_and_extract_metadata(file)
                if not metadata.city or not metadata.year:
                    raise HTTPException(status_code=400, detail="Не удалось извлечь город или год из имени файла")

                # создаём FileModel с UUID ДО обработки листов
                file_model = FileModel.create_new(filename=file.filename, year=metadata.year, city=metadata.city)

                # читаем и обрабатываем листы
                await file.seek(0)
                sheet_models, flat_data = await self.sheet_processor.extract_and_process_sheets(
                    file,
                    file_model,
                    form_id=form_id,
                    skip_sheets=skip_sheets,
                    spravochno_keywords=spravochno_keywords
                )

                # обновляем поля файла (size, sheets)
                file_model.size = len(sheet_models) if sheet_models else 0
                file_model.sheets = [m.sheet_name for m in sheet_models] if sheet_models else []

                # сохраняем всё
                await self.data_service.process_and_save_all(file_model, flat_data)

                file_responses.append(FileResponse(filename=file.filename, status=FileStatus.SUCCESS.value, error=""))
            except HTTPException as e:
                # Сохраняем stub с информацией об ошибке
                err_msg = e.detail if isinstance(e.detail, str) else str(e)
                temp_id = file.filename
                stub = FileModel.create_stub(
                    file_id=temp_id,
                    filename=file.filename,
                    error_message=err_msg,
                    year=metadata.year if metadata else None,
                    city=metadata.city if metadata else None
                )
                try:
                    await self.data_service.save_file(stub)
                except Exception:
                    logger.exception("Не удалось сохранить stub запись файла")
                file_responses.append(FileResponse(filename=file.filename, status=FileStatus.FAILED.value, error=err_msg))
            except Exception as e:
                # Непредвиденная ошибка
                logger.exception("Непредвиданная ошибка при обработке файла")
                err_msg = str(e)
                temp_id = file.filename
                stub = FileModel.create_stub(
                    file_id=temp_id,
                    filename=file.filename,
                    error_message=err_msg,
                    year=metadata.year if metadata else None,
                    city=metadata.city if metadata else None
                )
                try:
                    await self.data_service.save_file(stub)
                except Exception:
                    logger.exception("Не удалось сохранить stub запись файла")
                file_responses.append(FileResponse(filename=file.filename, status=FileStatus.FAILED.value, error=err_msg))
        return UploadResponse(
            message=f"{len([r for r in file_responses if r.status==FileStatus.SUCCESS.value])} files processed successfully, {len([r for r in file_responses if r.status==FileStatus.FAILED.value])} failed.",
            details=file_responses
        )