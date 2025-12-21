# app/services/ingestion_service.py
from typing import List
from fastapi import HTTPException, UploadFile
from app.api.v2.schemas.files import UploadResponse, FileResponse
from app.services.file_processor import FileProcessor
from app.services.sheet_processor import SheetProcessor, is_file_unique
from app.models.file_model import FileModel
from app.models.file_status import FileStatus
from app.data.services.data_save import DataSaveService
from app.core.logger import logger
from app.core.database import mongo_connection

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

    async def process_files(self, files: List[UploadFile], form_id: str) -> UploadResponse:
        """
        Обрабатываем список файлов. form_id обязателен — если его нет, вызывающий код должен был это проверить.
        """
        file_responses: List[FileResponse] = []
        db = mongo_connection.get_database()

        for file in files:
            metadata = None
            try:
                # 1) извлекаем метаданные (город/год)
                metadata = self.file_processor.validate_and_extract_metadata(file)
                if not metadata.city or not metadata.year:
                    raise HTTPException(status_code=400, detail="Не удалось извлечь город или год из имени файла")

                # 2) проверка уникальности (по имени файла)
                unique = await is_file_unique(file.filename)
                if not unique:
                    raise HTTPException(status_code=400, detail=f"Файл '{file.filename}' уже был загружен.")

                # 3) создаём FileModel с UUID ДО обработки листов и сохраним form_id
                file_model = FileModel.create_new(
                    filename=file.filename,
                    year=metadata.year,
                    city=metadata.city,
                    form_id=form_id
                )
                # присвоим form_id до сохранения, чтобы при ошибках было понятно к какой форме привязан stub
                file_model.form_id = form_id

                # 4) получаем форму (requisites) из БД — на случай неверного form_id это ошибка
                form_doc = await db.Forms.find_one({"id": form_id})
                if not form_doc:
                    raise HTTPException(status_code=400, detail=f"Форма '{form_id}' не найдена")

                skip_sheets = form_doc.get("requisites", {}).get("skip_sheets", []) or []

                # 5) читаем и обрабатываем листы
                await file.seek(0)
                sheet_models, flat_data = await self.sheet_processor.extract_and_process_sheets(
                    file=file,
                    file_model=file_model,
                    skip_sheets=skip_sheets
                )

                # 6) обновляем поля файла (size и список sheet_name)
                file_model.size = len(sheet_models) if sheet_models else 0
                file_model.sheets = [m.sheet_name for m in sheet_models] if sheet_models else []

                # 7) добавляем управляющие поля ко всем flat-записям
                for rec in flat_data:
                    rec["file_id"] = file_model.file_id
                    rec["form"] = form_id
                    # city/ year/section должны уже быть установлены; приведение к единообразному регистру
                    if "city" in rec and isinstance(rec["city"], str):
                        rec["city"] = rec["city"].upper()

                # 8) Сохраняем всё (DataSaveService сделает откат в случае ошибки)
                await self.data_service.process_and_save_all(file_model, flat_data)

                file_responses.append(FileResponse(filename=file.filename, status=FileStatus.SUCCESS.value, error=""))
            except HTTPException as e:
                # Сохраняем stub с информацией об ошибке
                err_msg = e.detail if isinstance(e.detail, str) else str(e)
                temp_id = file.filename
                stub = FileModel.create_stub(
                    file_id=temp_id,
                    filename=file.filename,
                    form_id=form_id,
                    error_message=err_msg,
                    year=metadata.year if metadata else None,
                    city=metadata.city if metadata else None
                )

                # сохраняем form_id в stub если есть
                try:
                    if form_id:
                        stub.form_id = form_id
                except Exception:
                    pass
                try:
                    await self.data_service.save_file(stub)
                except Exception:
                    logger.exception("Не удалось сохранить stub запись файла")
                file_responses.append(FileResponse(filename=file.filename, status=FileStatus.FAILED.value, error=err_msg))
            except Exception as e:
                # Непредвиденная ошибка — логируем и сохраняем stub
                logger.exception("Непредвиденная ошибка при обработке файла: %s", e)
                err_msg = str(e)
                temp_id = file.filename
                stub = FileModel.create_stub(
                    file_id=temp_id,
                    filename=file.filename,
                    form_id=form_id,
                    error_message=err_msg,
                    year=metadata.year if metadata else None,
                    city=metadata.city if metadata else None
                )

                try:
                    if form_id:
                        stub.form_id = form_id
                except Exception:
                    pass
                try:
                    await self.data_service.save_file(stub)
                except Exception:
                    logger.exception("Не удалось сохранить stub запись файла")
                file_responses.append(FileResponse(filename=file.filename, status=FileStatus.FAILED.value, error=err_msg))

        # формируем итоговую структуру
        success_count = sum(1 for resp in file_responses if resp.status == FileStatus.SUCCESS.value)
        failure_count = len(file_responses) - success_count
        return UploadResponse(message=f"{success_count} files processed successfully, {failure_count} failed.", details=file_responses)
