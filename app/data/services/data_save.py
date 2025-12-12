from typing import List
from app.core.exceptions import log_and_raise_http
from app.data.services.log_service import LogService
from app.data.services.flat_data_service import FlatDataService
from app.data.services.file_service import FileService
from app.models.file_status import FileStatus
from app.models.file_model import FileModel
import logging

logger = logging.getLogger(__name__)


class DataSaveService:
    def __init__(
            self,
            log_service: LogService,
            flat_data_service: FlatDataService,
            file_service: FileService
    ):
        self.log_service = log_service
        self.flat_data_service = flat_data_service
        self.file_service = file_service

    async def process_and_save_all(self, file_model: FileModel, flat_data: List[dict]):
        """
        Сохраняет запись файла и связанные flat_data.
        Не использует Mongo транзакции (standalone). Делает ручной откат в случае ошибки
        """
        try:
            # 1) Сохраняем файл (processing)
            await self.file_service.update_or_create(file_model)
            await self.log_service.save_log(f"Начало обработки файла {file_model.file_id}")

            # 2) Сохраняем flat_data и получаем число вставленных документов
            inserted_total = 0
            if flat_data:
                inserted_total = await self.flat_data_service.save_flat_data(flat_data)
            else:
                await self.log_service.save_log(f"FlatData пуст для {file_model.file_id}", level="warning")

            # 3) Обновляем статус и поля файла
            file_model.status = FileStatus.SUCCESS
            # если file_model поддерживает size — обновим
            try:
                file_model.size = inserted_total if inserted_total else getattr(file_model, "size", 0)
            except Exception:
                pass

            await self.file_service.update_or_create(file_model)
            await self.log_service.save_log(
                f"Успешно сохранены данные для {file_model.file_id}; flat_inserted={inserted_total}")
            logger.info("DataSaveService: сохранение завершено для %s; flat_inserted=%s", file_model.file_id,
                        inserted_total)

        except Exception as e:
            # Откат: удаляем flat_data и пометим файл как FAILED
            try:
                await self.rollback(file_model, str(e))
            except Exception as inner:
                await self.log_service.save_log(f"Ошибка при попытке отката для {file_model.file_id}: {inner}",
                                                level="error")
            log_and_raise_http(500, "Ошибка при сохранении данных", e)

    async def rollback(self, file_model: FileModel, error: str):
        try:
            # удаляем по file_id
            await self.flat_data_service.delete_by_file_id(file_model.file_id)
        except Exception as e:
            await self.log_service.save_log(f"Не удалось удалить FlatData при откате для {file_model.file_id}: {e}",
                                            level="error")

        file_model.status = FileStatus.FAILED
        file_model.error = error
        try:
            await self.file_service.update_or_create(file_model)
        except Exception as e:
            await self.log_service.save_log(
                f"Не удалось обновить запись Files при откате для {file_model.file_id}: {e}", level="error")

        await self.log_service.save_log(f"Откат файла {file_model.file_id}: {error}", level="error")

    async def save_file(self, file_model: FileModel):
        """
        Сохраняет одиночную запись файла (используется для stub'ов при ошибках загрузки)
        """
        try:
            await self.file_service.update_or_create(file_model)
            await self.log_service.save_log(f"Сохранён stub файл {file_model.file_id}", level="warning")
        except Exception as e:
            logger.exception("Не удалось сохранить stub файл %s: %s", file_model.file_id, e)
            raise
