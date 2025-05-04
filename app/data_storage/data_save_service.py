from typing import List

from app.core.exception_handler import log_and_raise_http
from app.data_storage.repositories import FileRepository
from app.data_storage.services import FileService
from app.models.file_model import FileModel
from app.data_storage.services.log_service import LogService
from app.data_storage.services.flat_data_service import FlatDataService
from app.core.config import mongo_connection
from app.data_storage.repositories.logs_repository import LogsRepository
from app.data_storage.repositories.flat_data_repository import FlatDataRepository
from fastapi import HTTPException
import logging

from app.models.file_status import FileStatus

logger = logging.getLogger(__name__)


class DataSaveService:
    def __init__(self, log_service: LogService, flat_data_service: FlatDataService, file_service: FileService):
        self.log_service = log_service
        self.flat_data_service = flat_data_service
        self.file_service = file_service

    async def process_and_save_all(self, file_id: str, flat_data: List[dict], file_model: FileModel):
        """
        Обрабатывает и сохраняет данные во все коллекции
        """
        try:
            await self.save_file(file_model)
            await self.log_service.save_log(f"Начало обработки файла {file_id}")

            client = self.flat_data_service.flat_data_repo.collection.database.client
            async with await client.start_session() as session:
                async with session.start_transaction():
                    await self.save_flat_data(flat_data)
                    file_model.status = FileStatus.SUCCESS
                    await self.save_file(file_model)
                    await self.log_service.save_log(f"Успешно сохранены данные для {file_id}")
                    logger.info(f"Транзакция сохранения данных для {file_id} завершена")
        except Exception as e:
            await session.abort_transaction()
            await self.rollback(file_id, file_model, str(e))
            log_and_raise_http(500, "Произошла ошибка при сохранении данных", e)

    async def save_flat_data(self, records: List[dict]):
        """
        Делегирует сохранение плоских данных в FlatDataService
        """
        await self.flat_data_service.save_flat_data(records)

    async def save_logs(self, message: str, level: str = "info"):
        """
        Делегирует сохранение логов в LogService
        """
        await self.log_service.save_log(message, level)

    async def save_file(self, file_model: FileModel):
        """
        Сохраняет информацию о файле в коллекцию FileModel
        """

        await self.file_service.update_or_create(file_model)

    async def rollback(self, file_id: str, file_model: FileModel, error: str):
        """Откатывает изменения: удаляет FlatData, сохраняет ошибку в File и Logs."""
        try:
            await self.flat_data_service.delete_by_file_id(file_model)

            file_model.status = FileStatus.FAILED
            file_model.error = error
            await self.save_file(file_model)

            await self.log_service.save_log(f"Откат файла {file_id}: {error}", "error")
        except Exception as e:
            logger.error(f"Ошибка при откате файла {file_id}: {str(e)}")


def create_data_save_service():
    """
    Фабричный метод для создания экземпляра DataSaveService с инициализированными зависимостями
    """
    db = mongo_connection.get_database()

    log_service = LogService(LogsRepository(db.get_collection("Logs")))
    flat_data_service = FlatDataService(FlatDataRepository(db.get_collection("FlatData")))
    file_service = FileService(FileRepository(mongo_connection.get_database().get_collection("Files")))

    return DataSaveService(log_service, flat_data_service, file_service)
