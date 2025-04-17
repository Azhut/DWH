from typing import List
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
            await self.flat_data_service.save_flat_data(flat_data)
            await self.log_service.save_log(f"Successfully processed and saved data for file_id: {file_id}")
        except Exception as e:
            logger.error(f"Ошибка при сохранении данных: {str(e)}", exc_info=True)
            raise HTTPException(status_code=500, detail="Произошла ошибка при сохранении данных.")

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

        await self.file_service.save_file(file_model)


def create_data_save_service():
    """
    Фабричный метод для создания экземпляра DataSaveService с инициализированными зависимостями
    """
    db = mongo_connection.get_database()

    log_service = LogService(LogsRepository(db.get_collection("Logs")))
    flat_data_service = FlatDataService(FlatDataRepository(db.get_collection("FlatData")))
    file_service = FileService(FileRepository(mongo_connection.get_database().get_collection("Files")))

    return DataSaveService(log_service, flat_data_service, file_service)
