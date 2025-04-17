from typing import List
from datetime import datetime

from app.models.sheet_model import SheetModel
from app.models.file_model import FileModel
from app.data_storage.services.log_service import LogService
from app.data_storage.services.flat_data_service import FlatDataService
from app.data_storage.services.file_service import FileService
from app.core.config import mongo_connection
from app.data_storage.repositories.logs_repository import LogsRepository
from app.data_storage.repositories.flat_data_repository import FlatDataRepository
from app.data_storage.repositories.file_repository import FileRepository


class DataSaveService:
    def __init__(self, log_service: LogService, flat_data_service: FlatDataService):
        self.log_service = log_service
        self.flat_data_service = flat_data_service

    async def process_and_save_all(self, sheet_models: List[SheetModel], file_id: str, flat_data: List[dict]):
        """
        Обрабатывает и сохраняет данные во все коллекции
        """
        try:
            await self.flat_data_service.save_flat_data(flat_data)
            file_model = FileModel(
                file_id=file_id,
                name=file_id,  # Используем имя файла как имя
                filename=file_id,  # Добавлено корректное заполнение поля filename
                size=len(flat_data),  # Размер данных
                year=sheet_models[0].year if sheet_models else None,  # Год из SheetModel
                city=sheet_models[0].city if sheet_models else None,  # Город из SheetModel
                status="processed",  # Статус файла
                upload_timestamp=datetime.now()  # Временная метка загрузки
            )
            await self.save_file(file_model)
            await self.log_service.save_log(f"Successfully processed and saved data for file_id: {file_id}")
        except Exception as e:
            await self.log_service.save_log(f"Error processing data for file_id: {file_id}. Error: {str(e)}", level="error")
            raise

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
        file_service = FileService(FileRepository(mongo_connection.get_database().get_collection("Files")))
        await file_service.save_file(file_model)

def create_data_save_service():
    """
    Фабричный метод для создания экземпляра DataSaveService с инициализированными зависимостями
    """
    db = mongo_connection.get_database()

    log_service = LogService(LogsRepository(db.get_collection("Logs")))
    flat_data_service = FlatDataService(FlatDataRepository(db.get_collection("FlatData")))

    return DataSaveService(log_service, flat_data_service)
