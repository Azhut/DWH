from typing import List

from app.models.sheet_model import SheetModel
from app.data_storage.services.log_service import LogService
from app.data_storage.services.sheets_service import SheetsService
from app.data_storage.services.flat_data_service import FlatDataService
from app.core.config import mongo_connection
from app.data_storage.repositories.logs_repository import LogsRepository
from app.data_storage.repositories.sheets_repository import SheetsRepository
from app.data_storage.repositories.flat_data_repository import FlatDataRepository


class DataSaveService:
    def __init__(self, log_service: LogService, sheets_service: SheetsService, flat_data_service: FlatDataService):
        self.log_service = log_service
        self.sheets_service = sheets_service
        self.flat_data_service = flat_data_service

    async def process_and_save_all(self, sheet_models: List[SheetModel], file_id: str, flat_data: List[dict]):
        """
        Обрабатывает и сохраняет данные во все коллекции
        """
        try:
            all_unique = await self.sheets_service.save_sheets(sheet_models, file_id)
            await self.flat_data_service.save_flat_data(flat_data)

            if all_unique:
                await self.log_service.save_log(f"Successfully processed and saved data for file_id: {file_id}")
            else:
                await self.log_service.save_log(f"Document with file_id: {file_id} already exists.")
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

    async def save_sheets(self, sheet_models: List[SheetModel], file_id: str):
        """
        Делегирует сохранение листов в SheetsService
        """
        return await self.sheets_service.save_sheets(sheet_models, file_id)


def create_data_save_service():
    """
    Фабричный метод для создания экземпляра DataSaveService с инициализированными зависимостями
    """
    db = mongo_connection.get_database()

    log_service = LogService(LogsRepository(db.get_collection("Logs")))
    sheets_service = SheetsService(SheetsRepository(db.get_collection("Sheets")))
    flat_data_service = FlatDataService(FlatDataRepository(db.get_collection("FlatData")))

    return DataSaveService(log_service, sheets_service, flat_data_service)
