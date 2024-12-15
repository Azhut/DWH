from datetime import datetime
from typing import List
from bson import ObjectId
from app.models.sheet_model import SheetModel
from motor.motor_asyncio import AsyncIOMotorClient
from app.core.config import settings

class DataManagementService:
    def __init__(self):
        self.client = AsyncIOMotorClient(settings.DATABASE_URI)
        self.db = self.client[settings.DATABASE_NAME]

        self.sheets_collection = self.db.get_collection("Sheets")

        self.logs_collection = self.db.get_collection("Logs")

    def save_sheets(self, sheet_models: List[SheetModel], file_id: str):
        """
        Обрабатывает и сохраняет данные о листах в коллекцию `Sheets`.

        :param sheet_models: Список моделей листов
        :param file_id: Идентификатор файла
        """
        for sheet in sheet_models:
            sheet_doc = {
                "_id": str(ObjectId()),
                "file_id": file_id,
                "sheet_name": sheet.sheet_name,
                "sheet_fullname": sheet.sheet_fullname,
                "upload_timestamp": datetime.now(),
                "status": "processed",
                "year": sheet.year,
                "city": sheet.city,
                "headers": sheet.headers,
                "data": sheet.data,  # Сохраняем данные как есть, с учетом преобразований
            }
            self.sheets_collection.insert_one(sheet_doc)



    def save_logs(self, message: str, level: str = "info"):
        """
        Сохраняет лог-сообщение в коллекцию `Logs`.

        :param message: Текст лог-сообщения
        :param level: Уровень логирования
        """
        log_doc = {
            "_id": str(ObjectId()),
            "timestamp": datetime.now(),
            "level": level,
            "message": message,
        }
        self.logs_collection.insert_one(log_doc)

    def process_and_save_all(self, sheet_models: List[SheetModel], file_id: str):
        """
        Универсальный метод, который обрабатывает и сохраняет данные во все коллекции.
        """
        try:
            self.save_sheets(sheet_models, file_id)

            self.save_logs(f"Successfully processed and saved data for file_id: {file_id}")
        except Exception as e:
            self.save_logs(f"Error processing data for file_id: {file_id}. Error: {str(e)}", level="error")
            raise
