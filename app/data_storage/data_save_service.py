from datetime import datetime
from typing import List
from bson import ObjectId
from pymongo import MongoClient

from app.models.sheet_model import SheetModel
from app.core.config import settings

class DataSaveService:
    def __init__(self):
        self.client = MongoClient(settings.DATABASE_URI)
        self.db = self.client[settings.DATABASE_NAME]

        self.sheets_collection = self.db.get_collection("Sheets")

        self.logs_collection = self.db.get_collection("Logs")
        self.isUnique = True
        self.flat_data_collection = self.db.get_collection("FlatData")



    def save_sheets(self, sheet_models: List[SheetModel], file_id: str):
        """
        Обрабатывает и сохраняет данные о листах в коллекцию `Sheets`.

        :param sheet_models: Список моделей листов
        :param file_id: Идентификатор файла
        """
        for sheet in sheet_models:
            self.isUnique = True
            existing_doc = self.sheets_collection.find_one({
                "file_id": file_id,
                "sheet_name": sheet.sheet_name
            })

            # Выводим результат для отладки
            if existing_doc:
                self.isUnique=False
                continue
            else:

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
                    "data": sheet.data,
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

            if self.isUnique:
                self.save_logs(f"Successfully processed and saved data for file_id: {file_id}")
            else:
                self.save_logs(f"Document with file_id: {file_id}  already exists.")
        except Exception as e:
            self.save_logs(f"Error processing data for file_id: {file_id}. Error: {str(e)}", level="error")
            raise

    def save_flat_data(self, records: List[dict]):
        if records:
            print(f"[DEBUG] Saving {len(records)} flat records")  # Логируем
            self.flat_data_collection.insert_many(records)
        else:
            print("[WARNING] No flat data to save")  # Логируем