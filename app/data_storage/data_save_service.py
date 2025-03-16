from datetime import datetime
from typing import List
from bson import ObjectId
from pymongo import MongoClient, UpdateOne

from app.models.sheet_model import SheetModel
from app.core.config import settings
from core.logger import logger


class DataSaveService:
    def __init__(self):
        self.client = MongoClient(settings.DATABASE_URI)
        self.db = self.client[settings.DATABASE_NAME]

        self.sheets_collection = self.db.get_collection("Sheets")
        self.logs_collection = self.db.get_collection("Logs")
        self.flat_data_collection = self.db.get_collection("FlatData")
        self.is_unique = True
        self._ensure_flat_data_unique_index()

    def _ensure_flat_data_unique_index(self):
        """
        Создает уникальный составной индекс для коллекции FlatData
        """
        self.flat_data_collection.create_index(
            [("year", 1), ("city", 1), ("section", 1), ("row", 1), ("column", 1), ("value", 1)],
            unique=True
        )

    def save_sheets(self, sheet_models: List[SheetModel], file_id: str):
        """
        Сохраняет данные листов в коллекцию Sheets

        :param sheet_models: Список моделей листов
        :param file_id: Идентификатор файла
        """
        all_unique = True
        for sheet in sheet_models:
            if self.sheets_collection.find_one({"file_id": file_id, "sheet_name": sheet.sheet_name}):
                all_unique = False
                continue

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
        self.is_unique = all_unique

    def save_logs(self, message: str, level: str = "info"):
        """
        Сохраняет лог в коллекцию Logs

        :param message: Текст лога
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
        Обрабатывает и сохраняет данные во все коллекции
        """
        try:
            self.save_sheets(sheet_models, file_id)
            if self.is_unique:
                self.save_logs(f"Successfully processed and saved data for file_id: {file_id}")
            else:
                self.save_logs(f"Document with file_id: {file_id} already exists.")
        except Exception as e:
            self.save_logs(f"Error processing data for file_id: {file_id}. Error: {str(e)}", level="error")
            raise

    def save_flat_data(self, records: List[dict]):
        """
        Сохраняет уникальные записи в коллекцию FlatData

        :param records: Список записей
        """
        if not records:
            return
        try:
            operations = []
            for record in records:
                filter_criteria = {
                    "year": record.get("year"),
                    "city": record.get("city"),
                    "section": record.get("section"),
                    "row": record.get("row"),
                    "column": record.get("column"),
                    "value": record.get("value")
                }
                operations.append(
                    UpdateOne(filter_criteria, {"$setOnInsert": record}, upsert=True)
                )
            if operations:
                result = self.flat_data_collection.bulk_write(operations, ordered=False)

                inserted_count = (
                    result.upserted_count if hasattr(result, "upserted_count")
                    else len(result.upserted_ids)
                )
                logger.info(f"Inserted {inserted_count} unique flat records out of {len(records)}")
        except Exception as e:
            logger.error(f"Failed to save flat data: {str(e)}")
            raise
