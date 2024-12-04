from datetime import datetime
from typing import List, Dict
from bson import ObjectId
from pymongo import MongoClient
from app.models.sheet_model import SheetModel

from app.core.config import db

class DataManagementService:
    def __init__(self, db_client: MongoClient):
        """
        Сервис для подготовки данных, управления коллекциями и сохранения их в MongoDB.
        """
        self.db = db
        self.sheets_collection = self.db.get_collection("Sheets")
        self.data_tables_collection = self.db.get_collection("DataTables")
        self.cities_and_years_collection = self.db.get_collection("CitiesAndYears")
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
                "upload_timestamp": datetime.utcnow(),
                "status": "processed",
            }
            self.sheets_collection.insert_one(sheet_doc)

    def save_data_tables(self, sheet_models: List[SheetModel]):
        """
        Сохраняет сырые данные таблиц в коллекцию `DataTables`.

        :param sheet_models: Список моделей листов
        """
        for sheet in sheet_models:
            for year, city_data in sheet.data.items():
                for city, table in city_data.items():
                    table_doc = {
                        "_id": str(ObjectId()),
                        "sheet_name": sheet.sheet_name,
                        "year": year,
                        "city": city,
                        "headers": table["headers"],
                        "rows": [{"value": row} for row in table["rows"]],
                    }
                    self.data_tables_collection.insert_one(table_doc)

    def save_cities_and_years(self, sheet_models: List[SheetModel]):
        """
        Сохраняет данные на уровне города и года в коллекцию `CitiesAndYears`.

        :param sheet_models: Список моделей листов
        """
        for sheet in sheet_models:
            for year, city_data in sheet.data.items():
                for city in city_data.keys():
                    city_year_doc = {
                        "_id": str(ObjectId()),
                        "sheet_name": sheet.sheet_name,
                        "year": year,
                        "city": city,
                        "timestamp": datetime.utcnow(),
                    }
                    self.cities_and_years_collection.insert_one(city_year_doc)

    def save_logs(self, message: str, level: str = "info"):
        """
        Сохраняет лог-сообщение в коллекцию `Logs`.

        :param message: Текст лог-сообщения
        :param level: Уровень логирования
        """
        log_doc = {
            "_id": str(ObjectId()),
            "timestamp": datetime.utcnow(),
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
            self.save_data_tables(sheet_models)
            self.save_cities_and_years(sheet_models)
            self.save_logs(f"Successfully processed and saved data for file_id: {file_id}")
        except Exception as e:
            self.save_logs(f"Error processing data for file_id: {file_id}. Error: {str(e)}", level="error")
            raise
