from app.data_storage.repositories.sheets_repository import SheetsRepository
from datetime import datetime
from bson import ObjectId
from typing import List
from app.models.sheet_model import SheetModel

class SheetsService:
    def __init__(self, sheets_repo: SheetsRepository):
        self.sheets_repo = sheets_repo

    async def save_sheets(self, sheet_models: List[SheetModel], file_id: str) -> bool:
        """
        Сохраняет данные листов в коллекцию Sheets

        :param sheet_models: Список моделей листов
        :param file_id: Идентификатор файла
        :return: True, если все листы уникальны, иначе False
        """
        all_unique = True
        for sheet in sheet_models:
            if await self.sheets_repo.find_one({"file_id": file_id, "sheet_name": sheet.sheet_name}):
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
            await self.sheets_repo.insert_one(sheet_doc)
        return all_unique