from datetime import datetime

from app.core.exception_handler import logger
from app.data_storage.repositories.file_repository import FileRepository
from app.models.file_model import FileModel
from typing import List

class FileService:
    def __init__(self, file_repo: FileRepository):
        self.file_repo = file_repo


    async def update_or_create(self, file_model: FileModel):
        try:
            existing = await self.get_file_by_id(file_model.file_id)
            if existing:
                file_model.updated_at = datetime.now()
                await self.file_repo.update_one(
                    {"file_id": file_model.file_id},
                    {"$set": file_model.dict()}
                )
            else:
                await self.file_repo.insert_one(file_model.dict())


            updated = await self.get_file_by_id(file_model.file_id)
            if not updated or updated.status != file_model.status:
                raise ValueError("Ошибка сохранения статуса файла")

        except Exception as e:
            logger.error(f"Ошибка в FileService: {str(e)}")
            raise

    async def get_file_by_id(self, file_id: str) -> FileModel:
        """
        Получить файл из базы данных по его ID.
        """
        file_doc = await self.file_repo.find_by_file_id(file_id)
        return FileModel(**file_doc) if file_doc else None