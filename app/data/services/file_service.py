from datetime import datetime
from app.data.repositories.file import FileRepository
from app.models.file_model import FileModel
from app.models.file_status import FileStatus
from app.core.logger import logger

class FileService:
    def __init__(self, file_repo: FileRepository):
        self.file_repo = file_repo

    async def update_or_create(self, file_model: FileModel):
        """
        Сохраняет или обновляет запись Files.
        Преобразует pydantic-модель в dict и сохраняет enum-статус как строку.
        """
        try:
            existing = await self.file_repo.find_by_file_id(file_model.file_id)
            data = file_model.model_dump()
            # Сохраняем статус как строку
            if isinstance(data.get("status"), FileStatus):
                data["status"] = data["status"].value
            # Обновляем updated_at
            data["updated_at"] = datetime.now()
            if existing:
                await self.file_repo.update_one(
                    {"file_id": file_model.file_id},
                    {"$set": data},
                    upsert=False
                )
            else:
                # При вставке убедимся, что поле file_id уникально и status — строка
                await self.file_repo.insert_one(data)
        except Exception as e:
            logger.error(f"Ошибка при сохранении файла {file_model.file_id}: {e}", exc_info=True)
            raise

    async def get_file_by_id(self, file_id: str):
        doc = await self.file_repo.find_by_file_id(file_id)
        return FileModel(**doc) if doc else None
