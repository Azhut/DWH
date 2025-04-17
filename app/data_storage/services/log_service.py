from app.data_storage.repositories.logs_repository import LogsRepository
from datetime import datetime
from bson import ObjectId

class LogService:
    def __init__(self, logs_repo: LogsRepository):
        self.logs_repo = logs_repo

    async def save_log(self, message: str, level: str = "info"):
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
        await self.logs_repo.insert_one(log_doc)