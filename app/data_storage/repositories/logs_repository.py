from app.data_storage.repositories.base_repository import BaseRepository

class LogsRepository(BaseRepository):
    def __init__(self, collection):
        super().__init__(collection)

    async def find_by_level(self, level: str):
        """
        Найти логи по уровню
        """
        return await self.find({"level": level})