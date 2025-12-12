from app.data.repositories.base import BaseRepository

class LogsRepository(BaseRepository):
    async def find_by_level(self, level: str):
        return await self.find({"level": level})
