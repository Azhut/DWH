"""Репозиторий агрегата Log: работа с коллекцией Logs."""
from app.domain.base import BaseRepository


class LogRepository(BaseRepository):
    async def find_by_level(self, level: str):
        return await self.find({"level": level})
