"""Репозиторий агрегата Log: работа с коллекцией Logs."""
from datetime import datetime
from typing import Any, Dict, List, Optional

from app.domain.base import BaseRepository


class LogRepository(BaseRepository):
    async def find_by_level(self, level: str) -> List[Dict[str, Any]]:
        return await self.find({"level": level})

    async def find_logs(
        self,
        *,
        limit: int = 1000,
        scenario: Optional[str] = None,
        level: Optional[str] = None,
        from_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Гибкий поиск логов для выгрузки."""
        query: Dict[str, Any] = {}
        if scenario:
            query["scenario"] = scenario
        if level:
            query["level"] = level
        if from_id:
            # _id в коллекции хранится как строка ObjectId, поэтому фильтруем по строковому сравнению
            query["_id"] = {"$gt": from_id}

        cursor = (
            self.collection.find(query)
            .sort("timestamp", -1)
            .limit(int(limit))
        )
        return await cursor.to_list(length=limit)

    async def cleanup_old_logs(self, max_logs: int) -> None:
        """Удаляет самые старые логи, оставляя не более max_logs последних."""
        if max_logs <= 0:
            return

        count = await self.collection.count_documents({})
        if count <= max_logs:
            return

        skip_count = max_logs
        oldest_to_keep = await (
            self.collection.find({}, {"timestamp": 1})
            .sort("timestamp", -1)
            .skip(skip_count)
            .limit(1)
        ).to_list(length=1)

        if not oldest_to_keep:
            return

        oldest_timestamp: datetime = oldest_to_keep[0]["timestamp"]
        await self.collection.delete_many({"timestamp": {"$lt": oldest_timestamp}})
