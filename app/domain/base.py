"""Базовый репозиторий для коллекций MongoDB в доменном слое."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from motor.motor_asyncio import AsyncIOMotorCollection


class BaseRepository:
    """Обёртка над AsyncIOMotorCollection с единообразной передачей сессии транзакции."""

    def __init__(self, collection: AsyncIOMotorCollection) -> None:
        self.collection = collection

    async def find(
        self,
        query: Dict[str, Any],
        projection: Optional[Dict[str, Any]] = None,
        limit: int = 0,
        skip: int = 0,
        session: Any = None,
    ) -> List[Dict[str, Any]]:
        cursor = self.collection.find(query, projection, session=session).skip(skip).limit(limit)
        return [doc async for doc in cursor]

    async def find_one(
        self,
        query: Dict[str, Any],
        projection: Optional[Dict[str, Any]] = None,
        session: Any = None,
    ) -> Optional[Dict[str, Any]]:
        return await self.collection.find_one(query, projection, session=session)

    async def insert_one(self, document: Dict[str, Any], session: Any = None) -> Any:
        return await self.collection.insert_one(document, session=session)

    async def update_one(
        self,
        query: Dict[str, Any],
        update: Dict[str, Any],
        upsert: bool = False,
        session: Any = None,
    ) -> Any:
        return await self.collection.update_one(query, update, upsert=upsert, session=session)

    async def delete_one(self, query: Dict[str, Any], session: Any = None) -> Any:
        return await self.collection.delete_one(query, session=session)

    async def delete_many(self, query: Dict[str, Any], session: Any = None) -> Any:
        return await self.collection.delete_many(query, session=session)

    async def count_documents(self, query: Dict[str, Any], session: Any = None) -> int:
        return await self.collection.count_documents(query, session=session)
