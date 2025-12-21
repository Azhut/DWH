# app/data/repositories/base.py - делаем все методы async
from typing import Any, Dict, List, Optional
from motor.motor_asyncio import AsyncIOMotorCollection


class BaseRepository:
    def __init__(self, collection: AsyncIOMotorCollection):
        self.collection = collection

    async def find(self, query: Dict[str, Any],
                   projection: Dict[str, Any] = None,
                   limit: int = 0,
                   skip: int = 0) -> List[Dict[str, Any]]:
        cursor = self.collection.find(query, projection).skip(skip).limit(limit)
        return [doc async for doc in cursor]

    async def find_one(self, query: Dict[str, Any],
                       projection: Dict[str, Any] = None) -> Optional[Dict[str, Any]]:
        return await self.collection.find_one(query, projection)


    async def insert_one(self, document: Dict[str, Any]) -> Any:
        return await self.collection.insert_one(document)

    async def update_one(self, query: Dict[str, Any], update: Dict[str, Any], upsert: bool = False) -> Any:
        return await self.collection.update_one(query, update, upsert=upsert)

    async def delete_one(self, query: Dict[str, Any]) -> Any:
        return await self.collection.delete_one(query)

    async def delete_many(self, query: Dict[str, Any]) -> Any:
        return await self.collection.delete_many(query)

    async def count_documents(self, query: Dict[str, Any]) -> int:
        return await self.collection.count_documents(query)
