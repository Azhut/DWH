from typing import Any, Dict, List
from motor.motor_asyncio import AsyncIOMotorCollection

class BaseRepository:
    def __init__(self, collection: AsyncIOMotorCollection):
        self.collection = collection

    async def find(self, query: Dict[str, Any], projection: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        cursor = self.collection.find(query, projection)
        return [doc async for doc in cursor]

    async def find_one(self, query: Dict[str, Any], projection: Dict[str, Any] = None) -> Dict[str, Any]:
        return await self.collection.find_one(query, projection)

    async def insert_one(self, document: Dict[str, Any]) -> Any:
        return await self.collection.insert_one(document)

    async def insert_many(self, documents: List[Dict[str, Any]]) -> Any:
        return await self.collection.insert_many(documents)

    async def update_one(self, query: Dict[str, Any], update: Dict[str, Any], upsert: bool = False) -> Any:
        return await self.collection.update_one(query, update, upsert=upsert)

    async def delete_one(self, query: Dict[str, Any]) -> Any:
        return await self.collection.delete_one(query)

    async def count_documents(self, query: Dict[str, Any]) -> int:
        return await self.collection.count_documents(query)