from typing import List, Dict, Any

from motor.motor_asyncio import AsyncIOMotorCollection
from app.data_storage.repositories.base import BaseRepository

class MongoRepository(BaseRepository):
    def __init__(self, collection: AsyncIOMotorCollection):
        self.collection = collection

    async def get_all(self) -> List[Dict]:
        return await self.collection.find().to_list(None)

    async def get_by_filter(self, filters: Dict) -> List[Dict]:
        return await self.collection.find(filters).to_list(None)

    async def create(self, data: Dict) -> Any:
        return await self.collection.insert_one(data)

    async def update(self, filters: Dict, data: Dict) -> int:
        result = await self.collection.update_many(filters, {"$set": data})
        return result.modified_count

    async def delete(self, filters: Dict) -> int:
        result = await self.collection.delete_many(filters)
        return result.deleted_count