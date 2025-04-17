# Moved index creation logic to a separate module
from motor.motor_asyncio import AsyncIOMotorClient
from app.core.config import settings

class MongoIndexManager:
    def __init__(self):
        self.client = AsyncIOMotorClient(settings.DATABASE_URI)
        self.db = self.client[settings.DATABASE_NAME]

    async def create_flat_data_index(self):
        await self.db.FlatData.create_index([
            ("year", 1),
            ("city", 1),
            ("section", 1),
            ("row", 1),
            ("column", 1)
        ], unique=True)

    async def create_sheets_index(self):
        await self.db["Sheets"].create_index([("sheet_name", 1)])