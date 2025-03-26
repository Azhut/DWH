from motor.motor_asyncio import AsyncIOMotorClient

from app.core.config import settings


async def create_indexes():
    client = AsyncIOMotorClient(settings.DATABASE_URI)
    db = client[settings.DATABASE_NAME]

    await db.FlatData.create_index([
        ("year", 1),
        ("city", 1),
        ("section", 1),
        ("row", 1),
        ("column", 1)
    ])
    await db["Sheets"].create_index([("sheet_name", 1)])