from fastapi import Depends
from motor.motor_asyncio import AsyncIOMotorClient
from app.core.config import settings
from app.data_storage.repositories.mongo import MongoRepository

def get_mongo_client():
    return AsyncIOMotorClient(settings.DATABASE_URI)

def get_sheet_repository(client: AsyncIOMotorClient = Depends(get_mongo_client)):
    return MongoRepository(client[settings.DATABASE_NAME].Sheets)

def get_flat_data_repository(client: AsyncIOMotorClient = Depends(get_mongo_client)):
    return MongoRepository(client[settings.DATABASE_NAME].FlatData)