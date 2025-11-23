from pathlib import Path

from pydantic_settings import BaseSettings
from motor.motor_asyncio import AsyncIOMotorClient


class Settings(BaseSettings):
    # DATABASE_URI: str = "mongodb://mongo:27017"
    DATABASE_URI: str = "mongodb://localhost:27017"
    DATABASE_NAME: str = "sport_data"
    MANUAL_MAP_PATH: Path = Path("app/utils/manual_map.json")


settings = Settings()


class DatabaseConnection:
    def __init__(self, db_uri: str, db_name: str):
        # оставить retryWrites=False как в проекте, но схема обязательна
        self.client = AsyncIOMotorClient(
            db_uri,
            maxPoolSize=100,
            minPoolSize=10,
            socketTimeoutMS=60000,
            connectTimeoutMS=10000,
            serverSelectionTimeoutMS=10000,
            waitQueueTimeoutMS=2000,
            retryWrites=False
        )
        self.db = self.client[db_name]

    def get_database(self):
        return self.db


mongo_connection = DatabaseConnection(settings.DATABASE_URI, settings.DATABASE_NAME)
