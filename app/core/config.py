from pydantic_settings import BaseSettings
from motor.motor_asyncio import AsyncIOMotorClient


class Settings(BaseSettings):
    # DATABASE_URI: str = "mongodb://mongo:27017"
    DATABASE_URI: str = "localhost:27017"
    DATABASE_NAME: str = "sport_data"


settings = Settings()


class DatabaseConnection:
    def __init__(self, db_uri: str, db_name: str):
        self.client = AsyncIOMotorClient(db_uri)
        self.db = self.client[db_name]

    def get_database(self):
        return self.db


# Создаем глобальный объект подключения к базе данных
mongo_connection = DatabaseConnection(settings.DATABASE_URI, settings.DATABASE_NAME)
