from motor.motor_asyncio import AsyncIOMotorClient

from config import config


class DatabaseConnection:
    def __init__(self, db_uri: str, db_name: str):
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

    async def close(self):
        self.client.close()

mongo_connection = DatabaseConnection(config.DATABASE_URI, config.DATABASE_NAME)