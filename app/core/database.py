from motor.motor_asyncio import AsyncIOMotorClient
from typing import Optional
from config import config

class DatabaseConnection:
    def __init__(self, db_uri: str, db_name: str):

        self._uri = db_uri
        self._db_name = db_name
        self._client: Optional[AsyncIOMotorClient] = None
        self.db = None


        self._init_client()

    def _init_client(self):
        try:
            self._client = AsyncIOMotorClient(
                self._uri,
                maxPoolSize=100,
                minPoolSize=10,
                socketTimeoutMS=60000,
                connectTimeoutMS=10000,
                serverSelectionTimeoutMS=10000,
                waitQueueTimeoutMS=2000,
                retryWrites=False
            )
            self.db = self._client[self._db_name]
        except Exception:
            self._client = None
            self.db = None

    def get_database(self):
        if self.db is None:
            self._init_client()
            if self.db is None:
                raise RuntimeError("Не удалось установить соединение с MongoDB")
        return self.db

    async def close(self):
        if self._client:
            self._client.close()
            self._client = None
            self.db = None

mongo_connection = DatabaseConnection(config.MONGO_URI, config.DATABASE_NAME)
