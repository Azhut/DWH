
from app.core.config import settings
from app.core.config import mongo_connection

class MongoIndexManager:
    def __init__(self, db):
        self.db = db

    async def create_flat_data_index(self):
        await self.db.FlatData.create_index([
            ("year", 1),
            ("city", 1),
            ("section", 1),
            ("row", 1),
            ("column", 1)
        ], unique=True)

    async def create_file_indexes(self):
        await self.db.Files.create_index("file_id", unique=True)
        await self.db.Files.create_index("status")

    async def create_all_indexes(self):
        await self.create_flat_data_index()

async def create_indexes():
    """
    Фабричный метод для создания всех индексов через MongoIndexManager.
    """
    db = mongo_connection.get_database()
    index_manager = MongoIndexManager(db)
    await index_manager.create_flat_data_index()