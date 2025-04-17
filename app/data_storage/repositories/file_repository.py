from app.data_storage.repositories.base_repository import BaseRepository

class FileRepository(BaseRepository):
    def __init__(self, collection):
        super().__init__(collection)

    async def find_by_file_id(self, file_id: str):
        """
        Найти файл по айди
        """
        return await self.find_one({"file_id": file_id})