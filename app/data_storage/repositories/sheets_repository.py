from app.data_storage.repositories.base_repository import BaseRepository

class SheetsRepository(BaseRepository):
    def __init__(self, collection):
        super().__init__(collection)

    async def find_by_file_id(self, file_id: str):
        """
        Найти все листы по идентификатору файла
        """
        return await self.find({"file_id": file_id})