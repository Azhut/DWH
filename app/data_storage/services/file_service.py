from app.data_storage.repositories.file_repository import FileRepository
from app.models.file_model import FileModel
from typing import List

class FileService:
    def __init__(self, file_repo: FileRepository):
        self.file_repo = file_repo

    async def save_file(self, file_model: FileModel):
        """
        Save a file document to the database.
        """
        file_doc = file_model.dict()
        await self.file_repo.insert_one(file_doc)

    async def get_file_by_id(self, file_id: str) -> FileModel:
        """
        Retrieve a file document by its ID.
        """
        file_doc = await self.file_repo.find_by_file_id(file_id)
        return FileModel(**file_doc) if file_doc else None