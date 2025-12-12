from typing import Optional, Dict, Any
from app.data.repositories.base import BaseRepository

class FileRepository(BaseRepository):
    async def find_by_file_id(self, file_id: str) -> Optional[Dict[str, Any]]:
        return await self.find_one({"file_id": file_id})

    async def find_by_filename(self, filename: str) -> Optional[Dict[str, Any]]:
        # На уровне базы filename — не уникальное поле, но поиск помогает обнаруживать дубликат загрузок
        return await self.find_one({"filename": filename})
