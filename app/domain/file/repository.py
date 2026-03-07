"""Репозиторий агрегата File: работа с коллекцией Files."""
from typing import Any, Dict, Optional

from app.domain.base import BaseRepository
from app.domain.file import FileStatus


class FileRepository(BaseRepository):
    async def find_by_file_id(self, file_id: str) -> Optional[Dict[str, Any]]:
        return await self.find_one({"file_id": file_id})

    async def find_by_filename(self, filename: str) -> Optional[Dict[str, Any]]:
        return await self.find_one({"filename": filename})

    async def find_by_filename_and_status(self, filename: str,status: FileStatus) -> Optional[Dict[str, Any]]:
        """Находит файл по имени и статусу."""
        return await self.find_one({
            "filename": filename,
            "status": status.value
        })
