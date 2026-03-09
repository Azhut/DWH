"""Repository for Files aggregate."""

from typing import Any, Dict, Optional

from app.domain.base import BaseRepository
from app.domain.file import FileStatus


class FileRepository(BaseRepository):
    async def find_by_file_id(self, file_id: str) -> Optional[Dict[str, Any]]:
        return await self.find_one({"file_id": file_id})

    async def find_by_filename(self, filename: str, form_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        query: Dict[str, Any] = {"filename": filename}
        if form_id is not None:
            query["form_id"] = form_id
        return await self.find_one(query)

    async def find_by_filename_and_status(
        self,
        filename: str,
        status: FileStatus,
        form_id: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Find file by filename and status, optionally scoped by form_id."""
        query: Dict[str, Any] = {
            "filename": filename,
            "status": status.value,
        }
        if form_id is not None:
            query["form_id"] = form_id
        return await self.find_one(query)

    async def list_by_form_id(self, form_id: str, *, projection: Optional[Dict[str, Any]] = None) -> list[Dict[str, Any]]:
        return await self.find(query={"form_id": form_id}, projection=projection)

    async def delete_by_form_id(self, form_id: str) -> Any:
        return await self.delete_many({"form_id": form_id})
