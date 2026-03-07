"""File aggregate service."""

import logging
import re
from datetime import datetime
from typing import List, Optional

from fastapi import HTTPException

from app.domain.file.models import FileInfo, FileModel, FileStatus
from app.domain.file.repository import FileRepository

logger = logging.getLogger(__name__)

VALID_EXTENSIONS = (".xlsx", ".xls", ".xlsm")
REPORTER_YEAR_PATTERN = re.compile(r"^(.+?)\s+(\d{4}).*\.(xls|xlsx|xlsm)$", re.IGNORECASE)


def validate_and_extract_metadata_from_filename(filename: str) -> FileInfo:
    """Validate filename and extract reporter/year metadata."""
    logger.debug("Validating file name: %s", filename)

    if not filename:
        raise HTTPException(status_code=400, detail="File name cannot be empty")

    if not any(filename.lower().endswith(ext) for ext in VALID_EXTENSIONS):
        raise HTTPException(
            status_code=400,
            detail=f"Invalid file extension: {filename}. Allowed: {list(VALID_EXTENSIONS)}",
        )

    match = REPORTER_YEAR_PATTERN.match(filename)
    if not match:
        raise HTTPException(
            status_code=400,
            detail="Invalid file name format. Expected: 'REPORTER YYYY.extension'",
        )

    reporter = match.group(1).strip().upper()
    year = int(match.group(2))
    ext = filename.split(".")[-1].lower() if "." in filename else ""

    if not reporter:
        raise HTTPException(status_code=400, detail="Reporter cannot be empty")
    if year < 1900 or year > 2100:
        raise HTTPException(
            status_code=400,
            detail="Invalid year. Allowed range: 1900..2100",
        )

    return FileInfo(reporter=reporter, year=year, extension=ext)


class FileService:
    """Business logic for Files aggregate."""

    def __init__(self, repository: FileRepository):
        self._repo = repository

    def validate_and_extract_metadata_from_filename(self, filename: str) -> FileInfo:
        """Instance wrapper to keep metadata API on FileService."""
        return validate_and_extract_metadata_from_filename(filename)

    async def update_or_create(self, file_model: FileModel) -> None:
        """Insert file record or update existing record by file_id."""
        try:
            existing = await self._repo.find_by_file_id(file_model.file_id)
            data = file_model.model_dump()
            if isinstance(data.get("status"), FileStatus):
                data["status"] = data["status"].value
            data["updated_at"] = datetime.now()

            if existing:
                await self._repo.update_one(
                    {"file_id": file_model.file_id},
                    {"$set": data},
                    upsert=False,
                )
                logger.debug("File %s updated", file_model.file_id)
            else:
                await self._repo.insert_one(data)
                logger.debug("File %s created", file_model.file_id)
        except Exception as exc:
            logger.error(
                "Failed to persist file %s: %s",
                file_model.file_id,
                exc,
                exc_info=True,
            )
            raise

    async def get_by_id(self, file_id: str) -> Optional[FileModel]:
        doc = await self._repo.find_by_file_id(file_id)
        return FileModel(**doc) if doc else None

    async def list_files(
        self,
        limit: int = 100,
        offset: int = 0,
        year: Optional[int] = None,
    ) -> List[dict]:
        query = {} if year is None else {"year": year}
        return await self._repo.find(query=query, limit=limit, skip=offset)

    async def is_filename_unique(self, filename: str) -> bool:
        doc = await self._repo.find_by_filename_and_status(filename, FileStatus.SUCCESS)
        return doc is None

    async def delete_by_file_id(self, file_id: str) -> int:
        result = await self._repo.delete_one({"file_id": file_id})
        return getattr(result, "deleted_count", 0) or 0

    async def get_by_filename(self, filename: str) -> Optional[FileModel]:
        doc = await self._repo.find_by_filename(filename)
        return FileModel(**doc) if doc else None

    async def get_by_filename_and_status(self, filename: str, status: FileStatus) -> Optional[FileModel]:
        doc = await self._repo.find_by_filename_and_status(filename, status)
        return FileModel(**doc) if doc else None
