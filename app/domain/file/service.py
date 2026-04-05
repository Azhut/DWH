"""Сервис агрегата Files: метаданные, сохранение и удаление записей файлов."""

from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Any, List, Optional

from app.core.exceptions import FileValidationError
from app.domain.file.models import FileInfo, FileModel, FileStatus
from app.domain.file.repository import FileRepository

logger = logging.getLogger(__name__)

VALID_EXTENSIONS = (".xlsx", ".xls", ".xlsm")
YEAR_SEGMENT_PATTERN = re.compile(r"[0-9]{4}")


def validate_and_extract_metadata_from_filename(filename: str) -> FileInfo:
    """
    Проверяет имя файла и извлекает респондента, год и расширение.

    Правила (для всех форм одинаковые): расширение — после последней точки;
    в имени без расширения ровно одна последовательность из четырёх цифр (год);
    остальное в имени без года — субъект (reporter).
    """
    logger.debug("Validating file name: %s", filename)

    if not filename:
        raise FileValidationError("File name cannot be empty", filename=filename)

    if "." not in filename:
        raise FileValidationError(
            "File name must include an extension after '.'",
            filename=filename,
        )

    stem, ext = filename.rsplit(".", 1)
    if not ext.strip():
        raise FileValidationError("File name has an empty extension", filename=filename)

    ext_lower = ext.lower()
    if f".{ext_lower}" not in VALID_EXTENSIONS:
        raise FileValidationError(
            f"Invalid file extension: {filename}. Allowed: {list(VALID_EXTENSIONS)}",
            filename=filename,
        )

    year_matches = list(YEAR_SEGMENT_PATTERN.finditer(stem))
    if not year_matches:
        raise FileValidationError(
            "File name must contain exactly one 4-digit year in the name (before the extension)",
            filename=filename,
        )
    if len(year_matches) > 1:
        raise FileValidationError(
            "File name must contain only one sequence of 4 digits (the year)",
            filename=filename,
        )

    ym = year_matches[0]
    year = int(ym.group())
    subject_raw = stem[: ym.start()] + stem[ym.end() :]
    reporter = re.sub(r"\s+", " ", subject_raw).strip().upper()

    if not reporter:
        raise FileValidationError("Reporter (subject) cannot be empty", filename=filename)
    if year < 1900 or year > 2100:
        raise FileValidationError("Invalid year. Allowed range: 1900..2100", filename=filename)

    return FileInfo(reporter=reporter, year=year, extension=ext_lower)


class FileService:
    """Бизнес-логика коллекции Files."""

    def __init__(self, repository: FileRepository) -> None:
        self._repo = repository

    def validate_and_extract_metadata_from_filename(self, filename: str) -> FileInfo:
        """Извлекает метаданные из имени файла по правилам проекта."""
        return validate_and_extract_metadata_from_filename(filename)

    async def update_or_create(self, file_model: FileModel, *, session: Any = None) -> None:
        """
        Вставляет или обновляет документ по file_id.

        Параметр session используется для участия в много-документной транзакции MongoDB.
        """
        try:
            existing = await self._repo.find_by_file_id(file_model.file_id, session=session)
            data = file_model.model_dump()
            if isinstance(data.get("status"), FileStatus):
                data["status"] = data["status"].value
            data["updated_at"] = datetime.now()

            if existing:
                await self._repo.update_one(
                    {"file_id": file_model.file_id},
                    {"$set": data},
                    upsert=False,
                    session=session,
                )
                logger.debug("File %s updated", file_model.file_id)
            else:
                await self._repo.insert_one(data, session=session)
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

    async def list_files_by_status(
        self,
        status: FileStatus,
        limit: int = 1000,
    ) -> List[dict]:
        return await self._repo.find(
            query={"status": status.value if isinstance(status, FileStatus) else status},
            limit=limit,
        )

    async def list_files_by_form_id(self, form_id: str) -> List[dict]:
        return await self._repo.list_by_form_id(form_id)

    async def is_filename_unique(self, filename: str, form_id: Optional[str] = None) -> bool:
        doc = await self._repo.find_by_filename_and_status(filename, FileStatus.SUCCESS, form_id)
        return doc is None

    async def delete_by_file_id(self, file_id: str, *, session: Any = None) -> int:
        result = await self._repo.delete_one({"file_id": file_id}, session=session)
        return getattr(result, "deleted_count", 0) or 0

    async def delete_by_form_id(self, form_id: str, *, session: Any = None) -> int:
        result = await self._repo.delete_by_form_id(form_id, session=session)
        return getattr(result, "deleted_count", 0) or 0

    async def get_by_filename(self, filename: str, form_id: Optional[str] = None) -> Optional[FileModel]:
        doc = await self._repo.find_by_filename(filename, form_id)
        return FileModel(**doc) if doc else None

    async def get_by_filename_and_status(
        self,
        filename: str,
        status: FileStatus,
        form_id: Optional[str] = None,
    ) -> Optional[FileModel]:
        doc = await self._repo.find_by_filename_and_status(filename, status, form_id)
        return FileModel(**doc) if doc else None
