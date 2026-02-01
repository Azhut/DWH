"""Сервис агрегата File: бизнес-логика (сохранение, получение, валидация имени, уникальность)."""
import logging
import re
from datetime import datetime
from typing import List, Optional

from fastapi import HTTPException, UploadFile

from app.domain.file.models import FileModel, FileStatus, FileInfo
from app.domain.file.repository import FileRepository

logger = logging.getLogger(__name__)

VALID_EXTENSIONS = (".xlsx", ".xls", ".xlsm")
CITY_YEAR_PATTERN = re.compile(r"^(.+?)\s+(\d{4}).*\.(xls|xlsx|xlsm)$", re.IGNORECASE)


class FileService:
    """Вся бизнес-логика по сущности File: персистенция, валидация имени, проверка уникальности."""

    def __init__(self, repository: FileRepository):
        self._repo = repository

    async def update_or_create(self, file_model: FileModel) -> None:
        """Сохраняет или обновляет запись Files."""
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
            else:
                await self._repo.insert_one(data)
        except Exception as e:
            logger.error("Ошибка при сохранении файла %s: %s", file_model.file_id, e, exc_info=True)
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
        """Список файлов с пагинацией и опциональным фильтром по году."""
        query = {} if year is None else {"year": year}
        docs = await self._repo.find(query=query, limit=limit, skip=offset)
        return docs

    def validate_and_extract_metadata(self, file: UploadFile) -> FileInfo:
        """Валидирует расширение и имя файла, извлекает город и год. Ожидается формат: ГОРОД ГГГГ.расширение."""
        filename = file.filename or ""
        logger.debug("Валидация файла %s", filename)

        if not any(filename.lower().endswith(ext) for ext in VALID_EXTENSIONS):
            raise HTTPException(
                status_code=400,
                detail=f"Некорректное расширение файла: {filename}. Допустимые: {list(VALID_EXTENSIONS)}",
            )

        match = CITY_YEAR_PATTERN.match(filename)
        if not match:
            raise HTTPException(
                status_code=400,
                detail="Некорректное имя файла. Ожидается: 'ГОРОД ГГГГ.расширение'",
            )

        city = match.group(1).strip().upper()
        year = int(match.group(2))
        ext = filename.split(".")[-1].lower() if "." in filename else ""

        if not city:
            raise HTTPException(status_code=400, detail="Название города не может быть пустым")
        if year < 1900 or year > 2100:
            raise HTTPException(
                status_code=400,
                detail="Недопустимый год. Допустимый диапазон: от 1900 до 2100 года",
            )

        return FileInfo(filename=filename, city=city, year=year, extension=ext)

    async def is_filename_unique(self, filename: str) -> bool:
        """Проверяет, что файл с таким именем ещё не загружался."""
        doc = await self._repo.find_by_filename(filename)
        return doc is None

    async def delete_by_file_id(self, file_id: str) -> int:
        """Удаляет запись файла по file_id. Возвращает deleted_count."""
        result = await self._repo.delete_one({"file_id": file_id})
        return getattr(result, "deleted_count", 0) or 0
