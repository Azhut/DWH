"""
Сервис по работе с файлами при загрузке: валидация, метаданные, уникальность.
Вся логика, связанная с обработкой файлов на эндпоинте upload, делегируется сюда.
"""
import re
from dataclasses import dataclass
from typing import Tuple

from fastapi import HTTPException, UploadFile

from app.core.database import mongo_connection
from app.core.logger import logger


@dataclass
class FileInfo:
    """Модель файла с метаданными (город, год) для upload pipeline."""
    filename: str
    city: str
    year: int
    extension: str


VALID_EXTENSIONS = (".xlsx", ".xls", ".xlsm")
CITY_YEAR_PATTERN = re.compile(
    r"^(.+?)\s+(\d{4}).*\.(xls|xlsx|xlsm)$",
    re.IGNORECASE,
)


class FileHandlingService:
    """Сервис работы с файлами: валидация имени, извлечение метаданных, проверка уникальности."""

    def validate_and_extract_metadata(self, file: UploadFile) -> FileInfo:
        """
        Валидирует расширение и имя файла, извлекает город и год.
        Ожидаемый формат имени: «ГОРОД ГГГГ.расширение».
        """
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
        """Проверяет, что файл с таким именем ещё не загружался (нет в коллекции Files)."""
        db = mongo_connection.get_database()
        doc = await db.Files.find_one({"filename": filename})
        return doc is None
