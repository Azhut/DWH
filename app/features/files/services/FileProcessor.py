from fastapi import UploadFile, HTTPException
from pydantic import BaseModel
from typing import Tuple
import re

from app.core.exception_handler import logger, log_and_raise_http


class FileMetadata(BaseModel):
    city: str
    year: int
    filename: str
    extension: str


class FileProcessor:
    VALID_EXTENSIONS = [".xlsx", ".xls", ".xlsm"]

    def validate_and_extract_metadata(self, file: UploadFile) -> FileMetadata:
        logger.debug(f"Валидация файла {file.filename}")
        try:
            self._validate_file_extension(file.filename)
            city, year = self._extract_city_year(file.filename)
            return FileMetadata(
                city=city,
                year=year,
                filename=file.filename,
                extension=file.filename.split(".")[-1].lower()
            )
        except HTTPException as e:
            raise
        except Exception as e:
            logger.error(f"Ошибка извлечения метаданных для {file.filename}: {str(e)}")
            log_and_raise_http(400, "Некорректное имя файла. Ожидается: 'ГОРОД ГГГГ.расширение", e)

    def _validate_file_extension(self, filename: str):
        if not any(filename.endswith(ext) for ext in self.VALID_EXTENSIONS):
            raise HTTPException(
                status_code=400,
                detail=f"Некорректное расширение файла: {filename}. Допустимые: {self.VALID_EXTENSIONS}"
            )

    def _extract_city_year(self, filename: str) -> Tuple[str, int]:
        """
        Извлекает год и город из имени файла.
        """

        pattern = r"""
            ^               # Начало строки
            (.+?)           # Город (нежадный захват)
            \s+             # Разделитель (один или больше пробелов)
            (\d{4})         # Год (4 цифры)
            [^\d]*          # Все символы после года и до расширения (нежадный захват)
            \.              # Точка перед расширением
            (xls|xlsx|xlsm) # Расширение файла
            $               # Конец строки
        """
        match = re.match(pattern, filename, re.VERBOSE | re.IGNORECASE)
        if not match:
            raise HTTPException(400, "Некорректный формат имени файла. Ожидается: 'ГОРОД ГГГГ.расширение'")

        city = match.group(1).strip().upper()
        year = int(match.group(2))

        if not city:
            raise HTTPException(400, "Название города не может быть пустым")
        if year < 1900 or year > 2100:
            raise HTTPException(400, "Недопустимый год. Допустимый диапазон: от 1900 до 2100 года")

        return city, year

    def get_metadata_dict(self, metadata: FileMetadata) -> dict:
        return {
            "city": metadata.city.title(),
            "year": metadata.year,
            "original_name": metadata.filename,
            "file_type": metadata.extension.upper()
        }