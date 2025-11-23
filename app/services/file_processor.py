from fastapi import UploadFile, HTTPException
from pydantic import BaseModel
from typing import Tuple
import re
from app.core.logger import logger

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
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Ошибка извлечения метаданных для {file.filename}: {e}", exc_info=True)
            raise HTTPException(status_code=400, detail="Некорректное имя файла. Ожидается: 'ГОРОД ГГГГ.расширение'")

    def _validate_file_extension(self, filename: str):
        if not any(filename.lower().endswith(ext) for ext in self.VALID_EXTENSIONS):
            raise HTTPException(status_code=400, detail=f"Некорректное расширение файла: {filename}. Допустимые: {self.VALID_EXTENSIONS}")

    def _extract_city_year(self, filename: str) -> Tuple[str, int]:
        """
        Извлекает год и город из имени файла.
        Ожидаемый формат: "ГОРОД YYYY ... .xls(x)"
        """
        pattern = r"""
            ^               # Начало строки
            (.+?)           # Город (нежадный захват)
            \s+             # Разделитель (один или больше пробелов)
            (\d{4})         # Год (4 цифры)
            .*              # Остальное до расширения
            \.(xls|xlsx|xlsm)$
        """
        match = re.match(pattern, filename, re.VERBOSE | re.IGNORECASE)
        if not match:
            raise HTTPException(status_code=400, detail="Некорректный формат имени файла. Ожидается: 'ГОРОД ГГГГ.расширение'")

        city = match.group(1).strip().upper()
        year = int(match.group(2))

        if not city:
            raise HTTPException(status_code=400, detail="Название города не может быть пустым")
        if year < 1900 or year > 2100:
            raise HTTPException(status_code=400, detail="Недопустимый год. Допустимый диапазон: от 1900 до 2100 года")

        return city, year
