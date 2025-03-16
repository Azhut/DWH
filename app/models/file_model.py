from datetime import datetime
from typing import Optional, List

from pydantic import BaseModel


class FileModel(BaseModel):
    file_id: str                      # Уникальный идентификатор файла
    filename: str                     # Имя загруженного файла
    year: int                         # Год, если доступно
    city: str                         # Город, если доступно
    status: str                       # Статус обработки файла (e.g., "processed", "error")
    error: Optional[str] = None       # Сообщение об ошибке, если есть
    upload_timestamp: datetime        # Время загрузки файла
    sheets: List[str] = []            # Список имен листов в файле
