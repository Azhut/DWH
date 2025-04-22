from dataclasses import Field
from datetime import datetime
from uuid import uuid4

from pydantic import BaseModel
from typing import Optional, List

class FileModel(BaseModel):
    file_id: str                              # Уникальный идентификатор файла
    filename: str                             # Имя загруженного файла
    year: Optional[int] = None                # Год, если доступно
    city: Optional[str] = None                # Город, если доступно
    status: str                               # Статус обработки файла (e.g., "processed", "error")
    error: Optional[str] = None               # Сообщение об ошибке, если есть
    upload_timestamp: datetime                # Время загрузки файла
    sheets: List[str] = []                    # Список имен листов в файле
    size: int = 0                             # Размер данных

    @classmethod
    def create_stub(cls, file_id: str, filename: str, error_message: str = "Unknown error") -> "FileModel":
        """
        Создает "заглушечную" модель FileModel для случаев ошибок.
        :param file_id: Уникальный идентификатор файла.
        :param filename: Имя файла.
        :param error_message: Сообщение об ошибке.
        :return: Экземпляр FileModel со статусом "failed".
        """
        return cls(
            file_id=file_id,
            filename=filename,
            status="failed",
            error=error_message,
            upload_timestamp=datetime.now()
        )