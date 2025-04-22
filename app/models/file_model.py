from datetime import datetime
from pydantic import BaseModel, Field
from typing import Optional, List

from app.models.file_status import FileStatus


class FileModel(BaseModel):
    file_id: str                                                # Уникальный идентификатор файла
    filename: str                                               # Имя загруженного файла
    year: Optional[int] = None                                  # Год, если доступно
    city: Optional[str] = None                                  # Город, если доступно
    status: FileStatus = FileStatus.PROCESSING                  # Статус обработки файла (e.g., "processed", "error")
    error: Optional[str] = None                                 # Сообщение об ошибке, если есть
    upload_timestamp: datetime                                  # Время загрузки файла
    sheets: List[str] = []                                      # Список имен листов в файле
    size: int = 0                                               # Размер данных
    updated_at: datetime = Field(default_factory=datetime.now)  # Время последнего обновления

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
            status=FileStatus.FAILED,
            error=error_message,
            upload_timestamp=datetime.now()
        )