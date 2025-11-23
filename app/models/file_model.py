from datetime import datetime
from pydantic import BaseModel, Field
from typing import Optional, List
from uuid import uuid4

from app.models.file_status import FileStatus


class FileModel(BaseModel):
    file_id: str                                                # UUID
    filename: str                                               # Оригинальное имя
    year: Optional[int] = None
    city: Optional[str] = None
    status: FileStatus = FileStatus.PROCESSING
    error: Optional[str] = None
    upload_timestamp: datetime = Field(default_factory=datetime.now)
    sheets: List[str] = []
    size: int = 0
    updated_at: datetime = Field(default_factory=datetime.now)

    @classmethod
    def create_new(cls, filename: str, year: Optional[int] = None, city: Optional[str] = None) -> "FileModel":
        """
        Создаёт новый FileModel с сгенерированным UUID (file_id).
        Использовать при старте обработки файла.
        """
        return cls(
            file_id=str(uuid4()),
            filename=filename,
            year=year,
            city=city,
            status=FileStatus.PROCESSING,
            error=None,
            upload_timestamp=datetime.now(),
            sheets=[],
            size=0
        )

    @classmethod
    def create_stub(
        cls,
        file_id: str,
        filename: str,
        error_message: str,
        year: Optional[int] = None,
        city: Optional[str] = None
    ) -> "FileModel":
        """
        Создаёт заглушечную модель при ошибках.
        """
        return cls(
            file_id=file_id,
            filename=filename,
            year=year,
            city=city,
            status=FileStatus.FAILED,
            error=error_message,
            upload_timestamp=datetime.now(),
            sheets=[],
            size=0
        )
