from datetime import datetime
from pydantic import BaseModel, Field, ConfigDict
from typing import Optional, List
from uuid import uuid4

from app.models.file_status import FileStatus


class FileModel(BaseModel):
    file_id: str
    form_id: Optional[str] = None
    filename: str
    year: Optional[int] = None
    city: Optional[str] = None
    status: FileStatus = FileStatus.PROCESSING
    error: Optional[str] = None
    upload_timestamp: datetime = Field(default_factory=datetime.now)
    sheets: List[str] = []
    size: int = 0
    updated_at: datetime = Field(default_factory=datetime.now)
    model_config = ConfigDict(arbitrary_types_allowed=True)

    @classmethod
    def create_new(cls, filename: str, year: Optional[int] = None, city: Optional[str] = None, form_id: Optional[str] = None) -> "FileModel":
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
            size=0,
            form_id=form_id
        )

    @classmethod
    def create_stub(
            cls,
            file_id: str,
            filename: str,
            form_id: str,
            error_message: str,  # Этот параметр не используется правильно
            year: int | None = None,
            city: str | None = None
    ):
        return cls(
            file_id=file_id,
            filename=filename,
            form_id=form_id,
            error=error_message,
            year=year,
            city=city,
            status=FileStatus.FAILED
        )
