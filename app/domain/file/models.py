"""Модели агрегата File: сущность файла, статус, метаданные для upload."""
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import List, Optional
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field


class FileStatus(str, Enum):
    SUCCESS = "success"
    FAILED = "failed"
    PROCESSING = "processing"
    DUPLICATE = "duplicate"


class FileModel(BaseModel):
    """Сущность файла (запись в коллекции Files)."""
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
    def create_new(
        cls,
        filename: str,
        year: Optional[int] = None,
        city: Optional[str] = None,
        form_id: Optional[str] = None,
    ) -> "FileModel":
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
            form_id=form_id,
        )

    @classmethod
    def create_stub(
        cls,
        file_id: str,
        filename: str,
        form_id: str,
        error_message: str,
        year: Optional[int] = None,
        city: Optional[str] = None,
    ) -> "FileModel":
        return cls(
            file_id=file_id,
            filename=filename,
            form_id=form_id,
            error=error_message,
            year=year,
            city=city,
            status=FileStatus.FAILED,
        )


@dataclass
class FileInfo:
    """Метаданные файла (город, год) из имени файла для upload pipeline."""
    filename: str
    city: str
    year: int
    extension: str
