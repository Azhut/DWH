"""File aggregate models."""
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


@dataclass
class FileInfo:
    """Metadata extracted from file name."""
    reporter: str
    year: int
    extension: str


class FileModel(BaseModel):
    """Record in Files collection."""

    file_id: str
    form_id: Optional[str] = None
    filename: str
    year: Optional[int] = None
    reporter: Optional[str] = None
    status: FileStatus = FileStatus.PROCESSING
    error: Optional[str] = None
    upload_timestamp: datetime = Field(default_factory=datetime.now)
    sheets: List[str] = []
    flat_data_size: int = 0
    updated_at: datetime = Field(default_factory=datetime.now)
    model_config = ConfigDict(arbitrary_types_allowed=True)

    @classmethod
    def create_processing(
        cls,
        filename: str,
        form_id: Optional[str] = None,
        file_info: Optional[FileInfo] = None,
    ) -> "FileModel":
        """Create a processing record that can be reused across retries."""
        return cls(
            file_id=str(uuid4()),
            filename=filename,
            year=file_info.year if file_info else None,
            reporter=file_info.reporter if file_info else None,
            status=FileStatus.PROCESSING,
            error=None,
            upload_timestamp=datetime.now(),
            sheets=[],
            size=0,
            form_id=form_id,
        )

    @classmethod
    def create_new(
        cls,
        filename: str,
        file_info: FileInfo,
        form_id: Optional[str] = None,
    ) -> "FileModel":
        return cls.create_processing(
            filename=filename,
            form_id=form_id,
            file_info=file_info,
        )

    @classmethod
    def create_stub(
        cls,
        filename: str,
        form_id: str,
        error_message: str,
        file_info: Optional[FileInfo] = None,
    ) -> "FileModel":
        return cls(
            file_id=str(uuid4()),
            filename=filename,
            form_id=form_id,
            error=error_message,
            year=file_info.year if file_info else None,
            reporter=file_info.reporter if file_info else None,
            status=FileStatus.FAILED,
        )
