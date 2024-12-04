from typing import Optional

from pydantic import BaseModel
from datetime import datetime

class FileModel(BaseModel):
    file_id: str
    filename: str
    year: int
    city: str
    status: str
    error: Optional[str] = None  # Добавлено поле error
    upload_timestamp: datetime
