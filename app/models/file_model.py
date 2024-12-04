from pydantic import BaseModel
from datetime import datetime

class FileModel(BaseModel):
    file_id: str
    filename: str
    year: int
    city: str
    status: str
    upload_timestamp: datetime
