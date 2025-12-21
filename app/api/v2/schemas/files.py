from typing import List, Optional
from datetime import datetime
from pydantic import BaseModel

class FileListResponse(BaseModel):
    file_id: str
    filename: str
    status: str
    error: Optional[str] = None
    upload_timestamp: datetime
    updated_at: datetime
    year: Optional[int] = None
    form_id: Optional[str] = None


class DeleteFileResponse(BaseModel):
    detail: str

class FileResponse(BaseModel):
    filename: str
    status: str
    error: Optional[str] = None

class UploadResponse(BaseModel):
    message: str
    details: List[FileResponse]
