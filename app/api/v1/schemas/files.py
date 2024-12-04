from pydantic import BaseModel
from typing import Optional

class FileResponse(BaseModel):
    filename: str
    status: str
    error: Optional[str] = None

class UploadResponse(BaseModel):
    message: str
    details: list[FileResponse]
