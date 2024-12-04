from pydantic import BaseModel
from typing import List, Optional

class FileResponse(BaseModel):
    filename: str
    status: str
    error: Optional[str] = None

class UploadResponse(BaseModel):
    message: str
    details: List[FileResponse]