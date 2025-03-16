from typing import List, Optional

from pydantic import BaseModel


class FileResponse(BaseModel):
    filename: str
    status: str
    error: Optional[str] = None

class UploadResponse(BaseModel):
    message: str
    details: List[FileResponse]
