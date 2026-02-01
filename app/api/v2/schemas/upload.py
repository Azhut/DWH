from typing import List, Optional
from datetime import datetime
from pydantic import BaseModel
from app.api.v2.schemas.files import FileResponse


class UploadResponse(BaseModel):
    message: str
    details: List[FileResponse]
