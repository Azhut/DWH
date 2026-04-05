from typing import List, Optional
from pydantic import BaseModel
from app.api.v2.schemas.files import FileResponse


class UploadResponse(BaseModel):
    message: str
    details: List[FileResponse]
    upload_id: Optional[str] = None  # Optional для обратной совместимости
