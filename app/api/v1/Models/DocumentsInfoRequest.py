from pydantic import BaseModel
from typing import List, Optional

class DocumentsInfoRequest(BaseModel):
    cities: Optional[List[str]] = []
    years: Optional[List[int]] = []