from pydantic import BaseModel
from typing import List, Optional

class DocumentsRequest(BaseModel):
    section: str
    cities: Optional[List[str]] = []
    years: Optional[List[int]] = []
    rows: Optional[List[str]] = []
    columns: Optional[List[str]] = []