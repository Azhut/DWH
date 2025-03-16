from typing import List, Optional

from pydantic import BaseModel


class DocumentsFieldsRequest(BaseModel):
    section: str
    cities: Optional[List[str]] = []
    years: Optional[List[int]] = []