from pydantic import BaseModel
from typing import List, Dict, Optional, Union

class DocumentsFieldsRequest(BaseModel):
    section: str
    cities: Optional[List[str]] = []
    years: Optional[List[int]] = []