from pydantic import BaseModel
from typing import List, Dict, Optional, Union

class DocumentsInfoRequest(BaseModel):
    cities: Optional[List[str]] = []
    years: Optional[List[int]] = []