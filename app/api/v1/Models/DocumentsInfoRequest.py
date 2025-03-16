from typing import List, Optional

from pydantic import BaseModel


class DocumentsInfoRequest(BaseModel):
    cities: Optional[List[str]] = []
    years: Optional[List[int]] = []