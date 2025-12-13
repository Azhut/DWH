from typing import List, Optional
from datetime import datetime
from pydantic import BaseModel, Field

class FormCreateRequest(BaseModel):
    name: str
    spravochno_keywords: Optional[List[str]] = []
    skip_sheets: Optional[List[int]] = []

class FormResponse(BaseModel):
    id: str
    name: str
    spravochno_keywords: Optional[List[str]] = []
    skip_sheets: Optional[List[int]] = []
    created_at: Optional[str] = None

class FormsListResponse(BaseModel):
    forms: List[FormResponse]
