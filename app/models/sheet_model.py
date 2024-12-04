from pydantic import BaseModel
from typing import Dict

class SheetModel(BaseModel):
    sheet_name: str
    data: Dict
    file_id: str
