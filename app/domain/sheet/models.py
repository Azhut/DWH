"""Модели агрегата Sheet: представление листа (SheetModel)."""
from typing import Dict, List, Optional, Union

from pydantic import BaseModel, ConfigDict


class SheetModel(BaseModel):
    file_id: str
    sheet_name: str
    sheet_fullname: str
    year: int
    city: str
    headers: Dict[str, List[str]]
    data: List[Dict[str, Union[str, List[Dict[str, Optional[Union[str, float]]]]]]]

    model_config = ConfigDict(arbitrary_types_allowed=True)
