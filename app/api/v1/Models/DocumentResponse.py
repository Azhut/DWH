from pydantic import BaseModel
from typing import List, Dict, Optional, Union



class DocumentResponse(BaseModel):
    year: int
    city: str
    data: Union[Dict[str, Union[List[str], List[List[Union[str, float]]]]], List[Dict[str, Union[str, str, float]]]]