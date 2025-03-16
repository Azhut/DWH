from typing import List, Union, Optional

from pydantic import BaseModel, Field


class FilterItem(BaseModel):
    filter_name: str = Field(..., alias="filter-name")
    values: List[Union[str, int]]

    def to_query_dict(self):
        return {
            "filter-name": self.filter_name,
            "values": self.values
        }

class FilterValuesRequest(BaseModel):
    filter_name: str = Field(..., alias="filter-name")
    filters: List[FilterItem] = []
    pattern: Optional[str] = None

    class Config:
        populate_by_name = True

class FilterValuesResponse(BaseModel):
    filter_name: str = Field(..., alias="filter-name")
    values: List[Union[str, int]]

    class Config:
        populate_by_name = True

class FiltersNamesResponse(BaseModel):
    filters: List[str]


class FilteredDataRequest(BaseModel):
    filters: List[FilterItem]
    limit: Optional[int] = 100
    offset: Optional[int] = 0

class FilteredDataResponse(BaseModel):
    headers: List[str]
    data: List[List[Optional[Union[str, int]]]]
    size: int
    max_size: int