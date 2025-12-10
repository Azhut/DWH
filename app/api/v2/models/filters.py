from typing import List, Union, Optional
from pydantic import BaseModel, Field, ConfigDict


class FilterItem(BaseModel):
    filter_name: str = Field(..., alias="filter-name", description="Название фильтра, например, 'год' или 'город'.")
    values: List[Union[str, int]] = Field(..., description="Список значений для фильтра, например, [2022, 2023] или ['Москва', 'Санкт-Петербург'].")

    def to_query_dict(self):
        return {
            "filter-name": self.filter_name,
            "values": self.values
        }

class FilterValuesRequest(BaseModel):
    filter_name: str = Field(..., alias="filter-name", description="Название фильтра, для которого нужно получить значения.")
    filters: List[FilterItem] = Field(default=[], description="Список других фильтров и их значений для контекстного поиска.")
    pattern: Optional[str] = Field(None, description="Шаблон для поиска значений фильтра.")

    model_config = ConfigDict(populate_by_name=True)

class FilterValuesResponse(BaseModel):
    filter_name: str = Field(..., alias="filter-name", description="Название фильтра, для которого возвращены значения.")
    values: List[Union[str, int, float]] = Field(..., description="Список доступных значений для указанного фильтра.")
    model_config = ConfigDict(populate_by_name=True)

class FiltersNamesResponse(BaseModel):
    filters: List[str] = Field(..., description="Список доступных фильтров, например, ['год', 'город', 'раздел'].")

class FilteredDataRequest(BaseModel):
    filters: List[FilterItem] = Field(..., description="Список фильтров и их значений для получения данных.")
    limit: Optional[int] = Field(100, description="Максимальное количество записей в ответе.")
    offset: Optional[int] = Field(0, description="Смещение для пагинации.")

class FilteredDataResponse(BaseModel):
    headers: List[str] = Field(..., description="Список заголовков таблицы, например, ['год', 'город', 'раздел'].")
    data: List[List[Optional[Union[str, int, float]]]] = Field(..., description="Двумерный массив данных, соответствующий заголовкам.")
    size: int = Field(..., description="Количество записей в текущем ответе.")
    max_size: int = Field(..., description="Общее количество записей, доступных для данного набора фильтров.")