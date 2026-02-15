# app/api/v2/schemas/filters.py
from typing import List, Union, Optional
from pydantic import BaseModel, Field, ConfigDict


class FilterItem(BaseModel):
    filter_name: str = Field(..., alias="filter-name", description="Название фильтра")
    values: List[Union[str, int]] = Field(..., description="Возможные значения фильтра")


class FilterValuesRequest(BaseModel):
    filter_name: str = Field(..., alias="filter-name", description="Фильтр для поиска значений")
    filters: List[FilterItem] = Field(default_factory=list, description="Применённые фильтры (без form)")
    pattern: Optional[str] = Field(None, description="Шаблон для поиска значений")

    model_config = ConfigDict(populate_by_name=True)


class FilterValuesResponse(BaseModel):
    filter_name: str = Field(..., alias="filter-name", description="Фильтр, по которому возвращены значения")
    values: List[Union[str, int, float]] = Field(..., description="Найденные значения фильтра")

    model_config = ConfigDict(populate_by_name=True)


class FiltersNamesResponse(BaseModel):
    filters: List[str] = Field(..., description="Доступные фильтры, например ['год', 'субъект', ...]")


class FilteredDataRequest(BaseModel):
    filters: List[FilterItem] = Field(default_factory=list, description="Бизнес-фильтры для выборки данных")
    limit: Optional[int] = Field(100, description="Лимит записей")
    offset: Optional[int] = Field(0, description="Смещение для пагинации")


class FilteredDataResponse(BaseModel):
    headers: List[str] = Field(..., description="Названия столбцов таблицы")
    data: List[List[Optional[Union[str, int, float]]]] = Field(..., description="Двумерный массив данных")
    size: int = Field(..., description="Количество записей в ответе")
    max_size: int = Field(..., description="Всего записей по фильтру")
