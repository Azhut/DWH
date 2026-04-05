# app/api/v2/endpoints/filters.py
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Query

from app.api.v2.schemas.filters import (
    FilterValuesRequest,
    FilterValuesResponse,
    FiltersNamesResponse,
    FilteredDataRequest,
    FilteredDataResponse,
)
from app.core.exceptions import log_and_raise_http
from app.core.dependencies import get_flat_data_service
from app.domain.form.service import validate_form_id
from app.domain.flat_data.models import FILTER_MAP, TABLE_HEADERS

router = APIRouter()

# пользовательский набор фильтров (контракт API)
FILTERS = list(FILTER_MAP.keys())


def _validate_filter_name(filter_name: str) -> None:
    """Проверяет, что имя фильтра входит в допустимый набор."""
    if filter_name not in FILTERS:
        raise HTTPException(status_code=400, detail=f"Неизвестный фильтр: {filter_name}")


@router.get("/filters-names", response_model=FiltersNamesResponse)
async def get_filters_names(form_id: Optional[str] = Query(None, description="ID формы")):
    """
    Возвращает список доступных фильтров.
    form_id обязателен в query
    """
    validate_form_id(form_id)
    return {"filters": FILTERS}


@router.post("/filter-values", response_model=FilterValuesResponse)
async def get_filter_values(
    request: FilterValuesRequest,
    form_id: Optional[str] = Query(None, description="ID формы"),
    svc=Depends(get_flat_data_service)
):
    """
    Возвращает значения для указанного фильтра с учётом переданных бизнес-фильтров и pattern
    form_id обязательный query-параметр
    """
    try:
        validate_form_id(form_id)

        # валидация имени фильтра
        _validate_filter_name(request.filter_name)

        # подготовим фильтры в формате [{'filter-name':..., 'values':[...]}]
        filters_list = [item.model_dump(by_alias=True) for item in request.filters]

        values = await svc.get_filter_values(
            request.filter_name,
            filters_list,
            request.pattern or "",
            form_id
        )
        return FilterValuesResponse(filter_name=request.filter_name, values=values)
    except HTTPException:
        raise
    except Exception as e:
        log_and_raise_http(500, "Ошибка при получении значений фильтра", e)


@router.post("/filtered-data", response_model=FilteredDataResponse)
async def get_filtered_data(
    payload: FilteredDataRequest,
    form_id: Optional[str] = Query(None, description="ID формы"),
    svc=Depends(get_flat_data_service)
):
    """
    Возвращает таблицу данных по бизнес-фильтрам с пагинацией.
    form_id обязательный query-параметр
    """
    try:
        validate_form_id(form_id)

        # валидация имен фильтров внутри списка
        for f in payload.filters:
            _validate_filter_name(f.filter_name)

        filters_list = [item.model_dump(by_alias=True) for item in payload.filters]

        data, total = await svc.get_filtered_data(
            filters_list,
            payload.limit,
            payload.offset,
            form_id
        )
        return FilteredDataResponse(
            headers=TABLE_HEADERS,
            data=data,
            size=len(data),
            max_size=total
        )
    except HTTPException:
        raise
    except Exception as e:
        log_and_raise_http(500, "Ошибка при получении отфильтрованной таблицы", e)
