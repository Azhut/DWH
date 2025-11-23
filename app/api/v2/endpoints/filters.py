from fastapi import APIRouter

from app.api.v2.models.filters import (
    FilterValuesRequest,
    FilterValuesResponse,
    FiltersNamesResponse,
    FilteredDataRequest,
    FilteredDataResponse
)
from app.core.exceptions import log_and_raise_http
from app.data.services.data_retrieval import create_data_retrieval_service

router = APIRouter()

@router.get("/filters-names", response_model=FiltersNamesResponse)
async def get_filters_names():
    """
    Возвращает список доступных фильтров.

    **Пример ответа:**
    ```json
    {
        "filters": ["год", "город", "раздел", "строка", "колонка"]
    }
    ```

    **Коды ответа:**
    - `200 OK`: Успешный запрос.
    """
    return {"filters": ["год", "город", "раздел", "строка", "колонка"]}

@router.post("/filter-values", response_model=FilterValuesResponse)
async def get_filter_values(request: FilterValuesRequest):
    """
    Возвращает доступные значения для указанного фильтра с учётом других фильтров и шаблона поиска.

    **Пример запроса:**
    ```json
    {
        "filter-name": "раздел",
        "filters": [
            {
                "filter-name": "год",
                "values": [2022, 2023, 2024]
            },
            {
                "filter-name": "город",
                "values": ["Алапаевск"]
            }
        ],
        "pattern": ""
    }
    ```

    **Пример ответа:**
    ```json
    {
        "filter-name": "раздел",
        "values": ["Раздел 1", "Раздел 2", "Раздел 3"]
    }
    ```

    **Коды ответа:**
    - `200 OK`: Успешный запрос.
    - `500 Internal Server Error`: Ошибка сервера.
    """
    service = create_data_retrieval_service()
    try:
        filters_list = [item.model_dump(by_alias=True) for item in request.filters]
        values = await service.get_filter_values(
            request.filter_name,
            filters_list,
            request.pattern or ""
        )
        return FilterValuesResponse(filter_name=request.filter_name, values=values)
    except Exception as e:
        log_and_raise_http(500, "Ошибка при получении значений фильтра", e)

@router.post("/filtered-data", response_model=FilteredDataResponse)
async def get_filtered_data(payload: FilteredDataRequest):
    """
    Возвращает данные, отфильтрованные по указанным параметрам.

    **Пример запроса:**
    ```json
    {
        "filters": [
            {
                "filter-name": "год",
                "values": [2022, 2023, 2024]
            },
            {
                "filter-name": "город",
                "values": ["Алапаевск"]
            }
        ],
        "limit": 4,
        "offset": 0
    }
    ```

    **Пример ответа:**
    ```json
    {
        "headers": ["год", "город", "раздел", "строка", "колонка", "значение"],
        "data": [
            [2022, "Алапаевск", "Раздел1", "строка1", "колонка1", 10],
            [2022, "Алапаевск", "Раздел1", "строка2", "колонка1", 11],
            [2023, "Алапаевск", "Раздел1", "строка1", "колонка1", 13],
            [2023, "Алапаевск", "Раздел1", "строка2", "колонка1", 18]
        ],
        "size": 4,
        "max_size": 100
    }
    ```

    **Коды ответа:**
    - `200 OK`: Успешный запрос.
    - `500 Internal Server Error`: Ошибка сервера.
    """
    service = create_data_retrieval_service()
    try:
        filters_list = [item.model_dump(by_alias=True) for item in payload.filters]
        data, total = await service.get_filtered_data(
            filters_list,
            payload.limit,
            payload.offset
        )
        return FilteredDataResponse(
            headers=["год", "город", "раздел", "строка", "колонка", "значение"],
            data=data,
            size=len(data),
            max_size=total
        )
    except Exception as e:
        log_and_raise_http(500, "Ошибка при получении отфильтрованной таблицы", e)
