from fastapi import APIRouter, HTTPException

from app.api.v2.models.filters import (
    FilterValuesRequest,
    FilterValuesResponse,
    FiltersNamesResponse,
    FilteredDataRequest,
    FilteredDataResponse
)
from app.core.config import settings
from app.data_storage.data_retrieval_service import DataRetrievalService

router = APIRouter()

@router.get("/filters-names", response_model=FiltersNamesResponse)
async def get_filters_names():
    return {"filters": ["год", "город", "раздел", "строка", "колонка"]}

@router.post("/filter-values", response_model=FilterValuesResponse)
async def get_filter_values(request: FilterValuesRequest):
    service = DataRetrievalService(settings.DATABASE_URI, settings.DATABASE_NAME)
    try:
        filters_list = [item.model_dump(by_alias=True) for item in request.filters]
        values = await service.get_filter_values(
            request.filter_name,
            filters_list,
            request.pattern or ""
        )
        return FilterValuesResponse(filter_name=request.filter_name, values=values)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/filtered-data", response_model=FilteredDataResponse)
async def get_filtered_data(payload: FilteredDataRequest):
    service = DataRetrievalService(settings.DATABASE_URI, settings.DATABASE_NAME)
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
        raise HTTPException(status_code=500, detail=str(e))
