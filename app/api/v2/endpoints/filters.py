from fastapi import APIRouter, HTTPException, Depends
from typing import List
from app.data_storage.data_retrieval_service import DataRetrievalService
from app.core.config import settings

router = APIRouter()

@router.get("/filters-names", response_model=dict)
async def get_filters_names():
    return {"filters": ["год", "город", "раздел", "строка", "колонка"]}

@router.post("/filter-values", response_model=dict)
async def get_filter_values(payload: dict):
    service = DataRetrievalService(settings.DATABASE_URI, settings.DATABASE_NAME)
    try:
        values = await service.get_filter_values(
            payload["filter-name"],
            payload.get("filters", []),
            payload.get("pattern", "")
        )
        return {"filter-name": payload["filter-name"], "values": values}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/filtered-data", response_model=dict)
async def get_filtered_data(payload: dict):
    service = DataRetrievalService(settings.DATABASE_URI, settings.DATABASE_NAME)
    try:
        data, total = await service.get_filtered_data(
            payload.get("filters", []),
            payload.get("limit", 100),
            payload.get("offset", 0)
        )
        return {
            "headers": ["год", "город", "раздел", "строка", "колонка", "значение"],
            "data": data,
            "size": len(data),
            "max_size": total
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))