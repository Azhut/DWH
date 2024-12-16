from fastapi import APIRouter, HTTPException, Depends
from typing import List, Dict, Optional
from app.models.sheet_model import SheetModel
from app.data_storage.data_management_service import DataManagementService
from app.data_storage.model_managment_service import DataRetrievalService
from app.core.config import settings

router = APIRouter()


data_retrieval_service = DataRetrievalService(settings.DATABASE_URI, settings.DATABASE_NAME)

@router.get("/sections", response_model=Dict[str, List[str]])
async def get_sections():
    try:
        sections = await data_retrieval_service.get_all_sections()
        return {"sections": sections}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка получения секций: {str(e)}")


@router.post("/documents-info", response_model=Dict[str, List])
async def get_documents_info(payload: Dict):
    cities = payload.get('cities', [])
    years = payload.get('years', [])

    try:
        cities, years, sections = await data_retrieval_service.get_document_info(cities, years)
        return {
            "cities": cities,
            "years": years,
            "sections": sections
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка получения информации о документах: {str(e)}")


@router.post("/documents-fields", response_model=Dict[str, List[str]])
async def get_documents_fields(payload: Dict):
    section = payload.get('section')
    cities = payload.get('cities', [])
    years = payload.get('years', [])

    try:
        rows, columns = await data_retrieval_service.get_document_fields(section, cities, years)
        return {
            "rows": rows,
            "columns": columns
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка получения полей документов: {str(e)}")


@router.post("/documents", response_model=Dict[str, List[Dict]])
async def get_documents(payload: Dict):
    section = payload.get('section')
    cities = payload.get('cities', [])
    years = payload.get('years', [])
    rows = payload.get('rows', [])
    columns = payload.get('columns', [])

    try:
        documents = await data_retrieval_service.get_documents(section, cities, years, rows, columns)
        return {"documents": documents}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка получения документов: {str(e)}")
