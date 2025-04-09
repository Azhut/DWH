from typing import List

from fastapi import APIRouter, UploadFile, File, Depends, HTTPException

from app.api.v2.schemas.files import UploadResponse
from app.features.files.services.ingestion_service import IngestionService

router = APIRouter()


@router.post("/upload", response_model=UploadResponse)
async def upload_files(
    files: List[UploadFile] = File(...),
    ingestion_service: IngestionService = Depends()
):
    try:
        result = await ingestion_service.process_files(files)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка загрузки файлов: {str(e)}")
