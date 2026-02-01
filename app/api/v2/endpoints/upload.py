from typing import List
from app.api.v2.schemas.upload import UploadResponse
from app.core.exceptions import log_and_raise_http
from prometheus_client import Counter
from app.core.dependencies import get_ingestion_service
from app.services.ingestion_service import IngestionService
from fastapi import APIRouter, UploadFile, File, Form, Depends, HTTPException, Query

router = APIRouter()
FILE_PROCESSED = Counter('files_processed', 'Total processed files')


@router.post("/upload", response_model=UploadResponse)
async def upload_files(
     files: List[UploadFile] = File(...),
     form_id: str = Query(..., description="ID формы"),
     svc: IngestionService = Depends(get_ingestion_service)
):
    try:
        if not form_id:
            raise log_and_raise_http(status_code=400, detail="отсутствует обязательный параметр form_id")
        result = await svc.process_files(files, form_id)
        return result
    except HTTPException:
        raise
    except Exception as e:
        log_and_raise_http(500, "Ошибка при обработке файла", e)