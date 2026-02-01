from typing import List
from fastapi import APIRouter, UploadFile, File, Depends, HTTPException, Query
from prometheus_client import Counter

from app.api.v2.schemas.upload import UploadResponse
from app.application.upload import UploadManager
from app.core.dependencies import get_upload_manager, get_form_service
from app.core.exceptions import log_and_raise_http
from app.domain.form.service import FormService

router = APIRouter()
FILE_PROCESSED = Counter('files_processed', 'Total processed files')


@router.post("/upload", response_model=UploadResponse)
async def upload_files(
    files: List[UploadFile] = File(...),
    form_id: str = Query(..., description="ID формы"),
    form_service: FormService = Depends(get_form_service),
    upload_manager: UploadManager = Depends(get_upload_manager),
):
    """Валидация form_id и проверка существования формы — сервис агрегата Form (domain.form)."""
    try:
        await form_service.get_form_or_raise(form_id)
        return await upload_manager.process_files(files, form_id)
    except HTTPException:
        raise
    except Exception as e:
        log_and_raise_http(500, "Ошибка при обработке файла", e)