from fastapi import APIRouter, HTTPException, UploadFile
from typing import List
from app.features.files.services.ingestion_service import IngestionService
from app.api.v1.schemas.files import UploadResponse

router = APIRouter()
ingestion_service = IngestionService()

@router.post("/api/upload", response_model=UploadResponse)
async def upload_files(files: List[UploadFile]):
    """
    Загружает и обрабатывает несколько Excel-файлов.
    Возвращает статус обработки для каждого файла.
    """
    if not files:
        raise HTTPException(status_code=400, detail="Файлы не переданы.")

    try:
        file_models = await ingestion_service.process_files(files)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка при обработке файлов: {str(e)}")

    return await ingestion_service.format_upload_response(file_models)
