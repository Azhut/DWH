from fastapi import APIRouter, UploadFile, File, Depends
from typing import List
from app.features.files.services.ingestion_service import IngestionService
from app.api.v1.schemas.files import UploadResponse

router = APIRouter()

@router.post("/upload", response_model=UploadResponse)
async def upload_files(files: List[UploadFile] = File(...), ingestion_service: IngestionService = Depends()):
    return await ingestion_service.process_files(files)
