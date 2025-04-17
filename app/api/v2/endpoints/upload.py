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
    """
    Загружает файлы на сервер для обработки.

    **Пример запроса:**
    - Файлы передаются в теле запроса в формате `multipart/form-data`.

    **Пример ответа:**
    ```json
    {
        "message": "2 files processed successfully, 0 failed.",
        "details": [
            {
                "filename": "АЛАПАЕВСК 2020.xlsx",
                "status": "Success",
                "error": ""
            },
            {
                "filename": "ИРБИТ 2023.xls",
                "status": "Success",
                "error": ""
            }
        ]
    }
    ```

    **Коды ответа:**
    - `200 OK`: Успешная загрузка файлов.
    - `500 Internal Server Error`: Ошибка сервера при обработке файлов.
    """
    try:
        result = await ingestion_service.process_files(files)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка загрузки файлов: {str(e)}")
