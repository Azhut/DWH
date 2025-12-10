from typing import List
from fastapi import APIRouter, UploadFile, File, Depends
from app.api.v2.schemas.files import UploadResponse
from app.core.exceptions import log_and_raise_http
from app.services.ingestion_service import IngestionService
from prometheus_client import Counter

router = APIRouter()
FILE_PROCESSED = Counter('files_processed', 'Total processed files')

def get_ingestion_service() -> IngestionService:
    """
    Фабрика зависимости — возвращает текущую реализацию сервиса.
    Тесты могут переопределять именно эту функцию через app.dependency_overrides
    """
    return IngestionService()

@router.post("/upload", response_model=UploadResponse)
async def upload_files(
    files: List[UploadFile] = File(...),
    svc: IngestionService = Depends(get_ingestion_service)
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
        result = await svc.process_files(files)
        return result
    except Exception as e:
        log_and_raise_http(500, "Ошибка при обработке файла", e)

