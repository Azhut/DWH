from typing import List, Optional
from fastapi import APIRouter, UploadFile, File, Depends, Form
from app.api.v2.schemas.files import UploadResponse
from app.core.exceptions import log_and_raise_http
from prometheus_client import Counter
from app.core.dependencies import get_ingestion_service
from app.services.ingestion_service import IngestionService
router = APIRouter()
FILE_PROCESSED = Counter('files_processed', 'Total processed files')


@router.post("/upload", response_model=UploadResponse)
async def upload_files(
    files: List[UploadFile] = File(...),
    form_id: str = Form(...),
    skip_sheets: Optional[List[int]] = Form(None),
    spravochno_keywords: Optional[List[str]] = Form(None),
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
        result = await svc.process_files(files, form_id=form_id, skip_sheets=skip_sheets,
                                         spravochno_keywords=spravochno_keywords)
        return result
    except Exception as e:
        log_and_raise_http(500, "Ошибка при обработке файла", e)
