# app/api/v2/endpoints/upload.py
from typing import List

from fastapi import APIRouter, Depends, File, Query, UploadFile
from prometheus_client import Counter

from app.api.v2.schemas.upload import UploadResponse
from app.application.upload import UploadManager
from app.core.dependencies import get_upload_manager
from app.core.exceptions import RequestValidationError, log_and_raise_http

router = APIRouter()
FILE_PROCESSED = Counter("files_processed", "Total processed files")


@router.post(
    "/upload",
    response_model=UploadResponse,
    status_code=202,             # файлы приняты, обработка идёт в фоне
    summary="Загрузить файлы",
)
async def upload_files(
        files: List[UploadFile] = File(...),
        form_id: str = Query(..., description="ID формы"),
        upload_manager: UploadManager = Depends(get_upload_manager),
):
    """
    Принимает файлы, запускает фоновую обработку и немедленно
    возвращает **202 Accepted** с `upload_id`.

    Клиент должен подключиться к `GET /upload-progress/{upload_id}` (SSE),
    чтобы отслеживать прогресс и получить финальный результат последним
    событием потока.

    Возвращаемые статусы:
    - **202** — файлы приняты, обработка запущена в фоне
    - **400 / 404** — ошибки валидации (невалидный form_id, нет файлов,
                       форма не найдена)
    - **500** — критическая ошибка на уровне запроса
    """
    try:
        return await upload_manager.upload_files(files, form_id)

    except RequestValidationError as e:
        log_and_raise_http(e)

    except Exception as e:
        error = RequestValidationError(
            message="Внутренняя ошибка сервера при обработке запроса",
            http_status=500,
            domain="upload.endpoint",
            meta={"form_id": form_id, "error": str(e)},
        )
        log_and_raise_http(error)