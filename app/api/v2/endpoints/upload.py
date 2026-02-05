# app/api/v2/endpoints/upload.py
from typing import List
from fastapi import APIRouter, UploadFile, File, Depends, Query
from prometheus_client import Counter

from app.api.v2.schemas.upload import UploadResponse
from app.application.upload import UploadManager
from app.core.dependencies import get_upload_manager
from app.core.exceptions import RequestValidationError, log_and_raise_http

router = APIRouter()
FILE_PROCESSED = Counter('files_processed', 'Total processed files')


@router.post("/upload", response_model=UploadResponse)
async def upload_files(
        files: List[UploadFile] = File(...),
        form_id: str = Query(..., description="ID формы"),
        upload_manager: UploadManager = Depends(get_upload_manager),
):
    """
    Endpoint для загрузки файлов.

    Валидация запроса и обработка файлов делегируются UploadManager.

    Возвращаемые статусы:
    - 400/404 - ошибки валидации запроса (невалидный form_id, нет файлов, форма не найдена)
    - 500 - критическая ошибка на уровне запроса (проблемы с БД)
    - 200 - запрос обработан, детали по каждому файлу в теле ответа

    При статусе 200 в теле ответа details содержит статус каждого файла:
    - success - файл успешно обработан
    - failed - файл обработан с ошибкой (ошибка в поле error)
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