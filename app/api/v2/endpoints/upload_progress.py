# app/api/v2/endpoints/upload_progress.py
import asyncio
import json

from fastapi import APIRouter, Depends, HTTPException, Path
from fastapi.responses import StreamingResponse

from app.application.upload import UploadManager
from app.application.upload.response_builder import UploadResponseBuilder
from app.core.dependencies import get_upload_manager

router = APIRouter()

_POLL_INTERVAL = 0.1  # секунд


@router.get("/upload-progress/{upload_id}")
async def upload_progress(
        upload_id: str = Path(..., description="ID загрузки, полученный из POST /upload"),
        upload_manager: UploadManager = Depends(get_upload_manager),
):
    """
    SSE-эндпоинт для отслеживания прогресса фоновой обработки файлов.

    Клиент подключается сразу после получения upload_id из POST /upload
    и держит соединение открытым до завершения задачи.

    Формат промежуточных событий:
        {
            "upload_id":          "...",
            "status":             "processing",
            "current":            2,
            "total":              5,
            "progress_percentage": 40.0,
            "processed_files":    ["file1.xlsx", "file2.xlsx"],
            "errors":             []
        }

    Финальное событие (status = "completed" | "failed") дополнительно
    содержит поле "result" с полным UploadResponse — тем самым клиенту
    не нужен отдельный запрос за результатом:
        {
            ...промежуточные поля...,
            "result": {
                "message": "3 files processed successfully, 0 failed.",
                "details": [...],
                "upload_id": "..."
            }
        }

    После отправки финального события соединение закрывается,
    а память задачи освобождается.
    """

    # Проверяем upload_id ДО открытия потока — пока заголовки не отправлены
    # можно вернуть корректный 404.
    if not upload_manager.get_upload_progress(upload_id):
        raise HTTPException(status_code=404, detail=f"Upload '{upload_id}' not found")

    return StreamingResponse(
        _event_generator(upload_id, upload_manager),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",       # отключаем буфер nginx
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "*",
        },
    )


# ---------------------------------------------------------------------------
# Генератор событий (вынесен из эндпоинта для читаемости)
# ---------------------------------------------------------------------------

async def _event_generator(upload_id: str, upload_manager: UploadManager):
    """
    Отправляет SSE-события пока задача не перейдёт в терминальный статус.
    Последним событием отдаёт полный результат обработки.
    """
    last_sent: dict | None = None

    while True:
        progress = upload_manager.get_upload_progress(upload_id)
        if progress is None:
            # Задача уже очищена (race condition при повторном подключении)
            break

        is_terminal = progress.status in ("completed", "failed")

        data: dict = {
            "upload_id": upload_id,
            "status": progress.status,
            "current": progress.current_file,
            "total": progress.total_files,
            "progress_percentage": progress.progress_percentage,
            "processed_files": progress.processed_files.copy(),
            "errors": progress.errors.copy(),
        }

        if is_terminal:
            # Финальное событие: добавляем полный UploadResponse.
            # UploadResponseBuilder.build_response — единственная точка
            # формирования итогового ответа, как и предполагалось изначально.
            final_response = UploadResponseBuilder.build_response(
                file_responses=progress.file_responses,
                upload_id=upload_id,
            )
            data["result"] = final_response.model_dump()

        if data != last_sent:
            yield f"data: {json.dumps(data, ensure_ascii=False)}\n\n"
            last_sent = data

        if is_terminal:
            upload_manager.cleanup_upload_progress(upload_id)
            break

        await asyncio.sleep(_POLL_INTERVAL)