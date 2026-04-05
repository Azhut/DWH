import asyncio
import json


from fastapi import APIRouter, Path, HTTPException,  Depends
from fastapi.responses import StreamingResponse

from app.application.upload import UploadManager
from app.core.dependencies import get_upload_manager
from app.core.exceptions import log_and_raise_http, AppError

router = APIRouter()


@router.get("/upload-progress/{upload_id}")
async def upload_progress(
        upload_id: str = Path(..., description="ID загрузки для отслеживания прогресса"),
        upload_manager: UploadManager = Depends(get_upload_manager),
):
    """
    Server-Sent Events endpoint для отслеживания прогресса загрузки файлов.

    Отправляет обновления прогресса в реальном времени.

    Формат сообщений:
    {
        "current": 2,
        "total": 5,
        "status": "processing",
        "processed_files": ["file1.xlsx", "file2.xlsx"],
        "progress_percentage": 40.0,
        "errors": []
    }
    """

    # Проверяем существование upload_id ДО создания StreamingResponse,
    # пока заголовки ещё не отправлены и можно вернуть 404
    progress = upload_manager.get_upload_progress(upload_id)
    if not progress:
        raise HTTPException(status_code=404, detail="Upload not found")

    async def event_generator():
        """Генератор событий SSE с прогрессом загрузки"""

        current_progress = upload_manager.get_upload_progress(upload_id)
        if not current_progress:
            return

        # Отправляем начальное состояние
        last_sent_data = None
        data = {
            "current": current_progress.current_file,
            "total": current_progress.total_files,
            "status": current_progress.status,
            "processed_files": current_progress.processed_files.copy(),
            "progress_percentage": current_progress.progress_percentage,
            "errors": current_progress.errors.copy(),
        }
        yield f"data: {json.dumps(data)}\n\n"
        last_sent_data = data

        # Продолжаем отправлять обновления пока загрузка не завершится
        while True:
            await asyncio.sleep(0.1)  # Уменьшил для более быстрой реакции

            current_progress = upload_manager.get_upload_progress(upload_id)
            if not current_progress:
                break

            data = {
                "current": current_progress.current_file,
                "total": current_progress.total_files,
                "status": current_progress.status,
                "processed_files": current_progress.processed_files.copy(),
                "progress_percentage": current_progress.progress_percentage,
                "errors": current_progress.errors.copy(),
            }
            
            # Отправляем только если данные изменились
            if data != last_sent_data:
                yield f"data: {json.dumps(data)}\n\n"
                last_sent_data = data

            if current_progress.status in ["completed", "failed"]:
                break

        # Очищаем память после завершения
        upload_manager.cleanup_upload_progress(upload_id)

    try:
        return StreamingResponse(
            event_generator(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "*",
            }
        )
    except Exception as e:
        error_msg = f"Error creating SSE stream: {str(e)}"
        log_and_raise_http(
            AppError(error_msg),
        )