import asyncio
import json
from typing import AsyncGenerator

from fastapi import APIRouter, Path, HTTPException, Response, Depends
from fastapi.responses import StreamingResponse

from app.application.upload import UploadManager
from app.core.dependencies import get_upload_manager
from app.core.exceptions import log_and_raise_http

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
    
    async def event_generator():
        """Генератор событий SSE с прогрессом загрузки"""
        
        # Проверяем существование upload_id
        progress = upload_manager.get_upload_progress(upload_id)
        if not progress:
            raise HTTPException(status_code=404, detail="Upload not found")
        
        # Отправляем начальное состояние
        data = {
            "current": progress.current_file,
            "total": progress.total_files,
            "status": progress.status,
            "processed_files": progress.processed_files.copy(),
            "progress_percentage": progress.progress_percentage,
            "errors": progress.errors.copy(),
        }
        yield f"data: {json.dumps(data)}\n\n"
        
        # Продолжаем отправлять обновления пока загрузка не завершится
        while progress.status in ["processing"]:
            await asyncio.sleep(0.5)  # Обновляем каждые 0.5 секунды
            
            # Отправляем обновление только если есть изменения
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
            yield f"data: {json.dumps(data)}\n\n"
            
            # Если загрузка завершена, отправляем финальное состояние и выходим
            if current_progress.status in ["completed", "failed"]:
                data = {
                    "current": current_progress.current_file,
                    "total": current_progress.total_files,
                    "status": current_progress.status,
                    "processed_files": current_progress.processed_files.copy(),
                    "progress_percentage": current_progress.progress_percentage,
                    "errors": current_progress.errors.copy(),
                }
                yield f"data: {json.dumps(data)}\n\n"
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
            Exception(error_msg),
            http_status=500,
            domain="upload_progress.endpoint",
            meta={"upload_id": upload_id, "error": str(e)}
        )
