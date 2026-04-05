from typing import List, Optional
from pydantic import BaseModel, Field
from app.api.v2.schemas.files import FileResponse


class UploadResponse(BaseModel):
    message: str = Field(..., description="Сообщение о результате загрузки")
    details: List[FileResponse] = Field(..., description="Детали по каждому загруженному файлу")
    upload_id: Optional[str] = Field(None, description="ID для отслеживания прогресса (опционально)")


class UploadProgressResponse(BaseModel):
    """Схема ответа для прогресса загрузки (SSE)"""
    current: int = Field(..., description="Номер текущего обрабатываемого файла")
    total: int = Field(..., description="Общее количество файлов в загрузке")
    status: str = Field(..., description="Статус загрузки: processing | completed | failed")
    processed_files: List[str] = Field(..., description="Список уже обработанных файлов")
    progress_percentage: float = Field(..., description="Процент выполнения (0.0 - 100.0)")
    errors: List[str] = Field(..., description="Ошибки по отдельным файлам")
