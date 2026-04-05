# app/api/v2/schemas/upload.py
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from app.api.v2.schemas.files import FileResponse


class UploadResponse(BaseModel):
    """
    Ответ на POST /upload.

    При статусе 202 (фоновая обработка) поле `details` пустое,
    а `upload_id` содержит идентификатор для подключения к SSE-потоку прогресса.

    Финальный `UploadResponse` с заполненным `details` приходит клиенту
    как поле `result` последнего SSE-события в /upload-progress/{upload_id}.
    """

    message: str = Field(
        ...,
        description=(
            "Статусное сообщение. При 202 — ссылка на SSE-эндпоинт прогресса. "
            "При финальном результате — сводка вида '3 processed, 1 failed'."
        ),
        examples=[
            "Upload accepted. Track progress at GET /api/v2/upload-progress/abc-123",
            "3 files processed successfully, 1 failed.",
        ],
    )
    details: List[FileResponse] = Field(
        ...,
        description=(
            "Результаты по каждому файлу. "
            "Пустой список при 202 — заполняется только в финальном SSE-событии."
        ),
    )
    upload_id: Optional[str] = Field(
        None,
        description="UUID задачи загрузки для подключения к SSE-потоку прогресса.",
        examples=["a1b2c3d4-e5f6-7890-abcd-ef1234567890"],
    )


class UploadProgressResponse(BaseModel):
    """
    Схема одного SSE-события из GET /upload-progress/{upload_id}.

    Промежуточные события содержат только поля прогресса.
    Финальное событие (status = 'completed' | 'failed') дополнительно
    содержит поле `result` с полным UploadResponse — клиенту не нужен
    отдельный запрос за результатом.
    """

    upload_id: str = Field(
        ...,
        description="UUID задачи загрузки.",
    )
    status: str = Field(
        ...,
        description="Текущий статус: 'processing' | 'completed' | 'failed'.",
        examples=["processing", "completed", "failed"],
    )
    current: int = Field(
        ...,
        description="Количество уже обработанных файлов.",
    )
    total: int = Field(
        ...,
        description="Общее количество файлов в задаче.",
    )
    progress_percentage: float = Field(
        ...,
        description="Процент выполнения (0.0–100.0).",
        ge=0.0,
        le=100.0,
    )
    processed_files: List[str] = Field(
        ...,
        description="Имена уже обработанных файлов (в порядке завершения).",
    )
    errors: List[str] = Field(
        ...,
        description="Сообщения об ошибках по отдельным файлам.",
    )
    result: Optional[Dict[str, Any]] = Field(
        None,
        description=(
            "Финальный UploadResponse в виде словаря. "
            "Присутствует только в последнем событии (status = 'completed' | 'failed'). "
            "Структура соответствует схеме UploadResponse."
        ),
    )