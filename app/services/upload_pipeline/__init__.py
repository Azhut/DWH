"""
Upload pipeline: единый конвейер обработки файлов, попадающих на эндпоинт upload.
Оркестратор вызывает обработчики (шаги) по порядку; шаги можно менять и расширять.
"""
from app.services.upload_pipeline.context import UploadPipelineContext
from app.services.upload_pipeline.pipeline import (
    UploadPipelineRunner,
    build_default_pipeline,
)

__all__ = [
    "UploadPipelineContext",
    "UploadPipelineRunner",
    "build_default_pipeline",
]
