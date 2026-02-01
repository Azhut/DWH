"""
Базовый протокол шага upload pipeline.
Каждый шаг получает контекст и может его изменять или прервать выполнение (raise).
"""
from typing import Protocol, runtime_checkable

from app.services.upload_pipeline.context import UploadPipelineContext


@runtime_checkable
class UploadPipelineStep(Protocol):
    """Шаг pipeline: выполняет одну логическую операцию над контекстом."""

    async def execute(self, ctx: UploadPipelineContext) -> None:
        """
        Выполнить шаг. Может изменять ctx.
        При ошибке — выбросить HTTPException или Exception (pipeline сохранит stub и вернёт FAILED).
        """
        ...
