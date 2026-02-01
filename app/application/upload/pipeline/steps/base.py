"""Базовый протокол шага upload pipeline."""
from typing import Protocol, runtime_checkable

from app.application.upload.pipeline.context import UploadPipelineContext


@runtime_checkable
class UploadPipelineStep(Protocol):
    async def execute(self, ctx: UploadPipelineContext) -> None:
        ...
