from typing import Protocol, runtime_checkable

from app.application.upload.pipeline.context import UploadPipelineContext
from app.core.profiling import profile_step


@runtime_checkable
class UploadPipelineStep(Protocol):
    """Базовый протокол шага upload pipeline."""

    @profile_step()
    async def execute(self, ctx: UploadPipelineContext) -> None:
        ...
