"""Базовый протокол шага parsing pipeline."""
from typing import Protocol, runtime_checkable

from app.application.parsing.context import ParsingPipelineContext


@runtime_checkable
class ParsingPipelineStep(Protocol):
    """Протокол шага parsing pipeline."""

    async def execute(self, ctx: ParsingPipelineContext) -> None:
        """Выполняет шаг парсинга, модифицируя контекст."""
        ...
