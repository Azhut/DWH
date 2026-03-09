"""
Шаг предобработки: нормализует имя листа.

Идея:
- читает sheet_fullname из sheet_model;
- при наличии normalize_fn применяет его и записывает результат в sheet_model.sheet_name;
- если normalize_fn не задан — просто копирует исходное имя.

Стратегии форм могут переопределять логику нормализации, передавая свою функцию.
"""

from typing import Callable, Optional

from app.application.parsing.context import ParsingPipelineContext
from app.application.parsing.steps.base import BaseParsingStep


class NormalizeSheetNameStep(BaseParsingStep):
    """Нормализует имя листа по стратегии формы."""

    def __init__(self, normalize_fn: Optional[Callable[[str], str]] = None) -> None:
        self._normalize_fn = normalize_fn

    async def execute(self, ctx: ParsingPipelineContext) -> None:
        raw_name = ctx.sheet_model.sheet_fullname
        if self._normalize_fn is not None:
            ctx.sheet_model.sheet_name = self._normalize_fn(raw_name)
        else:
            ctx.sheet_model.sheet_name = raw_name

