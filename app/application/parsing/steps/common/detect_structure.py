"""Шаг: определение структуры таблицы через domain/parsing."""
import logging
from typing import Optional, Tuple

from app.application.parsing.context import ParsingPipelineContext
from app.application.parsing.steps.base import BaseParsingStep
from app.core.exceptions import CriticalParsingError
from app.domain.parsing import (
    detect_table_structure,
    FixedStructureStrategy,
    AutoDetectStructureStrategy,
)

logger = logging.getLogger(__name__)


class DetectTableStructureStep(BaseParsingStep):
    """
    Определяет структуру таблицы: заголовки, начало данных.

    Два режима — задаётся через конструктор, не через контекст:
    - auto_detect=True: AutoDetectStructureStrategy (автоматические формы).
    - fixed params: FixedStructureStrategy (ручные формы, например 1ФК).
    """

    def __init__(
        self,
        auto_detect: bool = False,
        fixed_header_range: Optional[Tuple[int, int]] = None,
        fixed_vertical_col: Optional[int] = None,
        fixed_data_start_row: Optional[int] = None,
    ) -> None:
        self._auto_detect = auto_detect
        self._fixed_header_range = fixed_header_range
        self._fixed_vertical_col = fixed_vertical_col
        self._fixed_data_start_row = fixed_data_start_row

    async def execute(self, ctx: ParsingPipelineContext) -> None:
        if self._auto_detect:
            strategy = AutoDetectStructureStrategy()
        else:
            if self._fixed_header_range is None or self._fixed_data_start_row is None:
                raise CriticalParsingError(
                    "DetectTableStructureStep: не указаны фиксированные параметры структуры. "
                    "Передайте fixed_header_range и fixed_data_start_row в конструктор.",
                    domain="parsing.steps.detect_structure",
                    meta={"sheet_name": ctx.sheet_name},
                )
            strategy = FixedStructureStrategy(
                header_start_row=self._fixed_header_range[0],
                header_end_row=self._fixed_header_range[1],
                data_start_row=self._fixed_data_start_row,
                vertical_header_column=self._fixed_vertical_col or 0,
            )

        try:
            structure = detect_table_structure(
                ctx.raw_dataframe,
                strategy,
                ctx.sheet_name,
            )
        except Exception as e:
            raise CriticalParsingError(
                f"Не удалось определить структуру таблицы листа '{ctx.sheet_name}': {e}",
                domain="parsing.steps.detect_structure",
                meta={"sheet_name": ctx.sheet_name, "error": str(e)},
                show_traceback=True,
            ) from e

        ctx.table_structure = structure
        ctx.header_start_row = structure.header_start_row
        ctx.header_end_row = structure.header_end_row
        ctx.data_start_row = structure.data_start_row
        ctx.vertical_header_column = structure.vertical_header_column

        logger.debug(
            "Структура для листа '%s': заголовки [%d:%d], данные с %d, вертикальная колонка=%d",
            ctx.sheet_name,
            structure.header_start_row,
            structure.header_end_row,
            structure.data_start_row,
            structure.vertical_header_column,
        )