"""Шаг: определение структуры таблицы через domain/parsing."""
import logging
from typing import Optional, Tuple

from app.application.parsing.context import ParsingPipelineContext
from app.application.parsing.steps.base import ParsingPipelineStep
from app.domain.parsing import (
    TableStructure,
    detect_table_structure,
    FixedStructureStrategy,
    AutoDetectStructureStrategy,
)

logger = logging.getLogger(__name__)


class DetectTableStructureStep(ParsingPipelineStep):
    """
    Определяет структуру таблицы: заголовки, начало данных.
    Использует domain/parsing: фиксированная стратегия (1ФК) или авто (5ФК, универсальный).
    """

    def __init__(
        self,
        fixed_header_range: Optional[Tuple[int, int]] = None,
        fixed_vertical_col: Optional[int] = None,
        fixed_data_start_row: Optional[int] = None,
    ):
        self.fixed_header_range = fixed_header_range
        self.fixed_vertical_col = fixed_vertical_col
        self.fixed_data_start_row = fixed_data_start_row
        self.auto_detect = fixed_header_range is None

    async def execute(self, ctx: ParsingPipelineContext) -> None:
        if self.auto_detect:
            strategy = AutoDetectStructureStrategy()
        else:
            if not all([self.fixed_header_range, self.fixed_data_start_row is not None]):
                ctx.add_error("Не указаны фиксированные параметры структуры таблицы")
                return
            strategy = FixedStructureStrategy(
                header_start_row=self.fixed_header_range[0],
                header_end_row=self.fixed_header_range[1],
                data_start_row=self.fixed_data_start_row,
                vertical_header_column=self.fixed_vertical_col or 0,
            )

        structure = detect_table_structure(ctx.raw_dataframe, strategy, ctx.sheet_name)
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
