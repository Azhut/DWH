"""Шаг: определение структуры таблицы через domain/parsing."""

import logging
from typing import Optional

from app.application.parsing.context import ParsingPipelineContext
from app.application.parsing.steps.base import BaseParsingStep
from app.core.exceptions import CriticalParsingError
from app.core.profiling import profile_step
from app.domain.parsing import (
    AutoDetectStructureStrategy,
    StructureDetectionStrategy,
    detect_table_structure,
)

logger = logging.getLogger(__name__)


class DetectTableStructureStep(BaseParsingStep):
    """
    Определяет структуру таблицы: строки заголовков, начало данных, вертикальную колонку.

    По умолчанию использует AutoDetectStructureStrategy (автоформы).
    Для фиксированных форм (например, 1ФК) стратегия передаётся явным образом.

    Требует:
    - ctx.processed_dataframe — подготовленный DataFrame (NormalizeDataFrameStep);
    - strategy — опционально, если нужно переопределить авто-детекцию.

    Записывает:
    - ctx.table_structure — единственный источник структуры для остальных шагов.
    """

    def __init__(self, strategy: Optional[StructureDetectionStrategy] = None) -> None:
        self._strategy = strategy

    @profile_step()
    async def execute(self, ctx: ParsingPipelineContext) -> None:
        if ctx.processed_dataframe is None:
            raise CriticalParsingError(
                message=(
                    "DetectTableStructureStep: отсутствует подготовленный DataFrame "
                    f"для листа '{ctx.sheet_name}'. "
                    "NormalizeDataFrameStep должен выполняться до DetectTableStructureStep."
                ),
                domain="parsing.steps.detect_structure",
                meta={"sheet_name": ctx.sheet_name},
                show_traceback=False,
            )

        strategy = self._strategy or AutoDetectStructureStrategy()

        try:
            structure = detect_table_structure(
                ctx.processed_dataframe,
                strategy,
                ctx.sheet_name,
            )
        except Exception as e:
            raise CriticalParsingError(
                f"Failed to detect table structure for sheet '{ctx.sheet_name}': {e}",
                domain="parsing.steps.detect_structure",
                meta={"sheet_name": ctx.sheet_name, "error": str(e)},
                show_traceback=True,
            ) from e

        ctx.table_structure = structure

        logger.debug(
            "Structure for '%s': headers [%d:%d], data from %d, vertical_col=%d",
            ctx.sheet_name,
            structure.header_start_row,
            structure.header_end_row,
            structure.data_start_row,
            structure.vertical_header_column,
        )
