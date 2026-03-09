"""
Шаг пайплайна: определить структуру таблицы и подготовить DataFrame к парсингу.

По умолчанию используется автоматическая детекция по строке нумерации столбцов (1..n):
- обрезаем все "лишние" столбцы вне диапазона [1..n]
- всё выше строки нумерации считаем заголовками
- всё ниже — данными

Стратегии форм могут переопределять способ детекции, передав свою стратегию:
- 1ФК: фиксированная структура (без поиска)
- 5ФК: автоматическая (по умолчанию)
"""

import logging
from typing import Callable, Optional, Tuple

import pandas as pd

from app.application.parsing.context import ParsingPipelineContext
from app.application.parsing.steps.base import BaseParsingStep
from app.core.exceptions import CriticalParsingError
from app.domain.parsing import (
    AutoDetectStructureStrategy,
    StructureDetectionStrategy,
    TableStructure,
    detect_table_structure,
)
from app.domain.parsing.structure_detection import AutoDetectedTableLayout, auto_detect_table_layout

logger = logging.getLogger(__name__)


class DetectTableStructureStep(BaseParsingStep):
    """
    Определяет границы таблицы: строки заголовков, начало данных и вертикальную колонку.

    По умолчанию работает в автоматическом режиме (AutoDetectStructureStrategy).
    Для фиксированных форм (например, 1ФК) стратегия передаётся явно.
    """

    def __init__(
        self,
        strategy: Optional[StructureDetectionStrategy] = None,
        normalize_fn: Optional[Callable[[str], str]] = None,
    ) -> None:
        self._strategy = strategy
        self._normalize_fn = normalize_fn

    async def execute(self, ctx: ParsingPipelineContext) -> None:
        raw_name = ctx.sheet_model.sheet_fullname
        ctx.sheet_model.sheet_name = (
            self._normalize_fn(raw_name) if self._normalize_fn is not None else raw_name
        )

        strategy = self._strategy or AutoDetectStructureStrategy()

        try:
            structure = detect_table_structure(
                ctx.raw_dataframe,
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

        df = ctx.processed_dataframe if ctx.processed_dataframe is not None else ctx.raw_dataframe

        df, structure = self._trim_and_refine(df, structure, strategy, ctx.sheet_name)

        ctx.processed_dataframe = df
        ctx.table_structure = structure

        logger.debug(
            "Structure for '%s': headers [%d:%d], data from %d, vertical_col=%d, cols_after_trim=%d",
            ctx.sheet_name,
            structure.header_start_row,
            structure.header_end_row,
            structure.data_start_row,
            structure.vertical_header_column,
            df.shape[1],
        )

    def _trim_and_refine(
        self,
        df: pd.DataFrame,
        structure: TableStructure,
        strategy: StructureDetectionStrategy,
        sheet_name: str,
    ) -> Tuple[pd.DataFrame, TableStructure]:
        """
        Подготавливает DataFrame под последующие шаги (парсинг заголовков и данных).

        - для авто-детекции: обрезаем столбцы по строке нумерации 1..n и уточняем структуру
        - для фиксированной структуры: обрезаем «хвост» пустых столбцов по зоне заголовков
        """
        if df.empty:
            return df, structure

        if isinstance(strategy, AutoDetectStructureStrategy):
            layout: Optional[AutoDetectedTableLayout] = auto_detect_table_layout(df, sheet_name=sheet_name)
            if layout is None:
                trimmed = self._trim_dataframe_by_header_zone(df, structure, sheet_name)
                return trimmed, structure

            trimmed_df = df.iloc[:, layout.first_col : layout.last_col + 1].copy()
            return trimmed_df, layout.structure

        trimmed = self._trim_dataframe_by_header_zone(df, structure, sheet_name)
        return trimmed, structure

    def _trim_dataframe_by_header_zone(
        self,
        df: pd.DataFrame,
        structure: TableStructure,
        sheet_name: str,
    ) -> pd.DataFrame:
        if df.empty:
            return df

        header_start = structure.header_start_row
        header_end = structure.header_end_row
        header_zone = df.iloc[header_start : header_end + 1]
        mask_list = header_zone.notna().any(axis=0).tolist()

        first_valid = -1
        last_valid = -1
        for i, v in enumerate(mask_list):
            if v and first_valid == -1:
                first_valid = i
            if v:
                last_valid = i

        if last_valid == -1:
            logger.warning("No header columns found in sheet '%s'", sheet_name)
            return df

        seen_empty = False
        first_empty = None

        for i in range(first_valid, last_valid + 1):
            v = mask_list[i]
            if not v:
                if not seen_empty:
                    first_empty = i
                seen_empty = True
            if v and seen_empty:
                logger.warning(
                    "Header mask has gaps in '%s' (first_empty=%s, next_non_empty=%d). Continue with best-effort trim.",
                    sheet_name,
                    first_empty,
                    i,
                )
                break

        trimmed_cols = last_valid + 1

        if trimmed_cols < df.shape[1]:
            logger.info(
                "Header-zone trim for '%s': %d -> %d columns",
                sheet_name,
                df.shape[1],
                trimmed_cols,
            )
            return df.iloc[:, :trimmed_cols].copy()

        return df
