"""
Шаг предобработки: нормализует DataFrame по строке нумерации столбцов 1..n.

Идея:
- находим строку, где по ряду столбцов идёт последовательность 1, 2, 3, ..., n;
- считаем эти столбцы единственно валидными и обрезаем всё слева/справа;
- все последующие шаги (структура, заголовки, данные) работают только с обрезанным df.

Отсутствие строки нумерации всегда считается критической ошибкой — дальнейший парсинг
будет некорректным и непредсказуемым.
"""

import logging
from typing import Optional

import pandas as pd

from app.application.parsing.context import ParsingPipelineContext
from app.application.parsing.steps.base import BaseParsingStep
from app.core.exceptions import CriticalParsingError
from app.core.profiling import profile_step
from app.domain.parsing.structure_detection import (
    AutoDetectedTableLayout,
    auto_detect_table_layout,
)

logger = logging.getLogger(__name__)


class NormalizeDataFrameStep(BaseParsingStep):
    """
    Нормализует DataFrame по строке нумерации столбцов (1..n) и сохраняет результат
    в ctx.processed_dataframe.

    Требует: ctx.raw_dataframe.
    Не читает и не меняет ctx.table_structure — это делает DetectTableStructureStep.
    """

    @profile_step()
    async def execute(self, ctx: ParsingPipelineContext) -> None:
        df: pd.DataFrame = ctx.raw_dataframe

        if ctx.sheet_name=='Раздел 2':
            print(1)
        layout: Optional[AutoDetectedTableLayout] = auto_detect_table_layout(
            df,
            sheet_name=ctx.sheet_name,
        )

        if layout is None:
            raise CriticalParsingError(
                message=(
                    f"Не удалось найти строку нумерации столбцов (1..n) на листе '{ctx.sheet_name}'."
                ),
                domain="parsing.steps.normalize_dataframe",
                meta={
                    "sheet_name": ctx.sheet_name,
                    "rows": len(df),
                    "cols": df.shape[1],
                },
                show_traceback=False,
            )

        trimmed_df = df.iloc[:, layout.first_col : layout.last_col + 1].copy()
        ctx.processed_dataframe = trimmed_df

        logger.info(
            "NormalizeDataFrameStep: лист '%s' — обрезка по нумерации: "
            "строка=%d, последовательность=1..%d, столбцы=[%d:%d], исходных=%d, осталось=%d",
            ctx.sheet_name,
            layout.numbering_row,
            layout.sequence_len,
            layout.first_col,
            layout.last_col,
            df.shape[1],
            trimmed_df.shape[1],
        )

