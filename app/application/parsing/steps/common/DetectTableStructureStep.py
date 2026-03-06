
"""Шаг: определение структуры таблицы через domain/parsing."""
import logging
from typing import Callable, Optional, Tuple

import pandas as pd

from app.application.parsing.context import ParsingPipelineContext
from app.application.parsing.steps.base import BaseParsingStep
from app.core.exceptions import CriticalParsingError
from app.domain.parsing import (
    detect_table_structure,
    FixedStructureStrategy,
    AutoDetectStructureStrategy, TableStructure,
)

logger = logging.getLogger(__name__)


class DetectTableStructureStep(BaseParsingStep):
    """
    Определяет структуру таблицы: заголовки, начало данных.

    Два режима — задаётся через конструктор, не через контекст:
    - auto_detect=True: AutoDetectStructureStrategy (автоматические формы).
    - fixed params: FixedStructureStrategy (ручные формы, например 1ФК).

    Нормализация имени листа:
    Этот шаг первым получает управление в pipeline и отвечает за заполнение
    sheet_model.sheet_name. Логика нормализации инкапсулирована в стратегиях
    и передаётся сюда через параметр normalize_fn.
    Если normalize_fn не передан — sheet_name = sheet_fullname (as-is).
    """

    def __init__(
        self,
        auto_detect: bool = False,
        fixed_header_range: Optional[Tuple[int, int]] = None,
        fixed_vertical_col: Optional[int] = None,
        fixed_data_start_row: Optional[int] = None,
        normalize_fn: Optional[Callable[[str], str]] = None,
    ) -> None:
        """
        Args:
            auto_detect: Использовать автоматическое определение структуры.
            fixed_header_range: (start, end) строки заголовка (для ручных форм).
            fixed_vertical_col: Индекс колонки с вертикальными заголовками.
            fixed_data_start_row: Индекс строки начала данных.
            normalize_fn: Callable[[str], str] для нормализации имени листа.
                Передаётся стратегией формы. Если None — имя не нормализуется.
        """
        self._auto_detect = auto_detect
        self._fixed_header_range = fixed_header_range
        self._fixed_vertical_col = fixed_vertical_col
        self._fixed_data_start_row = fixed_data_start_row
        self._normalize_fn = normalize_fn

    async def execute(self, ctx: ParsingPipelineContext) -> None:
        # 1. Нормализуем имя листа и записываем в sheet_model
        raw_name = ctx.sheet_model.sheet_fullname
        ctx.sheet_model.sheet_name = (
            self._normalize_fn(raw_name) if self._normalize_fn is not None else raw_name
        )

        # 2. Выбираем стратегию определения структуры
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

        # 3. Определяем структуру таблицы
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

        df = ctx.processed_dataframe if ctx.processed_dataframe is not None else ctx.raw_dataframe
        df = self._trim_dataframe_by_header_zone(df, structure, ctx.sheet_name)


        ctx.processed_dataframe = df

        ctx.table_structure = structure

        logger.debug(
            "Структура для листа '%s': заголовки [%d:%d], данные с %d, вертикальная колонка=%d, колонок после обрезки=%d",
            ctx.sheet_name,
            structure.header_start_row,
            structure.header_end_row,
            structure.data_start_row,
            structure.vertical_header_column,
            df.shape[1],
        )

    def _trim_dataframe_by_header_zone(
            self,
            df: pd.DataFrame,
            structure: TableStructure,
            sheet_name: str
    ) -> pd.DataFrame:

        if df.empty:
            return df

        header_start = structure.header_start_row
        header_end = structure.header_end_row

        header_zone = df.iloc[header_start:header_end + 1]

        # Есть ли хотя бы одно значение в колонке заголовков
        mask = header_zone.notna().any(axis=0)

        # Преобразуем в список bool
        mask_list = mask.tolist()

        # ищем последний True
        last_valid = -1
        for i, v in enumerate(mask_list):
            if v:
                last_valid = i

        if last_valid == -1:
            logger.warning(
                "Не найдено ни одного столбца заголовков на листе '%s'",
                sheet_name,
            )
            return df

        # проверяем артефакты: True -> False -> True
        seen_empty = False
        first_empty = None

        for i, v in enumerate(mask_list):
            if not v:
                if not seen_empty and i > last_valid:
                    break
                if not seen_empty:
                    first_empty = i
                seen_empty = True

            if v and seen_empty:
                raise CriticalParsingError(
                    message=(
                        f"Нарушена структура заголовков на листе '{sheet_name}'. "
                        f"Столбец {first_empty} пустой, но столбец {i} содержит данные."
                    ),
                    domain="parsing.steps.detect_structure.trim",
                    meta={
                        "sheet_name": sheet_name,
                        "header_range": f"[{header_start}:{header_end}]",
                        "first_empty_column": first_empty,
                        "next_non_empty_column": i,
                        "mask": mask_list,
                    },
                )

        trimmed_cols = last_valid + 1

        if trimmed_cols < df.shape[1]:
            logger.info(
                "Обрезка листа '%s': %d → %d колонок",
                sheet_name,
                df.shape[1],
                trimmed_cols,
            )
            return df.iloc[:, :trimmed_cols].copy()

        return df
