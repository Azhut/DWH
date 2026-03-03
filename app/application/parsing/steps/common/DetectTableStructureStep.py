
"""Шаг: определение структуры таблицы через domain/parsing."""
import logging
from typing import Callable, Optional, Tuple

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

        ctx.table_structure = structure

        logger.debug(
            "Структура для листа '%s' (оригинал: '%s'): заголовки [%d:%d], данные с %d, вертикальная колонка=%d",
            ctx.sheet_name,
            raw_name,
            structure.header_start_row,
            structure.header_end_row,
            structure.data_start_row,
            structure.vertical_header_column,
        )
