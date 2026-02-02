"""Шаг: определение структуры таблицы (заголовки, начало данных)."""
import logging
from typing import Optional, Tuple

from app.application.parsing.context import ParsingPipelineContext
from app.application.parsing.steps.base import ParsingPipelineStep

logger = logging.getLogger(__name__)


class DetectTableStructureStep(ParsingPipelineStep):
    """
    Определяет структуру таблицы: начало заголовков, конец заголовков, начало данных.
    
    Для 5ФК: автоматическое определение через эвристики.
    Для 1ФК: использование фиксированных параметров из конфигурации.
    """

    def __init__(
        self,
        fixed_header_range: Optional[Tuple[int, int]] = None,
        fixed_vertical_col: Optional[int] = None,
        fixed_data_start_row: Optional[int] = None,
    ):
        """
        Args:
            fixed_header_range: Фиксированный диапазон строк заголовков (start, end) для 1ФК
            fixed_vertical_col: Фиксированная колонка вертикальных заголовков для 1ФК
            fixed_data_start_row: Фиксированная строка начала данных для 1ФК
        """
        self.fixed_header_range = fixed_header_range
        self.fixed_vertical_col = fixed_vertical_col
        self.fixed_data_start_row = fixed_data_start_row
        self.auto_detect = fixed_header_range is None

    async def execute(self, ctx: ParsingPipelineContext) -> None:
        """Определяет структуру таблицы и сохраняет в контекст."""
        if self.auto_detect:
            await self._auto_detect_structure(ctx)
        else:
            await self._use_fixed_structure(ctx)

    async def _use_fixed_structure(self, ctx: ParsingPipelineContext) -> None:
        """Использует фиксированные параметры структуры (для 1ФК)."""
        if not all([self.fixed_header_range, self.fixed_data_start_row is not None]):
            ctx.add_error("Не указаны фиксированные параметры структуры таблицы")
            return

        ctx.header_start_row = self.fixed_header_range[0]
        ctx.header_end_row = self.fixed_header_range[1]
        ctx.data_start_row = self.fixed_data_start_row
        ctx.vertical_header_column = self.fixed_vertical_col or 0

        logger.debug(
            "Использована фиксированная структура для листа '%s': заголовки [%d:%d], данные с %d",
            ctx.sheet_name,
            ctx.header_start_row,
            ctx.header_end_row,
            ctx.data_start_row,
        )

    async def _auto_detect_structure(self, ctx: ParsingPipelineContext) -> None:
        """
        Автоматически определяет структуру таблицы (для 5ФК).
        Использует логику из FiveFKParser._detect_table_structure.
        """
        # TODO: Вынести логику из FiveFKParser._detect_table_structure сюда
        # Пока используем временную реализацию через существующий парсер
        try:
            # Импортируем логику из существующего парсера
            from app.parsers.five_fk_parser import FiveFKParser

            parser = FiveFKParser(ctx.sheet_name)
            # Вызываем синхронный метод (он не требует async)
            header_start, header_end, data_start = parser._detect_table_structure(ctx.raw_dataframe)

            ctx.header_start_row = header_start
            ctx.header_end_row = header_end
            ctx.data_start_row = data_start

            # Определяем колонку с вертикальными заголовками
            # Используем индекс колонки, а не имя
            vertical_col_name = parser._find_first_non_empty_column(ctx.raw_dataframe.iloc[data_start:])
            if vertical_col_name:
                # Преобразуем имя колонки в индекс
                try:
                    ctx.vertical_header_column = ctx.raw_dataframe.columns.get_loc(vertical_col_name)
                except (KeyError, AttributeError):
                    ctx.vertical_header_column = 0
            else:
                ctx.vertical_header_column = 0

            logger.debug(
                "Автоматически определена структура для листа '%s': заголовки [%d:%d], данные с %d, вертикальная колонка=%d",
                ctx.sheet_name,
                ctx.header_start_row,
                ctx.header_end_row,
                ctx.data_start_row,
                ctx.vertical_header_column,
            )
        except Exception as e:
            error_msg = f"Не удалось определить структуру таблицы: {str(e)}"
            logger.exception("Ошибка автоматического определения структуры для листа '%s'", ctx.sheet_name)
            ctx.add_error(error_msg)
