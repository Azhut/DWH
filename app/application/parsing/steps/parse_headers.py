"""Шаг: парсинг заголовков (горизонтальных и вертикальных)."""
import logging

from app.application.parsing.context import ParsingPipelineContext
from app.application.parsing.steps.base import ParsingPipelineStep

logger = logging.getLogger(__name__)


class ParseHeadersStep(ParsingPipelineStep):
    """Парсит горизонтальные и вертикальные заголовки из листа."""

    async def execute(self, ctx: ParsingPipelineContext) -> None:
        """Парсит заголовки и сохраняет в контекст."""
        if ctx.processed_dataframe is None:
            ctx.processed_dataframe = ctx.raw_dataframe

        if ctx.header_start_row is None or ctx.header_end_row is None or ctx.data_start_row is None:
            ctx.add_error("Не определена структура таблицы перед парсингом заголовков")
            return

        try:
            # Используем логику из BaseSheetParser
            from app.parsers.base_sheet_parser import BaseSheetParser

            # Создаём временный парсер для использования его методов
            temp_parser = BaseSheetParser(
                header_row_range=(ctx.header_start_row, ctx.header_end_row),
                vertical_header_col=ctx.vertical_header_column or 0,
                start_data_row=ctx.data_start_row,
            )

            # Парсим заголовки
            horizontal_headers, vertical_headers = temp_parser.parse_headers(ctx.processed_dataframe)

            ctx.horizontal_headers = horizontal_headers
            ctx.vertical_headers = vertical_headers

            logger.debug(
                "Распарсены заголовки для листа '%s': горизонтальных=%d, вертикальных=%d",
                ctx.sheet_name,
                len(horizontal_headers),
                len(vertical_headers),
            )
        except Exception as e:
            error_msg = f"Ошибка парсинга заголовков: {str(e)}"
            logger.exception("Ошибка парсинга заголовков для листа '%s'", ctx.sheet_name)
            ctx.add_error(error_msg)
