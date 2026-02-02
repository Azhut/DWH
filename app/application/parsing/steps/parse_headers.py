"""Шаг: парсинг заголовков через domain/parsing."""
import logging

from app.application.parsing.context import ParsingPipelineContext
from app.application.parsing.steps.base import ParsingPipelineStep
from app.domain.parsing import parse_headers

logger = logging.getLogger(__name__)


class ParseHeadersStep(ParsingPipelineStep):
    """Парсит горизонтальные и вертикальные заголовки по структуре из контекста."""

    async def execute(self, ctx: ParsingPipelineContext) -> None:
        if ctx.table_structure is None:
            ctx.add_error("Структура таблицы не определена перед парсингом заголовков")
            return
        df = ctx.processed_dataframe if ctx.processed_dataframe is not None else ctx.raw_dataframe
        try:
            result = parse_headers(df, ctx.table_structure)
            ctx.horizontal_headers = result.horizontal
            ctx.vertical_headers = result.vertical
            logger.debug(
                "Заголовки для листа '%s': горизонтальных=%d, вертикальных=%d",
                ctx.sheet_name,
                len(result.horizontal),
                len(result.vertical),
            )
        except Exception as e:
            ctx.add_error(f"Ошибка парсинга заголовков: {e}")
            logger.exception("Ошибка парсинга заголовков для листа '%s'", ctx.sheet_name)
