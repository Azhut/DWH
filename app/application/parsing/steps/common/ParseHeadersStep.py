"""Шаг: парсинг заголовков через domain/parsing."""
import logging

from app.application.parsing.context import ParsingPipelineContext
from app.application.parsing.steps.base import BaseParsingStep
from app.core.exceptions import CriticalParsingError
from app.domain.parsing import parse_headers

logger = logging.getLogger(__name__)


class ParseHeadersStep(BaseParsingStep):
    """
    Парсит горизонтальные и вертикальные заголовки по структуре из контекста.

    Требует: ctx.table_structure — должен быть заполнен DetectTableStructureStep.
    Записывает: ctx.horizontal_headers, ctx.vertical_headers.
    """

    async def execute(self, ctx: ParsingPipelineContext) -> None:
        if ctx.table_structure is None:
            raise CriticalParsingError(
                f"ParseHeadersStep: структура таблицы не определена для листа '{ctx.sheet_name}'. "
                "DetectTableStructureStep должен выполняться перед ParseHeadersStep.",
                domain="parsing.steps.parse_headers",
                meta={"sheet_name": ctx.sheet_name},
            )

        df = ctx.processed_dataframe if ctx.processed_dataframe is not None else ctx.raw_dataframe

        try:
            result = parse_headers(df, ctx.table_structure)
        except Exception as e:
            raise CriticalParsingError(
                f"Ошибка парсинга заголовков листа '{ctx.sheet_name}': {e}",
                domain="parsing.steps.parse_headers",
                meta={"sheet_name": ctx.sheet_name, "error": str(e)},
                show_traceback=True,
            ) from e

        ctx.horizontal_headers = result.horizontal
        ctx.vertical_headers = result.vertical

        logger.debug(
            "Заголовки для листа '%s': горизонтальных=%d, вертикальных=%d",
            ctx.sheet_name,
            len(result.horizontal),
            len(result.vertical),
        )