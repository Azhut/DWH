"""Шаг: извлечение данных через domain/parsing."""
import logging

from app.application.parsing.context import ParsingPipelineContext
from app.application.parsing.steps.base import BaseParsingStep
from app.core.exceptions import CriticalParsingError
from app.domain.parsing import ParsedHeaders, extract_sheet_data

logger = logging.getLogger(__name__)


class ExtractDataStep(BaseParsingStep):
    """
    Извлекает структурированные данные по структуре и заголовкам.

    Требует: ctx.table_structure, ctx.horizontal_headers, ctx.vertical_headers.
    Записывает: ctx.extracted_data, ctx.parsed_data, ctx.sheet_model_data.

    Args:
        deduplicate_columns: Дедупликация колонок при извлечении.
            Передаётся через конструктор стратегией формы,
            а не читается из контекста.
    """

    def __init__(self, deduplicate_columns: bool = False) -> None:
        self._deduplicate_columns = deduplicate_columns

    async def execute(self, ctx: ParsingPipelineContext) -> None:
        if ctx.table_structure is None:
            raise CriticalParsingError(
                f"ExtractDataStep: структура таблицы не определена для листа '{ctx.sheet_name}'.",
                domain="parsing.steps.extract_data",
                meta={"sheet_name": ctx.sheet_name},
            )

        if not ctx.horizontal_headers or not ctx.vertical_headers:
            raise CriticalParsingError(
                f"ExtractDataStep: заголовки не заполнены для листа '{ctx.sheet_name}'. "
                "ParseHeadersStep должен выполняться перед ExtractDataStep.",
                domain="parsing.steps.extract_data",
                meta={
                    "sheet_name": ctx.sheet_name,
                    "horizontal_headers_count": len(ctx.horizontal_headers),
                    "vertical_headers_count": len(ctx.vertical_headers),
                },
            )

        df = ctx.processed_dataframe if ctx.processed_dataframe is not None else ctx.raw_dataframe
        headers = ParsedHeaders(
            horizontal=ctx.horizontal_headers,
            vertical=ctx.vertical_headers,
        )

        try:
            extracted = extract_sheet_data(
                df,
                ctx.table_structure,
                headers,
                sheet_name=ctx.sheet_name,
                deduplicate_columns=self._deduplicate_columns,
            )
        except Exception as e:
            raise CriticalParsingError(
                f"Ошибка извлечения данных листа '{ctx.sheet_name}': {e}",
                domain="parsing.steps.extract_data",
                meta={"sheet_name": ctx.sheet_name, "error": str(e)},
                show_traceback=True,
            ) from e

        ctx.extracted_data = extracted
        ctx.parsed_data = {
            "headers": {
                "horizontal": ctx.horizontal_headers,
                "vertical": ctx.vertical_headers,
            },
            "data": extracted.to_legacy_format(),
            "form_type": ctx.form_info.type.value,
        }
        ctx.sheet_model_data = {
            "headers": ctx.parsed_data["headers"],
            "data": ctx.parsed_data["data"],
        }

        logger.debug(
            "Извлечены данные для листа '%s': колонок=%d",
            ctx.sheet_name,
            len(extracted.columns),
        )