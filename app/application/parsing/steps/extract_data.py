"""Шаг: извлечение данных через domain/parsing."""
import logging

from app.application.parsing.context import ParsingPipelineContext
from app.application.parsing.steps.base import ParsingPipelineStep
from app.domain.parsing import ParsedHeaders, extract_sheet_data

logger = logging.getLogger(__name__)


class ExtractDataStep(ParsingPipelineStep):
    """Извлекает структурированные данные по структуре и заголовкам. Поддерживает дедупликацию колонок (5ФК)."""

    async def execute(self, ctx: ParsingPipelineContext) -> None:
        if ctx.table_structure is None or not ctx.horizontal_headers or not ctx.vertical_headers:
            ctx.add_error("Структура и заголовки должны быть заданы перед извлечением данных")
            return
        df = ctx.processed_dataframe if ctx.processed_dataframe is not None else ctx.raw_dataframe
        headers = ParsedHeaders(horizontal=ctx.horizontal_headers, vertical=ctx.vertical_headers)
        try:
            extracted = extract_sheet_data(
                df,
                ctx.table_structure,
                headers,
                sheet_name=ctx.sheet_name,
                deduplicate_columns=ctx.deduplicate_columns,
            )
            ctx.extracted_data = extracted
            ctx.parsed_data = {
                "headers": {"horizontal": ctx.horizontal_headers, "vertical": ctx.vertical_headers},
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
        except Exception as e:
            ctx.add_error(f"Ошибка извлечения данных: {e}")
            logger.exception("Ошибка извлечения данных для листа '%s'", ctx.sheet_name)
