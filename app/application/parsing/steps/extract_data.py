"""Шаг: извлечение данных из листа."""
import logging

from app.application.parsing.context import ParsingPipelineContext
from app.application.parsing.steps.base import ParsingPipelineStep

logger = logging.getLogger(__name__)


class ExtractDataStep(ParsingPipelineStep):
    """Извлекает структурированные данные из листа."""

    async def execute(self, ctx: ParsingPipelineContext) -> None:
        """Извлекает данные и сохраняет в контекст в формате parsed_data."""
        if ctx.processed_dataframe is None:
            ctx.processed_dataframe = ctx.raw_dataframe

        if not ctx.horizontal_headers or not ctx.vertical_headers:
            ctx.add_error("Не распарсены заголовки перед извлечением данных")
            return

        if ctx.data_start_row is None:
            ctx.add_error("Не определена строка начала данных")
            return

        try:
            # Используем логику из BaseSheetParser
            from app.parsers.base_sheet_parser import BaseSheetParser

            temp_parser = BaseSheetParser(
                header_row_range=(ctx.header_start_row or 0, ctx.header_end_row or 0),
                vertical_header_col=ctx.vertical_header_column or 0,
                start_data_row=ctx.data_start_row,
            )

            # Извлекаем данные
            data = temp_parser.create_data(
                ctx.processed_dataframe,
                ctx.horizontal_headers,
                ctx.vertical_headers,
            )

            # Формируем parsed_data в формате, совместимом со старым API
            ctx.parsed_data = {
                "headers": {
                    "horizontal": ctx.horizontal_headers,
                    "vertical": ctx.vertical_headers,
                },
                "data": data,
                "form_type": ctx.form_info.type.value,
            }

            # Сохраняем данные для SheetModel
            ctx.sheet_model_data = {
                "headers": ctx.parsed_data["headers"],
                "data": ctx.parsed_data["data"],
            }

            logger.debug(
                "Извлечены данные для листа '%s': колонок=%d, строк=%d",
                ctx.sheet_name,
                len(data),
                len(ctx.vertical_headers),
            )
        except Exception as e:
            error_msg = f"Ошибка извлечения данных: {str(e)}"
            logger.exception("Ошибка извлечения данных для листа '%s'", ctx.sheet_name)
            ctx.add_error(error_msg)
