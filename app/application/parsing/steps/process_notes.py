"""Шаг: обработка примечаний в листе."""
import logging

from app.application.parsing.context import ParsingPipelineContext
from app.application.parsing.steps.base import ParsingPipelineStep

logger = logging.getLogger(__name__)


class ProcessNotesStep(ParsingPipelineStep):
    """Обрабатывает примечания (notes) в листе Excel."""

    async def execute(self, ctx: ParsingPipelineContext) -> None:
        """Обрабатывает примечания и обновляет dataframe в контексте."""
        if ctx.header_end_row is None:
            ctx.add_error("Не определена структура таблицы перед обработкой примечаний")
            return

        try:
            from app.parsers.notes_processor import NotesProcessor

            processed_df = NotesProcessor.process_notes(
                ctx.raw_dataframe,
                raw_quantity=ctx.header_end_row + 1,  # +1 потому что end_row не включительно
            )
            ctx.processed_dataframe = processed_df

            logger.debug("Обработаны примечания для листа '%s'", ctx.sheet_name)
        except Exception as e:
            error_msg = f"Ошибка обработки примечаний: {str(e)}"
            logger.warning("Не удалось обработать примечания для листа '%s': %s", ctx.sheet_name, e)
            ctx.add_warning(error_msg)
            # Примечания не критичны, продолжаем с исходным dataframe
            ctx.processed_dataframe = ctx.raw_dataframe
