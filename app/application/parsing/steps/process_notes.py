"""Шаг: обработка примечаний (только для 1ФК)."""
import logging

from app.application.parsing.context import ParsingPipelineContext
from app.application.parsing.steps.base import ParsingPipelineStep

logger = logging.getLogger(__name__)


class ProcessNotesStep(ParsingPipelineStep):
    """Обрабатывает примечания (Справочно) в листе. Выполняется только если ctx.apply_notes=True (1ФК)."""

    async def execute(self, ctx: ParsingPipelineContext) -> None:
        if not ctx.apply_notes:
            ctx.processed_dataframe = ctx.raw_dataframe
            return
        if ctx.table_structure is None:
            ctx.add_error("Структура таблицы не определена перед обработкой примечаний")
            return
        try:
            from app.domain.parsing import process_notes_1fk
            # Для 1ФК в NotesProcessor зашито 7 строк заголовка (sheet.iloc[7:] — тело)
            header_rows_count = 7
            ctx.processed_dataframe = process_notes_1fk(ctx.raw_dataframe, header_rows_count)
            logger.debug("Обработаны примечания для листа '%s' (1ФК)", ctx.sheet_name)
        except Exception as e:
            logger.warning("Не удалось обработать примечания для листа '%s': %s", ctx.sheet_name, e)
            ctx.processed_dataframe = ctx.raw_dataframe
