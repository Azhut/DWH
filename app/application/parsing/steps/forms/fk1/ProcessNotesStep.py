"""Шаг: обработка примечаний (только для 1ФК)."""
import logging

from app.application.parsing.context import ParsingPipelineContext
from app.application.parsing.steps.base import BaseParsingStep
from app.core.exceptions import NonCriticalParsingError

logger = logging.getLogger(__name__)

_FK1_HEADER_ROWS_COUNT = 7


class ProcessNotesStep(BaseParsingStep):
    """
    Обрабатывает примечания (Справочно) в листах формы 1ФК.

    Этот шаг вставляется только в FK1FormParsingStrategy — он не проверяет
    флаги формы, так как сам факт его присутствия в pipeline означает,
    что обработка примечаний нужна.

    Неудача обработки некритична: продолжаем с исходным DataFrame.
    """

    async def execute(self, ctx: ParsingPipelineContext) -> None:
        try:
            from app.domain.parsing import process_notes_1fk
            ctx.processed_dataframe = process_notes_1fk(
                ctx.raw_dataframe,
                _FK1_HEADER_ROWS_COUNT,
            )
            logger.debug(
                "Обработаны примечания для листа '%s' (1ФК)",
                ctx.sheet_name,
            )
        except Exception as e:
            ctx.processed_dataframe = ctx.raw_dataframe
            raise NonCriticalParsingError(
                f"Не удалось обработать примечания для листа '{ctx.sheet_name}': {e}",
                domain="parsing.steps.fk1.process_notes",
                meta={"sheet_name": ctx.sheet_name},
            )