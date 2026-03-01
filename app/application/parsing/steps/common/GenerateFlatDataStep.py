"""Шаг: генерация FlatDataRecord через domain/parsing."""
import logging

from app.application.parsing.context import ParsingPipelineContext
from app.application.parsing.steps.base import BaseParsingStep
from app.core.exceptions import CriticalParsingError, NonCriticalParsingError
from app.domain.parsing import build_flat_data_records

logger = logging.getLogger(__name__)


class GenerateFlatDataStep(BaseParsingStep):
    """
    Строит список FlatDataRecord из извлечённых данных.

    Требует: ctx.extracted_data — должен быть заполнен ExtractDataStep.
    Записывает: ctx.flat_data_records.

    Отсутствие extracted_data — NonCriticalParsingError: лист пуст,
    но это не повод останавливать весь файл.
    """

    async def execute(self, ctx: ParsingPipelineContext) -> None:
        if ctx.extracted_data is None:
            raise NonCriticalParsingError(
                f"GenerateFlatDataStep: нет извлечённых данных для листа '{ctx.sheet_name}'. "
                "Лист будет пропущен без ошибки.",
                domain="parsing.steps.generate_flat_data",
                meta={"sheet_name": ctx.sheet_name},
            )

        try:
            ctx.flat_data_records = build_flat_data_records(
                ctx.extracted_data,
                year=ctx.file_year,
                reporter=ctx.file_reporter,
                section=ctx.sheet_name,
                file_id=ctx.file_id,
                form_id=ctx.form_id,
                skip_empty=True,
            )
        except Exception as e:
            raise CriticalParsingError(
                f"Ошибка генерации flat_data для листа '{ctx.sheet_name}': {e}",
                domain="parsing.steps.generate_flat_data",
                meta={"sheet_name": ctx.sheet_name, "error": str(e)},
                show_traceback=True,
            ) from e

        logger.debug(
            "Сгенерировано %d записей flat_data для листа '%s'",
            len(ctx.flat_data_records),
            ctx.sheet_name,
        )