"""Шаг: генерация FlatDataRecord через domain/parsing."""
import logging

from app.application.parsing.context import ParsingPipelineContext
from app.application.parsing.steps.base import ParsingPipelineStep
from app.domain.parsing import build_flat_data_records

logger = logging.getLogger(__name__)


class GenerateFlatDataStep(ParsingPipelineStep):
    """Строит список FlatDataRecord из извлечённых данных."""

    async def execute(self, ctx: ParsingPipelineContext) -> None:
        if ctx.extracted_data is None:
            ctx.add_warning("Нет извлечённых данных для генерации flat_data")
            return
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
            logger.debug(
                "Сгенерировано %d записей flat_data для листа '%s'",
                len(ctx.flat_data_records),
                ctx.sheet_name,
            )
        except Exception as e:
            ctx.add_error(f"Ошибка генерации flat_data: {e}")
            logger.exception("Ошибка генерации flat_data для листа '%s'", ctx.sheet_name)
