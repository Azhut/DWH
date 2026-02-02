"""Шаг: генерация flat_data записей из распарсенных данных."""
import logging
import math

from app.application.parsing.context import ParsingPipelineContext
from app.application.parsing.steps.base import ParsingPipelineStep
from app.domain.flat_data.models import FlatDataRecord

logger = logging.getLogger(__name__)


class GenerateFlatDataStep(ParsingPipelineStep):
    """Генерирует FlatDataRecord из распарсенных данных."""

    async def execute(self, ctx: ParsingPipelineContext) -> None:
        """Генерирует flat_data записи и сохраняет в контекст."""
        if not ctx.parsed_data or not ctx.parsed_data.get("data"):
            ctx.add_warning("Нет данных для генерации flat_data")
            return

        try:
            from app.parsers.notes_processor import _SERVICE_EMPTY

            flat_records: list[FlatDataRecord] = []
            data = ctx.parsed_data["data"]

            for column in data:
                column_header = column.get("column_header", "")
                values = column.get("values", [])

                for row in values:
                    if row.get("value") == _SERVICE_EMPTY:
                        continue

                    value = row.get("value", 0)
                    if isinstance(value, float) and math.isnan(value):
                        value = 0

                    flat_record = FlatDataRecord(
                        year=ctx.file_year,
                        city=ctx.file_city,
                        section=ctx.sheet_name,
                        row=row.get("row_header", ""),
                        column=column_header,
                        value=value,
                        file_id=ctx.file_id,
                        form=ctx.form_id,
                    )
                    flat_records.append(flat_record)

            ctx.flat_data_records = flat_records

            logger.debug(
                "Сгенерировано %d записей flat_data для листа '%s'",
                len(flat_records),
                ctx.sheet_name,
            )
        except Exception as e:
            error_msg = f"Ошибка генерации flat_data: {str(e)}"
            logger.exception("Ошибка генерации flat_data для листа '%s'", ctx.sheet_name)
            ctx.add_error(error_msg)
