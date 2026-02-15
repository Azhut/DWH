"""Шаг округления данных для формы 1ФК."""
import logging

from app.application.parsing.context import ParsingPipelineContext
from app.application.parsing.steps.common.RoundingStep import RoundingStep
from app.core.exceptions import NonCriticalParsingError

logger = logging.getLogger(__name__)


class FK1RoundingStep(RoundingStep):
    """
    Округление данных для формы 1ФК.

    Делегирует логику в SheetService.round_dataframe.
    Применяется только к листам с apply_rounding=True в _SHEET_CONFIGS.

    Неудача округления некритична — продолжаем с исходными данными.
    """

    def __init__(self, sheet_service) -> None:
        self._sheet_service = sheet_service

    async def execute(self, ctx: ParsingPipelineContext) -> None:
        try:
            ctx.raw_dataframe = self._sheet_service.round_dataframe(
                ctx.sheet_name,
                ctx.raw_dataframe,
                ctx.form_info,
            )
            logger.debug(
                "Округление применено для листа '%s' (1ФК)",
                ctx.sheet_name,
            )
        except Exception as e:
            raise NonCriticalParsingError(
                f"Не удалось применить округление для листа '{ctx.sheet_name}': {e}",
                domain="parsing.steps.fk1.rounding",
                meta={"sheet_name": ctx.sheet_name},
            )