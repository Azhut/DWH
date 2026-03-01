"""Стратегия парсинга для автоматических форм (5ФК, пользовательские)."""
import logging

from app.domain.form.models import FormInfo
from app.application.parsing.strategies.base import BaseFormParsingStrategy
from app.application.parsing.steps.base import ParsingPipelineStep

logger = logging.getLogger(__name__)


class AutoFormParsingStrategy(BaseFormParsingStrategy):
    """
    Стратегия для автоматических форм: 5ФК и любых форм, созданных пользователем
    через фронтенд.

    Характеристики:
    - Структура таблицы определяется автоматически (AutoDetectStructureStrategy).
    - Один и тот же набор шагов для всех листов файла.
    - Конфигурация читается из form_info.requisites — новый код не нужен.
    - Является стратегией по умолчанию в реестре.

    Реквизиты формы (form_info.requisites):
    - skip_sheets: list[int] — индексы листов, которые нужно пропустить.
    - deduplicate_columns: bool — дедупликация колонок при извлечении данных.
    """

    def should_process_sheet(
        self,
        sheet_name: str,
        sheet_index: int,
        form_info: FormInfo,
    ) -> bool:
        """
        Пропускает листы, индексы которых указаны в реквизите skip_sheets.
        Все остальные листы обрабатываются.
        """
        skip_sheets: list = form_info.requisites.get("skip_sheets", []) or []

        if sheet_index in skip_sheets:
            logger.debug(
                "Лист '%s' (индекс %d) пропущен по реквизиту skip_sheets формы '%s'",
                sheet_name,
                sheet_index,
                form_info.id,
            )
            return False

        return True

    def build_steps_for_sheet(
        self,
        sheet_name: str,
        form_info: FormInfo,
    ) -> list[ParsingPipelineStep]:
        """
        Стандартный набор шагов для автоматической формы.
        Одинаков для всех листов.
        """
        from app.application.parsing.steps.common.DetectTableStructureStep import DetectTableStructureStep
        from app.application.parsing.steps.common.ParseHeadersStep import ParseHeadersStep
        from app.application.parsing.steps.common.ExtractDataStep import ExtractDataStep
        from app.application.parsing.steps.common.GenerateFlatDataStep import GenerateFlatDataStep

        deduplicate_columns: bool = form_info.requisites.get("deduplicate_columns", False)

        return [
            DetectTableStructureStep(auto_detect=True),
            ParseHeadersStep(),
            ExtractDataStep(deduplicate_columns=deduplicate_columns),
            GenerateFlatDataStep(),
        ]