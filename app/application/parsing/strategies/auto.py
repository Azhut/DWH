"""Стратегия парсинга для автоматических форм (5ФК, пользовательские)."""
import logging
import re

from app.domain.form.models import FormInfo
from app.application.parsing.strategies.base import BaseFormParsingStrategy
from app.application.parsing.steps.base import ParsingPipelineStep

logger = logging.getLogger(__name__)

# Принимает: "Раздел0", "Раздел 1", "раздел2", "РАЗДЕЛ 3" и т.д.
_VALID_SHEET_RE = re.compile(r'^\s*раздел\s*\d+\s*$', re.IGNORECASE)


class AutoFormParsingStrategy(BaseFormParsingStrategy):
    """
    Стратегия для автоматических форм: 5ФК и любых форм, созданных пользователем
    через фронтенд.

    Характеристики:
    - Структура таблицы определяется автоматически (AutoDetectStructureStrategy).
    - Один и тот же набор шагов для всех листов файла.
    - Конфигурация читается из form_info.requisites — новый код не нужен.
    - Является стратегией по умолчанию в реестре.
    - Нормализация имён листов: не применяется (sheet_name = sheet_fullname as-is),
      так как автоформы не имеют фиксированной схемы листов.

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
        Пропускает листы с невалидным именем и листы из skip_sheets.

        Валидные имена: "Раздел N" / "РазделN" в любом регистре.
        Листы вроде "Р 8-12", служебные и скрытые листы макросов — пропускаются.
        """
        if not _VALID_SHEET_RE.match(sheet_name):
            return False

        skip_sheets: list = form_info.requisites.get("skip_sheets", []) or []
        if sheet_index in skip_sheets:
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

        normalize_fn не передаётся в DetectTableStructureStep —
        для автоформ sheet_name = sheet_fullname (имена не нормализуются).
        """
        from app.application.parsing.steps.common.DetectTableStructureStep import DetectTableStructureStep
        from app.application.parsing.steps.common.ParseHeadersStep import ParseHeadersStep
        from app.application.parsing.steps.common.ExtractDataStep import ExtractDataStep
        from app.application.parsing.steps.common.GenerateFlatDataStep import GenerateFlatDataStep

        deduplicate_columns: bool = form_info.requisites.get("deduplicate_columns", False)

        return [
            DetectTableStructureStep(auto_detect=True),  # normalize_fn=None -> as-is
            ParseHeadersStep(),
            ExtractDataStep(deduplicate_columns=deduplicate_columns),
            GenerateFlatDataStep(),
        ]