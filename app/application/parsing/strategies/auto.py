"""Стратегия парсинга для автоматических форм (5ФК, пользовательские)."""
import logging
import re

from app.domain.form.models import FormInfo
from app.application.parsing.strategies.base import DefaultFormParsingStrategy

logger = logging.getLogger(__name__)

# Принимает: "Раздел0", "Раздел 1", "раздел2", "РАЗДЕЛ 3" и т.д.
_VALID_SHEET_RE = re.compile(r"^\s*раздел\s*\d+\s*$", re.IGNORECASE)


class AutoFormParsingStrategy(DefaultFormParsingStrategy):
    """
    Стратегия для автоматических форм: 5ФК и любых форм, созданных пользователем
    через фронтенд.

    Базируется на DefaultFormParsingStrategy и использует общий pipeline:
    1) NormalizeSheetNameStep (без нормализации имён для автоформ);
    2) NormalizeDataFrameStep (по строке нумерации 1..n, критично для всех листов);
    3) DetectTableStructureStep (автоматическая стратегия структуры);
    4) ParseHeadersStep;
    5) ExtractDataStep (с возможной дедупликацией колонок);
    6) GenerateFlatDataStep.
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

