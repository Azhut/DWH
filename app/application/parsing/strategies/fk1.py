"""Стратегия парсинга для формы 1ФК (ручная), поверх общего авто-пайплайна."""
import logging
from dataclasses import dataclass

from app.domain.form.models import FormInfo
from app.application.parsing.strategies.base import DefaultFormParsingStrategy, normalize_sheet_name
from app.application.parsing.steps.base import ParsingPipelineStep

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _SheetConfig:
    """
    Конфигурация листа 1ФК.

    Сейчас хранит только флаг применения округления, но оставлена как dataclass
    на будущее (дополнительные форма-специфичные параметры).
    """

    apply_rounding: bool = False


# Конфигурация всех листов формы 1ФК.
# Ключ — нормализованное имя листа: "РазделN" (без пробела, с заглавной Р).
_SHEET_CONFIGS: dict[str, _SheetConfig] = {
    "Раздел0": _SheetConfig(apply_rounding=False),
    "Раздел1": _SheetConfig(apply_rounding=True),
    "Раздел2": _SheetConfig(apply_rounding=True),
    "Раздел3": _SheetConfig(apply_rounding=True),
    "Раздел4": _SheetConfig(apply_rounding=True),
    "Раздел5": _SheetConfig(apply_rounding=True),
    "Раздел6": _SheetConfig(apply_rounding=True),
    "Раздел7": _SheetConfig(apply_rounding=True),
}


class FK1FormParsingStrategy(DefaultFormParsingStrategy):
    """
    Стратегия парсинга для формы 1ФК.

    Основные отличия от автоформ:
    - используются только заранее известные листы (_SHEET_CONFIGS);
    - имена листов нормализуются к виду "РазделN";
    - для части листов применяется округление (FK1RoundingStep);
    - всегда выполняется обработка примечаний (ProcessNotesStep).

    Структура таблицы для 1ФК теперь определяется автоматикой (по строке нумерации),
    как и для автоформ — фиксированные границы в _SHEET_CONFIGS больше не используются.
    """

    def should_process_sheet(
        self,
        sheet_name: str,
        sheet_index: int,
        form_info: FormInfo,
    ) -> bool:
        normalized = normalize_sheet_name(sheet_name)
        if normalized not in _SHEET_CONFIGS:
            return False

        skip_sheets: list = form_info.requisites.get("skip_sheets", []) or []
        if sheet_index in skip_sheets:
            return False

        return True

    # --- Хуки DefaultFormParsingStrategy ---

    def get_normalize_sheet_name_fn(
        self,
        sheet_name: str,
        form_info: FormInfo,
    ):
        """Для 1ФК всегда нормализуем имена листов в формат 'РазделN'."""

        return normalize_sheet_name

    def get_additional_steps_before_headers(
        self,
        sheet_name: str,
        form_info: FormInfo,
    ) -> list[ParsingPipelineStep]:
        """
        Для 1ФК добавляем:
        - FK1RoundingStep (если apply_rounding=True для листа);
        - ProcessNotesStep — всегда.
        """
        from app.application.parsing.steps.forms.fk1.RoundingStep import FK1RoundingStep
        from app.application.parsing.steps.forms.fk1.ProcessNotesStep import ProcessNotesStep

        normalized = normalize_sheet_name(sheet_name)
        config = _SHEET_CONFIGS.get(normalized, _SHEET_CONFIGS["Раздел0"])

        steps: list[ParsingPipelineStep] = []
        if config.apply_rounding:
            steps.append(FK1RoundingStep())
        steps.append(ProcessNotesStep())
        return steps

    def get_deduplicate_columns(self, form_info: FormInfo) -> bool:
        """Для 1ФК дедупликация колонок не используется."""

        return True

    def get_horizontal_header_strip_fk1_banner(
        self,
        sheet_name: str,
        form_info: FormInfo,
    ) -> bool:
        """
        Для 1ФК дополнительно снимаем ведущий сегмент с «ОКЕИ» (после общего снятия «Раздел»).

        Выключить: ``horizontal_header_strip_fk1_banner``: false в requisites.
        """
        raw = (form_info.requisites or {}).get("horizontal_header_strip_fk1_banner")
        if raw is False:
            return False
        return True

