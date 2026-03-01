"""Стратегия парсинга для ручной формы 1ФК."""
import logging
from dataclasses import dataclass


from app.domain.form.models import FormInfo
from app.application.parsing.strategies.base import BaseFormParsingStrategy
from app.application.parsing.steps.base import ParsingPipelineStep
from app.core.exceptions import CriticalParsingError

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _SheetConfig:
    """
    Фиксированные параметры структуры одного листа 1ФК.

    Заморожен (frozen=True) — конфигурация не должна меняться в runtime.
    """
    header_row_range: tuple[int, int]   # (start, end) — диапазон строк заголовка
    vertical_header_col: int            # индекс колонки с вертикальными заголовками
    data_start_row: int                 # индекс строки начала данных
    apply_rounding: bool = False        # нужен ли RoundingStep для этого листа


# Конфигурация всех листов формы 1ФК.
# Ключ — точное имя листа в Excel файле.
_SHEET_CONFIGS: dict[str, _SheetConfig] = {
    "Раздел0": _SheetConfig(
        header_row_range=(1, 4),
        vertical_header_col=0,
        data_start_row=5,
        apply_rounding=False,
    ),
    "Раздел1": _SheetConfig(
        header_row_range=(1, 4),
        vertical_header_col=0,
        data_start_row=5,
        apply_rounding=True,
    ),
    "Раздел2": _SheetConfig(
        header_row_range=(1, 4),
        vertical_header_col=0,
        data_start_row=5,
        apply_rounding=True,
    ),
    "Раздел3": _SheetConfig(
        header_row_range=(1, 4),
        vertical_header_col=0,
        data_start_row=5,
        apply_rounding=True,
    ),
    "Раздел4": _SheetConfig(
        header_row_range=(1, 4),
        vertical_header_col=0,
        data_start_row=5,
        apply_rounding=True,
    ),
    "Раздел5": _SheetConfig(
        header_row_range=(1, 4),
        vertical_header_col=0,
        data_start_row=5,
        apply_rounding=True,
    ),
    "Раздел6": _SheetConfig(
        header_row_range=(1, 4),
        vertical_header_col=0,
        data_start_row=5,
        apply_rounding=True,
    ),
    "Раздел7": _SheetConfig(
        header_row_range=(1, 4),
        vertical_header_col=0,
        data_start_row=5,
        apply_rounding=True,
    ),
}


class FK1FormParsingStrategy(BaseFormParsingStrategy):
    """
    Стратегия парсинга для ручной формы 1ФК.

    Характеристики:
    - Фиксированные параметры структуры для каждого листа (_SHEET_CONFIGS).
    - Уникальный шаг ProcessNotesStep — только для этой формы.
    - Шаг RoundingStep применяется только к листам с apply_rounding=True.
    - Листы, не описанные в _SHEET_CONFIGS, вызывают CriticalParsingError.
    - Пропуск листов: только через skip_sheets в реквизитах (как у авто-форм).

    Для добавления нового листа: добавить запись в _SHEET_CONFIGS.
    Для изменения параметров листа: изменить запись в _SHEET_CONFIGS.
    """

    def __init__(self, sheet_service=None) -> None:
        self._sheet_service = sheet_service

    def should_process_sheet(
        self,
        sheet_name: str,
        sheet_index: int,
        form_info: FormInfo,
    ) -> bool:
        """
        Пропускает листы по реквизиту skip_sheets.
        Листы не из _SHEET_CONFIGS не пропускаются здесь — они упадут
        с CriticalParsingError в build_steps_for_sheet, что является
        корректным поведением (неизвестный лист = ошибка конфигурации).
        """
        skip_sheets: list = form_info.requisites.get("skip_sheets", []) or []

        if sheet_index in skip_sheets:
            logger.debug(
                "Лист '%s' (индекс %d) пропущен по реквизиту skip_sheets (1ФК)",
                sheet_name,
                sheet_index,
            )
            return False

        return True

    def build_steps_for_sheet(
        self,
        sheet_name: str,
        form_info: FormInfo,
    ) -> list[ParsingPipelineStep]:
        """
        Собирает шаги для конкретного листа 1ФК.

        Порядок шагов:
        1. DetectTableStructureStep — фиксированные параметры из _SHEET_CONFIGS
        2. FK1RoundingStep          — только если apply_rounding=True для листа
        3. ProcessNotesStep         — специфично для 1ФК, всегда присутствует
        4. ParseHeadersStep
        5. ExtractDataStep
        6. GenerateFlatDataStep

        Raises:
            CriticalParsingError: если лист не описан в _SHEET_CONFIGS.
        """
        from app.application.parsing.steps.common.DetectTableStructureStep import DetectTableStructureStep
        from app.application.parsing.steps.common.ParseHeadersStep import ParseHeadersStep
        from app.application.parsing.steps.common.ExtractDataStep import ExtractDataStep
        from app.application.parsing.steps.common.GenerateFlatDataStep import GenerateFlatDataStep
        from app.application.parsing.steps.forms.fk1.RoundingStep import FK1RoundingStep
        from app.application.parsing.steps.forms.fk1.ProcessNotesStep import ProcessNotesStep


        config = _SHEET_CONFIGS.get(sheet_name)

        if config is None:
            raise CriticalParsingError(
                f"Лист '{sheet_name}' не описан в конфигурации формы 1ФК. "
                f"Известные листы: {list(_SHEET_CONFIGS.keys())}",
                domain="parsing.strategies.fk1",
                meta={
                    "sheet_name": sheet_name,
                    "form_id": form_info.id,
                    "known_sheets": list(_SHEET_CONFIGS.keys()),
                },
            )

        steps: list[ParsingPipelineStep] = [
            DetectTableStructureStep(
                fixed_header_range=config.header_row_range,
                fixed_vertical_col=config.vertical_header_col,
                fixed_data_start_row=config.data_start_row,
            ),
        ]

        if config.apply_rounding:
            if self._sheet_service is None:
                raise CriticalParsingError(
                    f"FK1FormParsingStrategy: sheet_service не передан, "
                    f"но лист '{sheet_name}' требует RoundingStep.",
                    domain="parsing.strategies.fk1",
                    meta={"sheet_name": sheet_name},
                )
            steps.append(FK1RoundingStep(sheet_service=self._sheet_service))

        steps.extend([
            ProcessNotesStep(),
            ParseHeadersStep(),
            ExtractDataStep(),
            GenerateFlatDataStep(),
        ])

        logger.debug(
            "1ФК: собран pipeline для листа '%s' (%d шагов, rounding=%s)",
            sheet_name,
            len(steps),
            config.apply_rounding,
        )

        return steps