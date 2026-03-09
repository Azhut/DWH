
"""Стратегия парсинга для ручной формы 1ФК."""
import logging
from dataclasses import dataclass

from app.domain.form.models import FormInfo
from app.domain.parsing import FixedStructureStrategy
from app.application.parsing.strategies.base import BaseFormParsingStrategy, normalize_sheet_name
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
# Ключ — нормализованное имя листа: "РазделN" (без пробела, с заглавной Р).
_SHEET_CONFIGS: dict[str, _SheetConfig] = {
    "Раздел0": _SheetConfig(header_row_range=(2, 4), vertical_header_col=0, data_start_row=6),
    "Раздел1": _SheetConfig(header_row_range=(2, 4), vertical_header_col=0, data_start_row=6, apply_rounding=True),
    "Раздел2": _SheetConfig(header_row_range=(2, 5), vertical_header_col=0, data_start_row=7, apply_rounding=True),
    "Раздел3": _SheetConfig(header_row_range=(2, 4), vertical_header_col=0, data_start_row=6, apply_rounding=True),
    "Раздел4": _SheetConfig(header_row_range=(2, 5), vertical_header_col=0, data_start_row=7, apply_rounding=True),
    "Раздел5": _SheetConfig(header_row_range=(2, 5), vertical_header_col=0, data_start_row=7, apply_rounding=True),
    "Раздел6": _SheetConfig(header_row_range=(2, 5), vertical_header_col=0, data_start_row=7, apply_rounding=True),
    "Раздел7": _SheetConfig(header_row_range=(2, 2), vertical_header_col=0, data_start_row=4, apply_rounding=True),
}


class FK1FormParsingStrategy(BaseFormParsingStrategy):
    """
    Стратегия парсинга для ручной формы 1ФК.

    Характеристики:
    - Фиксированные параметры структуры для каждого листа (_SHEET_CONFIGS).
    - Нормализация имён: normalize_sheet_name() из base.py передаётся
      в DetectTableStructureStep через normalize_fn — стратегия владеет логикой,
      шаг её исполняет.
    - Уникальный шаг ProcessNotesStep — только для этой формы.
    - Шаг RoundingStep применяется только к листам с apply_rounding=True.
    - Листы, не найденные в _SHEET_CONFIGS после нормализации, пропускаются.
    """


    def should_process_sheet(
        self,
        sheet_name: str,
        sheet_index: int,
        form_info: FormInfo,
    ) -> bool:
        if normalize_sheet_name(sheet_name) not in _SHEET_CONFIGS:
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
        Собирает шаги для конкретного листа 1ФК.

        normalize_sheet_name передаётся в DetectTableStructureStep как normalize_fn —
        именно там происходит запись нормализованного имени в sheet_model.sheet_name.

        Порядок шагов:
        1. DetectTableStructureStep — фиксированные параметры + нормализация имени
        2. FK1RoundingStep          — только если apply_rounding=True
        3. ProcessNotesStep         — специфично для 1ФК, всегда присутствует
        4. ParseHeadersStep
        5. ExtractDataStep
        6. GenerateFlatDataStep
        """
        from app.application.parsing.steps.common.DetectTableStructureStep import DetectTableStructureStep
        from app.application.parsing.steps.common.ParseHeadersStep import ParseHeadersStep
        from app.application.parsing.steps.common.ExtractDataStep import ExtractDataStep
        from app.application.parsing.steps.common.GenerateFlatDataStep import GenerateFlatDataStep
        from app.application.parsing.steps.forms.fk1.RoundingStep import FK1RoundingStep
        from app.application.parsing.steps.forms.fk1.ProcessNotesStep import ProcessNotesStep

        normalized = normalize_sheet_name(sheet_name)
        config = _SHEET_CONFIGS.get(normalized)

        if config is None:
            raise CriticalParsingError(
                f"Лист '{sheet_name}' (нормализован: '{normalized}') не описан "
                f"в конфигурации формы 1ФК. Известные листы: {list(_SHEET_CONFIGS.keys())}",
                domain="parsing.strategies.fk1",
                meta={
                    "sheet_name": sheet_name,
                    "normalized_sheet_name": normalized,
                    "form_id": form_info.id,
                    "known_sheets": list(_SHEET_CONFIGS.keys()),
                },
            )

        steps: list[ParsingPipelineStep] = [
            DetectTableStructureStep(
                strategy=FixedStructureStrategy(
                    header_start_row=config.header_row_range[0],
                    header_end_row=config.header_row_range[1],
                    data_start_row=config.data_start_row,
                    vertical_header_column=config.vertical_header_col,
                ),
                normalize_fn=normalize_sheet_name,  # стратегия передаёт свою логику
            ),
        ]

        if config.apply_rounding:
            steps.append(FK1RoundingStep())

        steps.extend([
            ProcessNotesStep(),
            ParseHeadersStep(),
            ExtractDataStep(),
            GenerateFlatDataStep(),
        ])

        logger.debug(
            "1ФК: собран pipeline для листа '%s' (нормализован: '%s', %d шагов, rounding=%s)",
            sheet_name,
            normalized,
            len(steps),
            config.apply_rounding,
        )

        return steps