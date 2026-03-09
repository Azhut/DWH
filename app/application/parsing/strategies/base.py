"""Базовый контракт стратегии парсинга формы."""
import re
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Callable, Optional

from app.domain.form.models import FormInfo

if TYPE_CHECKING:
    from app.application.parsing.steps.base import ParsingPipelineStep


def normalize_sheet_name(sheet_name: str) -> str:
    """
    Нормализует имя листа к каноническому виду: "РазделN".

    Примеры:
        "Раздел 4"  -> "Раздел4"
        "раздел4"   -> "Раздел4"
        "РАЗДЕЛ 4"  -> "Раздел4"
        "РазДеЛ  4" -> "Раздел4"
        "Р 8-12"    -> "Р 8-12"  (не матчится — возвращается as-is)
    """
    match = re.match(r'^\s*раздел\s*(\d+)\s*$', sheet_name.strip(), re.IGNORECASE)
    if match:
        return f"Раздел{match.group(1)}"
    return sheet_name


class BaseFormParsingStrategy(ABC):
    """
    Абстрактная стратегия парсинга формы.

    Один экземпляр стратегии живёт на уровне формы и используется
    для всех листов файла. Стратегия — единственное место, где
    сосредоточена форма-специфичная логика:

    - Какие листы обрабатывать.
    - Какой набор шагов применить к каждому листу.

    Правила реализации:
    - Стратегия не хранит состояние конкретного файла или листа.
    - Стратегия не знает об UploadPipelineContext.
    - Стратегия не логирует — она только конфигурирует.
    - Форма-специфичные параметры шагов передаются через конструктор шага.
    """

    @abstractmethod
    def should_process_sheet(
        self,
        sheet_name: str,
        sheet_index: int,
        form_info: FormInfo,
    ) -> bool:
        """
        Определяет, нужно ли обрабатывать данный лист.

        Args:
            sheet_name: Название листа из Excel файла (оригинальное).
            sheet_index: Порядковый индекс листа (0-based).
            form_info: Информация о форме, включая реквизиты.

        Returns:
            True — лист нужно обрабатывать.
            False — лист пропускается без ошибки.
        """
        ...

    @abstractmethod
    def build_steps_for_sheet(
        self,
        sheet_name: str,
        form_info: FormInfo,
    ) -> list["ParsingPipelineStep"]:
        """
        Возвращает список шагов для обработки конкретного листа.

        Порядок шагов в списке — порядок выполнения в pipeline.
        Шаги создаются с нужными параметрами через конструктор.

        Args:
            sheet_name: Название листа (оригинальное).
            form_info: Информация о форме.

        Returns:
            Список шагов для ParsingPipelineRunner.

        Raises:
            CriticalParsingError: если для данного листа невозможно
                                  построить корректный pipeline
                                  (например, лист не описан в конфигурации
                                  ручной формы).
        """
        ...


class DefaultFormParsingStrategy(BaseFormParsingStrategy):
    """
    Базовая стратегия с типовым pipeline фаз:

    1. Нормализация имени листа (NormalizeSheetNameStep).
    2. Нормализация DataFrame по строке нумерации 1..n (NormalizeDataFrameStep).
    3. Определение структуры таблицы (DetectTableStructureStep).
    4. Дополнительные форма-специфичные шаги (опционально).
    5. Парсинг заголовков (ParseHeadersStep).
    6. Извлечение данных (ExtractDataStep).
    7. Построение плоских записей (GenerateFlatDataStep).

    Конкретные формы переопределяют только:
    - should_process_sheet(...)
    - get_normalize_sheet_name_fn(...)
    - get_additional_steps_before_headers(...)
    - get_deduplicate_columns(...)
    """

    # --- Расширяемые хуки для форм ---

    def get_normalize_sheet_name_fn(
        self,
        sheet_name: str,
        form_info: FormInfo,
    ) -> Optional[Callable[[str], str]]:
        """
        Возвращает функцию нормализации имени листа или None.

        По умолчанию — без нормализации (sheet_name = sheet_fullname).
        """
        return None

    def get_additional_steps_before_headers(
        self,
        sheet_name: str,
        form_info: FormInfo,
    ) -> list["ParsingPipelineStep"]:
        """
        Дополнительные шаги, выполняемые после DetectTableStructureStep,
        но до ParseHeadersStep.

        Примеры:
        - FK1RoundingStep / ProcessNotesStep для 1ФК.
        """
        return []

    def get_deduplicate_columns(self, form_info: FormInfo) -> bool:
        """
        Нужно ли дедуплицировать колонки при извлечении данных.

        По умолчанию — читается из form_info.requisites["deduplicate_columns"].
        """
        return bool((form_info.requisites or {}).get("deduplicate_columns", False))

    # --- Реализация типового pipeline ---

    def build_steps_for_sheet(
        self,
        sheet_name: str,
        form_info: FormInfo,
    ) -> list["ParsingPipelineStep"]:
        """
        Строит типовой pipeline на основе общих шагов и форма-специфичных хуков.
        """
        from app.application.parsing.steps.common.NormalizeSheetNameStep import (
            NormalizeSheetNameStep,
        )
        from app.application.parsing.steps.common.NormalizeDataFrameStep import (
            NormalizeDataFrameStep,
        )
        from app.application.parsing.steps.common.DetectTableStructureStep import (
            DetectTableStructureStep,
        )
        from app.application.parsing.steps.common.ParseHeadersStep import ParseHeadersStep
        from app.application.parsing.steps.common.ExtractDataStep import ExtractDataStep
        from app.application.parsing.steps.common.GenerateFlatDataStep import (
            GenerateFlatDataStep,
        )

        normalize_fn = self.get_normalize_sheet_name_fn(sheet_name, form_info)
        extra_steps = self.get_additional_steps_before_headers(sheet_name, form_info)
        deduplicate_columns = self.get_deduplicate_columns(form_info)

        steps: list["ParsingPipelineStep"] = [
            NormalizeSheetNameStep(normalize_fn=normalize_fn),
            NormalizeDataFrameStep(),
            DetectTableStructureStep(),
        ]

        steps.extend(extra_steps)
        steps.extend(
            [
                ParseHeadersStep(),
                ExtractDataStep(deduplicate_columns=deduplicate_columns),
                GenerateFlatDataStep(),
            ]
        )

        return steps