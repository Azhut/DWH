"""Базовый контракт стратегии парсинга формы."""
import re
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

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