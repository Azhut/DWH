"""Базовые типы шагов parsing pipeline."""
from abc import ABC, abstractmethod
from typing import Protocol, runtime_checkable

from app.application.parsing.context import ParsingPipelineContext


@runtime_checkable
class ParsingPipelineStep(Protocol):
    """
    Протокол шага parsing pipeline.

    Используется для duck typing — любой объект с методом execute()
    является валидным шагом. Не требует явного наследования.
    """

    async def execute(self, ctx: ParsingPipelineContext) -> None:
        """
        Выполняет шаг парсинга, модифицируя контекст.

        Raises:
            CriticalParsingError: если ошибка делает невозможной
                                  дальнейшую обработку листа.
            NonCriticalParsingError: если возникло предупреждение,
                                     после которого обработку можно продолжить.
        """
        ...


class BaseParsingStep(ABC):
    """
    ABC для шагов, которые хотят использовать наследование.

    Используется когда нужно:
    - Переопределить шаг для конкретной формы (например, FK1RoundingStep).
    - Вынести общую логику в базовый класс.

    Реализует протокол ParsingPipelineStep неявно через метод execute().
    """

    @abstractmethod
    async def execute(self, ctx: ParsingPipelineContext) -> None:
        """
        Выполняет шаг парсинга, модифицируя контекст.

        Raises:
            CriticalParsingError: если ошибка делает невозможной
                                  дальнейшую обработку листа.
            NonCriticalParsingError: если возникло предупреждение,
                                     после которого обработку можно продолжить.
        """
        ...