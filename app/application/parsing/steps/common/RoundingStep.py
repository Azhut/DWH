"""Шаг округления данных — базовый контракт.

Общей реализации округления нет, так как логика зависит от конкретной формы.
Для переопределения: унаследоваться от RoundingStep в директории forms/<form_name>/.

Пример: app.application.parsing.steps.forms.fk1.rounding.FK1RoundingStep
"""
from app.application.parsing.context import ParsingPipelineContext
from app.application.parsing.steps.base import BaseParsingStep
from app.core.exceptions import CriticalParsingError


class RoundingStep(BaseParsingStep):
    """
    Базовый шаг округления.

    Не содержит реализации — служит контрактом для форма-специфичных
    переопределений. Прямое использование вызовет CriticalParsingError.
    """

    async def execute(self, ctx: ParsingPipelineContext) -> None:
        raise CriticalParsingError(
            "RoundingStep используется напрямую без реализации. "
            "Используйте форма-специфичный шаг, например FK1RoundingStep.",
            domain="parsing.steps.rounding",
            meta={"sheet_name": ctx.sheet_name},
        )