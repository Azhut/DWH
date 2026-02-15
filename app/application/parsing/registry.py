"""Реестр стратегий parsing pipeline."""
import logging
from typing import Optional

from app.domain.form.models import FormInfo, FormType
from app.application.parsing.strategies.base import BaseFormParsingStrategy

logger = logging.getLogger(__name__)


class ParsingStrategyRegistry:
    """
    Реестр стратегий парсинга.

    Связывает FormType с конкретной реализацией BaseFormParsingStrategy.
    Ручные формы регистрируются явно в _register_manual_forms().
    Для всего остального возвращается AutoFormParsingStrategy по умолчанию.
    """

    def __init__(self, sheet_service=None) -> None:
        """
        Args:
            sheet_service: SheetService для шагов, требующих доменных операций
                           (например, FK1RoundingStep). Передаётся в стратегии
                           ручных форм при регистрации.
        """
        self._strategies: dict[FormType, BaseFormParsingStrategy] = {}
        self._default: Optional[BaseFormParsingStrategy] = None
        self._sheet_service = sheet_service
        self._init()

    def _init(self) -> None:
        self._init_default()
        self._register_manual_forms()

    def _init_default(self) -> None:
        from app.application.parsing.strategies.auto import AutoFormParsingStrategy
        self._default = AutoFormParsingStrategy()
        logger.debug("Стратегия по умолчанию: AutoFormParsingStrategy")

    def _register_manual_forms(self) -> None:
        """
        Регистрирует все ручные формы.

        Единственное место, где нужно что-то добавить при появлении
        новой ручной формы (помимо создания файла стратегии).
        """
        from app.application.parsing.strategies.fk1 import FK1FormParsingStrategy
        self.register(FormType.FK_1, FK1FormParsingStrategy(
            sheet_service=self._sheet_service,
        ))
        # Будущие ручные формы:
        # from app.application.parsing.strategies.fk3 import FK3FormParsingStrategy
        # self.register(FormType.FK_3, FK3FormParsingStrategy())

    def register(
        self,
        form_type: FormType,
        strategy: BaseFormParsingStrategy,
    ) -> None:
        self._strategies[form_type] = strategy
        logger.debug(
            "Зарегистрирована стратегия %s для формы %s",
            strategy.__class__.__name__,
            form_type.value,
        )

    def get_strategy(self, form_type: FormType) -> BaseFormParsingStrategy:
        strategy = self._strategies.get(form_type, self._default)
        if strategy is self._default:
            logger.debug(
                "Форма %s: используется стратегия по умолчанию (AutoFormParsingStrategy)",
                form_type.value,
            )
        else:
            logger.debug(
                "Форма %s: используется стратегия %s",
                form_type.value,
                strategy.__class__.__name__,
            )
        return strategy

    def build_pipeline_for_sheet(
        self,
        form_info: FormInfo,
        sheet_name: str,
        sheet_index: int,
    ):
        """
        Точка входа для ProcessSheetsStep.
        Возвращает ParsingPipelineRunner или None если лист нужно пропустить.
        """
        from app.application.parsing.pipeline import ParsingPipelineRunner

        strategy = self.get_strategy(form_info.type)

        if not strategy.should_process_sheet(sheet_name, sheet_index, form_info):
            logger.debug(
                "Лист '%s' (индекс %d) пропущен стратегией %s",
                sheet_name,
                sheet_index,
                strategy.__class__.__name__,
            )
            return None

        steps = strategy.build_steps_for_sheet(sheet_name, form_info)
        return ParsingPipelineRunner(steps=steps)


# --- Синглтон ---

_registry: Optional[ParsingStrategyRegistry] = None


def get_parsing_strategy_registry(sheet_service=None) -> ParsingStrategyRegistry:
    """
    Возвращает глобальный экземпляр реестра.

    При первом вызове создаёт реестр с переданным sheet_service.
    Последующие вызовы возвращают уже созданный экземпляр.

    Args:
        sheet_service: Передаётся только при первом вызове (инициализации).
    """
    global _registry
    if _registry is None:
        _registry = ParsingStrategyRegistry(sheet_service=sheet_service)
    return _registry