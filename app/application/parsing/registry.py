"""Централизованный реестр parsing pipeline для разных типов форм."""
import logging
from typing import Dict, Optional, Callable

from app.domain.form.models import FormType
from app.application.parsing.pipeline import ParsingPipelineRunner, build_parsing_pipeline

logger = logging.getLogger(__name__)


class ParsingPipelineRegistry:
    """
    Централизованный реестр для выбора parsing pipeline по типу формы и имени листа.
    
    Регистрирует конфигурации pipeline для разных форм:
    - 1ФК: фиксированные параметры для каждого листа
    - 5ФК: автоматическое определение структуры
    - UNKNOWN: универсальный режим
    """

    def __init__(self):
        self._configs: Dict[str, Dict[str, Callable]] = {}
        self._init_default_configs()

    def _init_default_configs(self) -> None:
        """Инициализирует конфигурации по умолчанию для известных форм."""
        # Конфигурация для 1ФК: фиксированные параметры для каждого листа
        fk1_configs = {
            "Раздел0": {
                "header_row_range": (1, 4),
                "vertical_header_col": 0,
                "start_data_row": 5,
            },
            "Раздел1": {
                "header_row_range": (1, 4),
                "vertical_header_col": 0,
                "start_data_row": 5,
            },
            "Раздел2": {
                "header_row_range": (1, 4),
                "vertical_header_col": 0,
                "start_data_row": 5,
            },
            "Раздел3": {
                "header_row_range": (1, 4),
                "vertical_header_col": 0,
                "start_data_row": 5,
            },
            "Раздел4": {
                "header_row_range": (1, 4),
                "vertical_header_col": 0,
                "start_data_row": 5,
            },
            "Раздел5": {
                "header_row_range": (1, 4),
                "vertical_header_col": 0,
                "start_data_row": 5,
            },
            "Раздел6": {
                "header_row_range": (1, 4),
                "vertical_header_col": 0,
                "start_data_row": 5,
            },
            "Раздел7": {
                "header_row_range": (1, 4),
                "vertical_header_col": 0,
                "start_data_row": 5,
            },
        }

        # Регистрируем конфигурации
        for sheet_name, config in fk1_configs.items():
            # Используем замыкание для фиксации значений
            def make_builder(sn: str, cfg: dict):
                return lambda: build_parsing_pipeline(
                    form_type=FormType.FK_1.value,
                    sheet_name=sn,
                    auto_detect_structure=False,
                    header_row_range=cfg["header_row_range"],
                    vertical_header_col=cfg["vertical_header_col"],
                    start_data_row=cfg["start_data_row"],
                )

            self.register(
                form_type=FormType.FK_1,
                sheet_name=sheet_name,
                config_builder=make_builder(sheet_name, config),
            )

        # Конфигурация для 5ФК: автоматическое определение структуры
        self.register(
            form_type=FormType.FK_5,
            sheet_name="*",  # Wildcard для всех листов
            config_builder=lambda: build_parsing_pipeline(
                form_type=FormType.FK_5.value,
                sheet_name="*",
                auto_detect_structure=True,
            ),
        )

        # Конфигурация для UNKNOWN: универсальный режим
        self.register(
            form_type=FormType.UNKNOWN,
            sheet_name="*",
            config_builder=lambda: build_parsing_pipeline(
                form_type=FormType.UNKNOWN.value,
                sheet_name="*",
                auto_detect_structure=False,
                header_row_range=(0, 2),
                vertical_header_col=0,
                start_data_row=3,
            ),
        )

    def register(
        self,
        form_type: FormType,
        sheet_name: str,
        config_builder: Callable[[], ParsingPipelineRunner],
    ) -> None:
        """
        Регистрирует конфигурацию pipeline для типа формы и листа.

        Args:
            form_type: Тип формы
            sheet_name: Название листа или "*" для всех листов
            config_builder: Функция, создающая ParsingPipelineRunner
        """
        key = f"{form_type.value}:{sheet_name}"
        self._configs[key] = config_builder
        logger.debug("Зарегистрирован parsing pipeline: %s", key)

    def get_pipeline(
        self, form_type: FormType, sheet_name: str
    ) -> Optional[ParsingPipelineRunner]:
        """
        Возвращает pipeline для указанного типа формы и листа.

        Args:
            form_type: Тип формы
            sheet_name: Название листа

        Returns:
            ParsingPipelineRunner или None, если конфигурация не найдена
        """
        # Сначала ищем точное совпадение
        specific_key = f"{form_type.value}:{sheet_name}"
        if specific_key in self._configs:
            logger.debug("Найден специфичный pipeline для %s", specific_key)
            return self._configs[specific_key]()

        # Затем ищем wildcard для типа формы
        wildcard_key = f"{form_type.value}:*"
        if wildcard_key in self._configs:
            logger.debug("Используется wildcard pipeline для %s", wildcard_key)
            return self._configs[wildcard_key]()

        # Fallback: UNKNOWN
        unknown_key = f"{FormType.UNKNOWN.value}:*"
        if unknown_key in self._configs:
            logger.warning(
                "Используется fallback pipeline (UNKNOWN) для формы %s, листа %s",
                form_type.value,
                sheet_name,
            )
            return self._configs[unknown_key]()

        logger.error("Не найден pipeline для формы %s, листа %s", form_type.value, sheet_name)
        return None


# Глобальный экземпляр registry
_global_registry: Optional[ParsingPipelineRegistry] = None


def get_parsing_pipeline_registry() -> ParsingPipelineRegistry:
    """Возвращает глобальный экземпляр registry."""
    global _global_registry
    if _global_registry is None:
        _global_registry = ParsingPipelineRegistry()
    return _global_registry
