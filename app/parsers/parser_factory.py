"""
Фабрика для создания парсеров в зависимости от типа формы
"""
import logging
from typing import Optional

from app.models.form_model import FormType
from app.parsers.parsers import PARSERS as FK1_PARSERS
from app.parsers.five_fk_parser import FiveFKParser
from app.parsers.universal_parser import UniversalParser
from app.parsers.base_sheet_parser import BaseSheetParser

logger = logging.getLogger(__name__)


class ParserFactory:
    """Фабрика для создания парсеров в зависимости от типа формы"""

    @staticmethod
    def create_parser(sheet_name: str, form_type: FormType) -> BaseSheetParser:
        """
        Создает парсер для указанного листа и типа формы.

        Args:
            sheet_name: Название листа (например, "Раздел0")
            form_type: Тип формы (1ФК, 5ФК и т.д.)

        Returns:
            Экземпляр парсера, наследованный от BaseSheetParser
        """
        logger.debug(f"Создание парсера для листа '{sheet_name}', тип формы: {form_type}")

        if form_type == FormType.FK_1:
            # Используем существующие парсеры для 1ФК
            parser_class = FK1_PARSERS.get(sheet_name)
            if parser_class:
                parser = parser_class()
                logger.debug(f"Используется парсер 1ФК для листа '{sheet_name}'")
                return parser
            else:
                # Если для этого листа нет специфичного парсера 1ФК
                logger.warning(f"Не найден специфичный парсер 1ФК для листа '{sheet_name}', "
                               f"используется универсальный парсер")

        elif form_type == FormType.FK_5:
            # Используем парсер для 5ФК
            logger.info(f"Используется парсер 5ФК для листа '{sheet_name}'")
            return FiveFKParser(sheet_name)

        # Fallback: универсальный парсер для неизвестных типов или отсутствующих парсеров
        logger.info(f"Используется универсальный парсер для листа '{sheet_name}' (тип формы: {form_type})")
        return UniversalParser(sheet_name)

    @staticmethod
    def get_available_parsers(form_type: FormType) -> dict:
        """
        Возвращает доступные парсеры для указанного типа формы.

        Args:
            form_type: Тип формы

        Returns:
            Словарь {название_листа: класс_парсера}
        """
        if form_type == FormType.FK_1:
            return FK1_PARSERS.copy()
        elif form_type == FormType.FK_5:
            # Для 5ФК пока только один парсер, но можно расширить
            return {"*": FiveFKParser}
        else:
            return {"*": UniversalParser}