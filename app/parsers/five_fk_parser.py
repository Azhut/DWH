"""
Парсер для форм типа 5ФК.
ВНИМАНИЕ: Это заглушка! Реальная логика парсинга 5ФК должна быть реализована позже.
"""
import pandas as pd
import logging
from typing import Dict, List, Optional, Any

from app.parsers.base_sheet_parser import BaseSheetParser

logger = logging.getLogger(__name__)


class FiveFKParser(BaseSheetParser):
    """
    Парсер для форм 5ФК.

    Особенности 5ФК (предположительно):
    - Другая структура заголовков
    - Возможно, другие разделы
    - Может потребоваться другая логика обработки

    Сейчас это заглушка, которая логирует факт обработки 5ФК и возвращает минимальные данные.
    """

    # Маппинг листов 5ФК на параметры парсинга
    # Это пример, нужно уточнить реальную структуру 5ФК
    SHEET_CONFIGS = {
        "Основные показатели": {
            "header_row_range": (1, 3),
            "vertical_header_col": 0,
            "start_data_row": 4
        },
        "Финансирование": {
            "header_row_range": (1, 2),
            "vertical_header_col": 0,
            "start_data_row": 3
        },
        "Спортсооружения": {
            "header_row_range": (1, 4),
            "vertical_header_col": 0,
            "start_data_row": 5
        }
    }

    def __init__(self, sheet_name: str):
        """
        Инициализация парсера 5ФК для конкретного листа.

        Args:
            sheet_name: Название листа
        """
        config = self.SHEET_CONFIGS.get(sheet_name, {
            "header_row_range": (1, 3),
            "vertical_header_col": 0,
            "start_data_row": 4
        })

        super().__init__(
            header_row_range=config["header_row_range"],
            vertical_header_col=config["vertical_header_col"],
            start_data_row=config["start_data_row"]
        )

        self.sheet_name = sheet_name
        logger.info(f"Инициализирован парсер 5ФК для листа: '{sheet_name}'")

    def parse(self, sheet: pd.DataFrame) -> Dict[str, Any]:
        """
        Основной метод парсинга листа 5ФК.

        Args:
            sheet: DataFrame с данными листа

        Returns:
            Словарь с распарсенными данными в стандартном формате
        """
        logger.warning(f"Используется ЗАГЛУШКА парсера 5ФК для листа '{self.sheet_name}'! "
                       f"Реальная логика парсинга не реализована.")

        try:
            # Пытаемся использовать базовую логику парсинга
            # Это может работать для простых таблиц 5ФК
            logger.info(f"Попытка базового парсинга листа 5ФК: '{self.sheet_name}'")
            result = super().parse(sheet)

            # Добавляем мета-информацию о типе формы
            result["form_type"] = "5ФК"
            result["sheet_name"] = self.sheet_name

            logger.info(f"Базовый парсинг 5ФК успешен для листа '{self.sheet_name}': "
                        f"{len(result.get('data', []))} колонок, "
                        f"{len(result.get('headers', {}).get('vertical', []))} строк")
            return result

        except Exception as e:
            # Если базовая логика не сработала, возвращаем заглушку
            logger.error(f"Ошибка при базовом парсинге 5ФК листа '{self.sheet_name}': {e}")

            return {
                "headers": {
                    "horizontal": [f"Колонка_5ФК_{i}" for i in range(5)],
                    "vertical": [f"Строка_5ФК_{i}" for i in range(10)]
                },
                "data": [],
                "form_type": "5ФК",
                "sheet_name": self.sheet_name,
                "warning": "Используется заглушка парсера 5ФК. Реальная логика не реализована."
            }

    def generate_flat_data(
            self,
            year: int,
            city: str,
            sheet_name: str,
            form_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Генерирует плоские данные для 5ФК.

        Args:
            year: Год данных
            city: Город
            sheet_name: Название листа
            form_id: ID формы (опционально)

        Returns:
            Список плоских записей
        """
        logger.warning(f"Генерация flat_data для 5ФК (заглушка): лист '{sheet_name}'")

        if not self.data:
            logger.error(f"Нет данных для генерации flat_data (5ФК, лист: {sheet_name})")
            return []

        # Используем родительскую логику, она должна работать
        flat_data = super().generate_flat_data(year, city, sheet_name, form_id)

        logger.info(f"Сгенерировано {len(flat_data)} записей flat_data для 5ФК (лист: {sheet_name})")
        return flat_data