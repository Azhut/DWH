"""
Универсальный парсер для неизвестных типов форм.
Используется как fallback, когда тип формы не распознан.
"""
import pandas as pd
import logging
from typing import Dict, List, Optional, Any

from app.parsers.base_sheet_parser import BaseSheetParser
from app.parsers.notes_processor import NotesProcessor

logger = logging.getLogger(__name__)


class UniversalParser(BaseSheetParser):
    """
    Универсальный парсер для обработки неизвестных типов форм.

    Особенности:
    - Использует консервативные настройки (минимальные предположения о структуре)
    - Логирует предупреждения о неоптимальном парсинге
    - Пытается извлечь данные в любом случае
    """

    def __init__(self, sheet_name: str):
        """
        Инициализация универсального парсера.

        Args:
            sheet_name: Название листа
        """
        # Консервативные настройки для максимальной совместимости
        super().__init__(
            header_row_range=(0, 2),  # Предполагаем заголовки в первых 3 строках
            vertical_header_col=0,  # Предполагаем вертикальные заголовки в первом столбце
            start_data_row=3  # Данные начинаются с 4-й строки
        )

        self.sheet_name = sheet_name
        logger.warning(f"Инициализирован УНИВЕРСАЛЬНЫЙ парсер для листа: '{sheet_name}'. "
                       f"Это может указывать на неизвестный тип формы или отсутствие специфичного парсера.")

    def parse(self, sheet: pd.DataFrame) -> Dict[str, Any]:
        """
        Универсальный метод парсинга.

        Args:
            sheet: DataFrame с данными листа

        Returns:
            Словарь с распарсенными данными в стандартном формате
        """
        logger.info(f"Начало универсального парсинга листа: '{self.sheet_name}'")

        # Сохраняем оригинальные размеры для отладки
        original_shape = sheet.shape
        logger.debug(f"Размер листа '{self.sheet_name}': {original_shape}")

        try:
            # Пытаемся обработать примечания (если есть)
            try:
                sheet = NotesProcessor.process_notes(sheet, raw_quantity=self.header_row_range[1])
                logger.debug(f"Обработаны примечания для листа '{self.sheet_name}'")
            except Exception as e:
                logger.warning(f"Не удалось обработать примечания для листа '{self.sheet_name}': {e}")

            # Используем базовую логику парсинга
            result = super().parse(sheet)

            # Добавляем мета-информацию
            result["form_type"] = "unknown"
            result["sheet_name"] = self.sheet_name
            result["parser_type"] = "universal"

            # Проверяем результат
            horizontal_count = len(result.get("headers", {}).get("horizontal", []))
            vertical_count = len(result.get("headers", {}).get("vertical", []))
            data_count = len(result.get("data", []))

            logger.info(f"Универсальный парсинг завершен для '{self.sheet_name}': "
                        f"{horizontal_count} колонок, {vertical_count} строк, {data_count} элементов данных")

            if horizontal_count == 0 or vertical_count == 0:
                logger.warning(f"Универсальный парсер извлек мало данных из листа '{self.sheet_name}'. "
                               f"Возможно, структура таблицы отличается от ожидаемой.")

            return result

        except Exception as e:
            logger.error(f"Критическая ошибка при универсальном парсинге листа '{self.sheet_name}': {e}")

            # Возвращаем минимальную структуру, чтобы система не упала
            return {
                "headers": {
                    "horizontal": [],
                    "vertical": []
                },
                "data": [],
                "form_type": "unknown",
                "sheet_name": self.sheet_name,
                "parser_type": "universal",
                "error": f"Ошибка парсинга: {str(e)[:100]}",
                "warning": "Используется аварийный режим универсального парсера"
            }

    def _get_header_rows(self, sheet: pd.DataFrame):
        """
        Переопределяем для большей гибкости.
        Пытаемся найти заголовки в первых строках.
        """
        max_rows_to_check = 5  # Проверяем первые 5 строк
        actual_rows = min(max_rows_to_check, sheet.shape[0])

        header_rows = sheet.iloc[:actual_rows].fillna("")
        logger.debug(f"Универсальный парсер проверяет {actual_rows} строк на заголовки")

        return header_rows

    def generate_flat_data(
            self,
            year: int,
            city: str,
            sheet_name: str,
            form_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Генерирует плоские данные с дополнительным логированием.
        """
        logger.info(f"Универсальная генерация flat_data для листа '{sheet_name}'")

        if not self.data:
            logger.warning(f"Нет данных для генерации flat_data (лист: {sheet_name}), "
                           f"возвращаем пустой список")
            return []

        flat_data = super().generate_flat_data(year, city, sheet_name, form_id)

        if flat_data:
            logger.info(f"Универсальный парсер сгенерировал {len(flat_data)} записей flat_data "
                        f"для листа '{sheet_name}'")
        else:
            logger.warning(f"Универсальный парсер не смог сгенерировать flat_data "
                           f"для листа '{sheet_name}'")

        return flat_data