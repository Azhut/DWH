"""
Парсер для форм типа 5ФК с автоматическим определением структуры таблицы.
"""
import pandas as pd
import numpy as np
import logging
from typing import Dict, List, Optional, Any, Tuple
import re
from app.parsers.base_sheet_parser import BaseSheetParser
from app.parsers.header_fixer import fix_header, finalize_header_fixing
from app.parsers.notes_processor import NotesProcessor, _SERVICE_EMPTY

logger = logging.getLogger(__name__)

class FiveFKParser(BaseSheetParser):
    """
    Парсер для форм типа 5ФК с автоматическим определением структуры таблицы.
    Особенности:
    - Автоматически определяет начало таблицы и количество строк заголовков
    - Использует pd.read_excel с правильными параметрами для чтения MultiIndex заголовков
    - Корректно обрабатывает Unnamed значения в заголовках
    - Правильно формирует flat_data с координатами ячеек
    - Использует первый ненулевой столбец как боковые заголовки
    """

    def __init__(self, sheet_name: str):
        """
        Инициализация парсера 5ФК.
        Args:
            sheet_name: Название листа
        """
        # Используем базовые параметры, но они будут переопределены автоматически
        super().__init__(
            header_row_range=(0, 5),  # Максимальный диапазон для поиска
            vertical_header_col=0,
            start_data_row=5
        )
        self.sheet_name = sheet_name
        self.original_df = None
        self.processed_df = None
        self.header_start_row = 0
        self.header_end_row = 0
        self.data_start_row = 0
        self.num_header_levels = 0
        self.horizontal_headers = []
        self.vertical_headers = []
        self.actual_columns = []  # Реальные имена колонок после обработки
        self.vertical_header_column = None  # Имя столбца с боковыми заголовками
        logger.info(f"Инициализирован парсер 5ФК для листа: '{sheet_name}'")

    def _is_empty_or_nan(self, value) -> bool:
        """Проверяет, является ли значение пустым или NaN"""
        if pd.isna(value) or value is None:
            return True
        str_val = str(value).strip().lower()
        return str_val in ['', 'nan', 'none', 'null', 'nat', '_x0000_', '_x000d_']

    def _is_numeric_value(self, value) -> bool:
        """Проверяет, является ли значение числовым"""
        if self._is_empty_or_nan(value):
            return False
        str_val = str(value).strip()
        if str_val == '':
            return False
        try:
            # Убираем разделители тысяч и заменяем запятую на точку
            clean_val = str_val.replace(' ', '').replace(',', '.')
            float(clean_val)
            return True
        except (ValueError, TypeError):
            return False

    def _find_first_non_empty_column(self, df: pd.DataFrame) -> str:
        """
        Находит первый ненулевой столбец, который содержит боковые заголовки.
        Пропускает Unnamed_Column_* столбцы.
        """
        for col_name in df.columns:
            if isinstance(col_name, str) and col_name.startswith('Unnamed_Column_'):
                continue
            # Проверяем, что в столбце есть непустые значения
            non_empty_count = sum(1 for val in df[col_name] if not self._is_empty_or_nan(val))
            if non_empty_count > 0:
                return col_name
        return None

    def _detect_table_structure(self, raw_df: pd.DataFrame) -> Tuple[int, int, int]:
        """
        Автоматически определяет структуру таблицы:
        - header_start_row: строка, с которой начинаются заголовки
        - header_end_row: строка, на которой заканчиваются заголовки
        - data_start_row: строка, с которой начинаются данные

        Использует эвристику: после заголовков идет строка с номерами столбцов (натуральные числа)
        """
        # Создаем копию для анализа
        analysis_df = raw_df.copy()

        # Шаг 1: Найти первую непустую строку (начало таблицы)
        first_non_empty_row = None
        max_rows_to_check = min(50, len(analysis_df))

        for i in range(max_rows_to_check):
            row = analysis_df.iloc[i]
            non_empty_count = sum(1 for val in row if not self._is_empty_or_nan(val))
            if non_empty_count > 3:  # Если в строке больше 3 непустых ячеек
                first_non_empty_row = i
                break

        if first_non_empty_row is None:
            first_non_empty_row = 0

        self.header_start_row = first_non_empty_row
        logger.debug(f"Начало таблицы определено на строке: {self.header_start_row}")

        # Шаг 2: Найти строку с номерами столбцов (натуральные числа)
        column_numbers_row = None
        current_row = first_non_empty_row

        # Проверяем максимум 15 строк после начала таблицы
        while current_row < min(first_non_empty_row + 15, len(analysis_df)):
            row = analysis_df.iloc[current_row]

            # Подсчитываем количество натуральных чисел в строке
            numeric_count = 0
            valid_numeric_row = True

            for value in row:
                if self._is_empty_or_nan(value):
                    continue

                str_val = str(value).strip()
                # Проверяем, является ли значение натуральным числом
                if str_val.isdigit() and int(str_val) > 0:
                    numeric_count += 1
                else:
                    # Если есть нечисловые значения, кроме специальных ключевых слов
                    if not any(keyword in str_val.lower() for keyword in ['итого', 'всего', 'сумма', '№ строки', '№', 'столбец', 'колонка']):
                        valid_numeric_row = False
                        break

            # Если в строке достаточно натуральных чисел и они последовательны
            if valid_numeric_row and numeric_count >= 3:
                column_numbers_row = current_row
                break

            current_row += 1

        if column_numbers_row is None:
            # Не нашли строку с номерами столбцов, используем эвристику по наличию чисел в данных
            logger.warning(f"Не найдена строка с номерами столбцов для листа '{self.sheet_name}'")
            # Ищем первую строку с реальными данными (числами)
            for i in range(first_non_empty_row, min(first_non_empty_row + 20, len(analysis_df))):
                row = analysis_df.iloc[i]
                numeric_count = sum(1 for val in row if self._is_numeric_value(val))
                if numeric_count >= 3:
                    column_numbers_row = i - 1  # Предполагаем, что заголовки заканчиваются на предыдущей строке
                    break

        if column_numbers_row is None:
            column_numbers_row = first_non_empty_row + 2  # Значение по умолчанию

        self.header_end_row = column_numbers_row - 1
        self.data_start_row = column_numbers_row + 1
        self.num_header_levels = max(1, self.header_end_row - self.header_start_row + 1)

        logger.debug(f"Структура определена для листа '{self.sheet_name}':")
        logger.debug(f"  Начало заголовков: {self.header_start_row}")
        logger.debug(f"  Конец заголовков: {self.header_end_row}")
        logger.debug(f"  Начало данных: {self.data_start_row}")
        logger.debug(f"  Уровней заголовков: {self.num_header_levels}")

        return self.header_start_row, self.header_end_row, self.data_start_row

    def _load_with_multilevel_headers(self, file_path: str, sheet_name: str) -> pd.DataFrame:
        """
        Загружает DataFrame с многоуровневыми заголовками, используя автоматически определенные параметры.
        """
        try:
            # Читаем весь лист для анализа структуры
            raw_df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
            self.original_df = raw_df.copy()

            # Определяем структуру таблицы
            self._detect_table_structure(raw_df)

            # Проверяем, что есть данные для обработки
            if self.data_start_row >= len(raw_df):
                logger.warning(f"Не найдены данные в листе '{sheet_name}'")
                return pd.DataFrame()

            # Определяем строки заголовков
            header_rows = list(range(self.header_start_row, self.header_end_row + 1))

            # Читаем файл с правильными заголовками
            df = pd.read_excel(
                file_path,
                sheet_name=sheet_name,
                header=header_rows
            )

            # Обработка примечаний
            df = NotesProcessor.process_notes(df, raw_quantity=self.num_header_levels)

            # Обрезаем строки до начала данных
            if self.data_start_row > self.header_end_row + 1:
                rows_to_skip = self.data_start_row - (self.header_end_row + 1)
                if rows_to_skip > 0 and rows_to_skip < len(df):
                    df = df.iloc[rows_to_skip:].reset_index(drop=True)

            # Обрабатываем имена колонок
            self.actual_columns = self._process_columns(df.columns)

            # Создаем копию с обработанными колонками
            processed_df = df.copy()
            processed_df.columns = self.actual_columns

            self.processed_df = processed_df
            return processed_df

        except Exception as e:
            logger.error(f"Ошибка при загрузке с многоуровневыми заголовками: {e}", exc_info=True)
            # Fallback: возвращаем исходный DataFrame
            return raw_df.iloc[self.data_start_row:].reset_index(drop=True)

    def _process_columns(self, columns) -> List[str]:
        """
        Обрабатывает имена колонок, объединяя уровни MultiIndex и удаляя Unnamed значения.
        """
        processed_columns = []

        for col in columns:
            if isinstance(col, tuple):
                # Для MultiIndex: обрабатываем каждый уровень
                levels = []
                for level_value in col:
                    if pd.isna(level_value) or self._is_empty_or_nan(level_value):
                        continue
                    str_val = str(level_value).strip()
                    # Пропускаем Unnamed значения
                    if 'unnamed' in str_val.lower():
                        continue
                    # Пропускаем служебные значения
                    if str_val.lower() in ['nan', 'none', 'null', '_x0000_', '_x000d_']:
                        continue
                    # Удаляем _x000D_ и переносы строк
                    str_val = str_val.replace('_x000D_', '').replace('\n', ' ').strip()
                    levels.append(str_val)

                if levels:
                    # Объединяем уровни через разделитель
                    combined = ' | '.join(levels)
                    processed_columns.append(combined)
                else:
                    processed_columns.append(f"Unnamed_Column_{len(processed_columns)}")
            else:
                # Для обычного Index
                str_val = str(col).strip()
                if 'unnamed' in str_val.lower() or self._is_empty_or_nan(str_val):
                    processed_columns.append(f"Unnamed_Column_{len(processed_columns)}")
                else:
                    str_val = str_val.replace('_x000D_', '').replace('\n', ' ').strip()
                    processed_columns.append(str_val)

        return processed_columns

    def _extract_vertical_headers(self, df: pd.DataFrame) -> List[str]:
        """
        Извлекает вертикальные заголовки из первого ненулевого столбца.
        """
        if df.empty:
            return []

        # Находим первый ненулевой столбец для вертикальных заголовков
        self.vertical_header_column = self._find_first_non_empty_column(df)

        if self.vertical_header_column is None:
            logger.warning(f"Не найден первый ненулевой столбец для листа '{self.sheet_name}'")
            return []

        logger.info(f"Найден столбец с боковыми заголовками: '{self.vertical_header_column}'")

        # Извлекаем значения из столбца с боковыми заголовками
        vertical_headers = []
        for idx, row in df.iterrows():
            value = row[self.vertical_header_column]
            if not self._is_empty_or_nan(value):
                str_value = str(value).strip()
                # Пропускаем числовые значения (это данные, а не заголовки)
                if self._is_numeric_value(str_value):
                    continue
                # Пропускаем служебные строки
                if str_value.lower() in ['итого', 'всего', 'сумма', '№ строки', '№', 'строка']:
                    continue
                vertical_headers.append(str_value)

        return vertical_headers

    def parse(self, sheet: pd.DataFrame) -> Dict[str, Any]:
        """
        Основной метод парсинга листа 5ФК с автоматическим определением структуры.
        """
        logger.info(f"Начало парсинга листа 5ФК: '{self.sheet_name}'")
        try:
            # Сохраняем исходный DataFrame
            self.original_df = sheet.copy()

            # Для корректной работы с файлами, нам нужен путь к файлу
            # В текущей архитектуре это сложно, поэтому используем обходной путь
            # Создаем временный файл в памяти
            import tempfile
            import os

            with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp_file:
                sheet.to_excel(tmp_file.name, index=False, header=False)
                tmp_path = tmp_file.name

            try:
                # Загружаем данные с многоуровневыми заголовками
                self.processed_df = self._load_with_multilevel_headers(tmp_path, 'Sheet1')
            finally:
                # Удаляем временный файл
                os.unlink(tmp_path)

            if self.processed_df.empty:
                logger.warning(f"Пустой DataFrame после обработки листа '{self.sheet_name}'")
                return self._create_empty_result()

            # Извлекаем заголовки
            self.horizontal_headers = self.actual_columns
            self.vertical_headers = self._extract_vertical_headers(self.processed_df)

            # Применяем фикс заголовков
            self.horizontal_headers = self._remove_newlines_from_headers(self.horizontal_headers)
            self.vertical_headers = self._remove_newlines_from_headers(self.vertical_headers)

            # Создаем структурированные данные
            self.data = self._create_data_structure(self.processed_df, self.horizontal_headers, self.vertical_headers)

            result = {
                "headers": {
                    "vertical": self.vertical_headers,
                    "horizontal": self.horizontal_headers
                },
                "data": self.data,
                "form_type": "5ФК",
                "sheet_name": self.sheet_name,
                "table_info": {
                    "header_start_row": self.header_start_row,
                    "header_end_row": self.header_end_row,
                    "data_start_row": self.data_start_row,
                    "num_header_levels": self.num_header_levels,
                    "num_rows": len(self.vertical_headers),
                    "num_columns": len(self.horizontal_headers),
                    "vertical_header_column": self.vertical_header_column
                }
            }

            logger.info(f"Успешный парсинг 5ФК для листа '{self.sheet_name}': "
                        f"{len(self.horizontal_headers)} колонок, {len(self.vertical_headers)} строк")

            finalize_header_fixing()
            return result

        except Exception as e:
            logger.error(f"Критическая ошибка при парсинге 5ФК листа '{self.sheet_name}': {e}", exc_info=True)
            return self._create_error_result(str(e))

    def _create_data_structure(self, df: pd.DataFrame, horizontal_headers: list, vertical_headers: list) -> List[Dict]:
        """
        Создает структурированные данные в формате, совместимом с существующей системой.
        Использует первый ненулевой столбец как боковые заголовки и прямой доступ к ячейкам.
        """
        data = []

        # Удаляем Unnamed колонки и столбец с боковыми заголовками из горизонтальных заголовков
        filtered_horizontal_headers = []
        filtered_columns = []

        for i, col_name in enumerate(horizontal_headers):
            if self.vertical_header_column and col_name == self.vertical_header_column:
                continue
            if col_name.startswith('Unnamed_Column_'):
                continue
            filtered_horizontal_headers.append(col_name)
            filtered_columns.append(df.columns[i])

        # Для каждого горизонтального заголовка создаем структуру данных
        for col_idx, column_header in enumerate(filtered_horizontal_headers):
            if col_idx >= len(filtered_columns):
                break

            df_col_name = filtered_columns[col_idx]
            column_values = []

            # Для каждого вертикального заголовка получаем значение из ячейки
            for row_idx, row_header in enumerate(vertical_headers):
                if row_idx >= len(df):
                    continue

                # Получаем значение из ячейки
                try:
                    cell_value = df.iloc[row_idx][df_col_name]

                    # Преобразуем значение в число если возможно
                    processed_value = cell_value
                    if self._is_numeric_value(cell_value):
                        try:
                            str_val = str(cell_value).strip().replace(',', '.').replace(' ', '')
                            if '.' in str_val:
                                processed_value = float(str_val)
                                if processed_value.is_integer():
                                    processed_value = int(processed_value)
                            else:
                                processed_value = int(str_val)
                        except (ValueError, TypeError):
                            pass

                    column_values.append({
                        "row_header": row_header,
                        "value": processed_value
                    })
                except KeyError:
                    logger.debug(f"Ключ {df_col_name} не найден в DataFrame для листа '{self.sheet_name}'")
                    continue
                except Exception as e:
                    logger.debug(f"Ошибка при получении значения ({row_idx}, {df_col_name}): {e}")
                    continue

            if column_values:  # Добавляем только если есть значения
                data.append({
                    "column_header": column_header,
                    "values": column_values
                })

        return data

    def _create_empty_result(self) -> Dict[str, Any]:
        """Создает пустой результат при ошибке"""
        return {
            "headers": {"vertical": [], "horizontal": []},
            "data": [],
            "form_type": "5ФК",
            "sheet_name": self.sheet_name,
            "warning": "Пустой результат после обработки"
        }

    def _create_error_result(self, error: str) -> Dict[str, Any]:
        """Создает результат с ошибкой"""
        return {
            "headers": {"vertical": [], "horizontal": []},
            "data": [],
            "form_type": "5ФК",
            "sheet_name": self.sheet_name,
            "error": error,
            "warning": "Ошибка при парсинге листа"
        }

    def _remove_newlines_from_headers(self, headers: list) -> list:
        """Удаляет переносы строк в заголовках."""
        return [fix_header(str(h)) if isinstance(h, str) else str(h) for h in headers]

    def generate_flat_data(
            self,
            year: int,
            city: str,
            sheet_name: str,
            form_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Генерирует плоские данные в формате, совместимом с 1ФК.
        Каждая запись содержит: year, city, section, row, column, value, form
        """
        logger.info(f"Генерация flat_data для 5ФК листа '{sheet_name}'")

        if not self.data:
            logger.warning(f"Нет данных для генерации flat_data (5ФК, лист: {sheet_name})")
            return []

        flat_data = []

        for column_data in self.data:
            column_header = column_data.get("column_header", "")
            values = column_data.get("values", [])

            for row_data in values:
                row_header = row_data.get("row_header", "")
                value = row_data.get("value", 0)

                # Очистка и преобразование значения
                if self._is_empty_or_nan(value) or str(value).strip() in [_SERVICE_EMPTY, 'nan', 'none', '']:
                    value = 0

                # Попытка преобразовать в число
                try:
                    if isinstance(value, str):
                        value_clean = value.strip().replace(',', '.').replace(' ', '')
                        if value_clean.replace('.', '', 1).replace('-', '', 1).isdigit():
                            value_num = float(value_clean)
                            value = int(value_num) if value_num.is_integer() else round(value_num, 2)
                except Exception:
                    pass

                flat_record = {
                    "year": year,
                    "city": city.upper(),
                    "section": sheet_name,
                    "row": row_header,
                    "column": column_header,
                    "value": value,
                    "form": form_id,
                    "form_type": "5ФК"
                }

                flat_data.append(flat_record)

        logger.info(f"Сгенерировано {len(flat_data)} записей flat_data для 5ФК (лист: {sheet_name})")
        return flat_data