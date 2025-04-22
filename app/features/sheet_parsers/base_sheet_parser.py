from typing import List, Dict, Union

import pandas as pd

from app.features.sheet_parsers.notes_processor import NotesProcessor, _SERVICE_EMPTY


class BaseSheetParser:
    """
    Базовый класс для парсинга данных с листов Excel.
    """

    def __init__(self, header_row_range, vertical_header_col, start_data_row):
        """
        Инициализация парсера с конкретными параметрами.

        :param header_row_range: Диапазон строк для верхних заголовков (например, (1, 4))
        :param vertical_header_col: Индекс колонки для вертикальных заголовков
        :param start_data_row: Строка, с которой начинаются данные
        """
        self.header_row_range = header_row_range
        self.vertical_header_col = vertical_header_col
        self.start_data_row = start_data_row

        self.data = None

    def parse_headers(self, sheet: pd.DataFrame):
        """Парсит заголовки с листа."""
        header_rows = self._get_header_rows(sheet)
        self._fill_empty_cells_in_headers(header_rows)
        horizontal_headers = self._get_horizontal_headers(header_rows)
        vertical_headers = self._get_vertical_headers(sheet)

        horizontal_headers = self._remove_newlines_from_headers(horizontal_headers)
        vertical_headers = self._remove_newlines_from_headers(vertical_headers)

        return horizontal_headers, vertical_headers

    def _get_header_rows(self, sheet: pd.DataFrame):
        """Получает строки заголовков."""
        return sheet.iloc[self.header_row_range[0]:self.header_row_range[1]].fillna("")

    def _fill_empty_cells_in_headers(self, header_rows: pd.DataFrame):
        """Заполняет пустые ячейки в заголовках."""
        for row_idx in range(len(header_rows) - 1, 0, -1):
            for col_idx in range(header_rows.shape[1]):
                if header_rows.iloc[row_idx, col_idx] == "":
                    for search_row in range(row_idx - 1, -1, -1):
                        if header_rows.iloc[search_row, col_idx] != "":
                            header_rows.iloc[row_idx, col_idx] = header_rows.iloc[search_row, col_idx]
                            break

        for row_idx in range(self.header_row_range[0] - 1, header_rows.shape[0]):
            for col_idx in range(1, header_rows.shape[1]):
                if header_rows.iloc[row_idx, col_idx] == "":
                    header_rows.iloc[row_idx, col_idx] = header_rows.iloc[row_idx, col_idx - 1]

    def _get_horizontal_headers(self, header_rows: pd.DataFrame):
        """Формирует горизонтальные заголовки.(колонки)"""
        horizontal_headers = []
        for col_idx in range(1, header_rows.shape[1]):
            current_path = []
            current_value = header_rows.iloc[len(header_rows) - 1, col_idx]
            current_path.append(current_value)

            for row_idx in range(len(header_rows) - 2, -1, -1):
                value = header_rows.iloc[row_idx, col_idx]
                if value != current_value:
                    current_path.insert(0, value)
                    current_value = value

            horizontal_headers.append("$".join(current_path))
        return horizontal_headers

    def _get_vertical_headers(self, sheet: pd.DataFrame):
        """Получает вертикальные заголовки.(строки)"""
        return sheet.iloc[self.start_data_row:, self.vertical_header_col].dropna().tolist()

    def create_data(self, sheet: pd.DataFrame, horizontal_headers: list, vertical_headers: list):
        """Создает структурированные данные."""
        data = []
        for col_idx, column_header in enumerate(horizontal_headers, start=1):
            column_values = []
            for row_idx, row_header in enumerate(vertical_headers):
                cell_value = sheet.iloc[row_idx + self.start_data_row, col_idx]
                column_values.append({"row_header": row_header, "value": cell_value})
            data.append({"column_header": column_header, "values": column_values})
        return data

    def parse(self, sheet: pd.DataFrame) -> Dict:
        """Основной метод парсинга."""

        sheet = NotesProcessor.process_notes(sheet, raw_quantity=self.header_row_range[1])
        horizontal_headers, vertical_headers = self.parse_headers(sheet)
        self.data = self.create_data(sheet, horizontal_headers, vertical_headers)
        return {
            "headers": {
                "vertical": vertical_headers,
                "horizontal": horizontal_headers
            },
            "data": self.data
        }

    def _remove_newlines_from_headers(self, headers: list):
        """Удаляет переносы строк в заголовках."""
        return [header.replace("\n", " ") for header in headers]

    def generate_flat_data(
            self,
            year: int,
            city: str,
            sheet_name: str
    ) -> List[Dict[str, Union[str, float]]]:
        """Генерирует плоскую структуру данных."""
        flat_data = []

        if not self.data:
            raise ValueError("Данные не были распаршены. Сначала вызовите parse()")

        for column in self.data:
            column_header = column.get("column_header", "")
            values = column.get("values", [])

            for row in values:
                if row.get("value") == _SERVICE_EMPTY:
                    continue

                flat_record = {
                    "year": year,
                    "city": city,
                    "section": sheet_name,
                    "row": row.get("row_header", ""),
                    "column": column_header,
                    "value": row.get("value", 0)
                }
                flat_data.append(flat_record)

        print(f"[DEBUG] Сгенерировано {len(flat_data)} записей")
        return flat_data