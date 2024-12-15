import pandas as pd

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

    def parse_headers(self, sheet: pd.DataFrame):
        """
        Парсит заголовки с листа.
        """

        # 1. Пропускаем строки для верхних заголовков
        header_rows = sheet.iloc[self.header_row_range[0]:self.header_row_range[1]].fillna("")

        # 2. Обработка заголовков с последней строки вниз
        for row_idx in range(len(header_rows) - 1, 0, -1):
            for col_idx in range(header_rows.shape[1]):
                if header_rows.iloc[row_idx, col_idx] == "":
                    # Поднимаемся вверх по строкам, пока не найдем не пустую ячейку
                    for search_row in range(row_idx - 1, -1, -1):
                        if header_rows.iloc[search_row, col_idx] != "":
                            header_rows.iloc[row_idx, col_idx] = header_rows.iloc[search_row, col_idx]
                            break

        # 3. Заполняем пустые ячейки в строках слева направо по всем строкам в диапазоне
        for row_idx in range(self.header_row_range[0]-1, header_rows.shape[0]):  # Проходим все строки диапазона
            for col_idx in range(1, header_rows.shape[1]):  # Пропускаем первую колонку
                if header_rows.iloc[row_idx, col_idx] == "":
                    header_rows.iloc[row_idx, col_idx] = header_rows.iloc[row_idx, col_idx - 1]

        # 4. Формирование уникальных горизонтальных заголовков через разделители
        horizontal_headers = []
        for col_idx in range(1, header_rows.shape[1]):  # Начинаем с 1-го столбца
            current_path = []
            current_value = header_rows.iloc[len(header_rows) - 1, col_idx]  # Начинаем с последней строки
            current_path.append(current_value)  # Добавляем текущую ячейку с символом $

            # Идем по строкам вверх, пока значения одинаковые
            for row_idx in range(len(header_rows) - 2, -1, -1):
                value = header_rows.iloc[row_idx, col_idx]
                if value != current_value:  # Когда встречаем отличное значение
                    current_path.insert(0, value)  # Добавляем его в начало пути
                    current_value = value  # Обновляем текущее значение

            # Соединяем все части пути
            horizontal_headers.append("$".join(current_path))

        # 5. Вертикальные заголовки (из первой колонки)
        vertical_headers = sheet.iloc[self.start_data_row:, self.vertical_header_col].dropna().tolist()

        return horizontal_headers, vertical_headers

    def create_data(self, sheet: pd.DataFrame, horizontal_headers: list, vertical_headers: list):
        """
        Универсальный метод для создания данных, используя горизонтальные и вертикальные заголовки.
        """
        data = []
        for col_idx, column_header in enumerate(horizontal_headers, start=1):  # Начинаем с 1-го столбца
            column_values = []
            for row_idx, row_header in enumerate(vertical_headers):
                cell_value = sheet.iloc[row_idx + self.start_data_row, col_idx]  # Данные начинаются с строки start_data_row
                column_values.append({"row_header": row_header, "value": cell_value})
            data.append({"column_header": column_header, "values": column_values})

        return data

    def parse(self, sheet: pd.DataFrame):
        """
        Основной метод для парсинга.
        """
        horizontal_headers, vertical_headers = self.parse_headers(sheet)
        data = self.create_data(sheet, horizontal_headers, vertical_headers)
        return {
            "headers": {
                "vertical": vertical_headers,
                "horizontal": horizontal_headers
            },
            "data": data
        }
