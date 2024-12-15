import pandas as pd


class Sheet1Parser():
    def parse(self, sheet: pd.DataFrame):
        """
        Парсит данные с листа 1, учитывая многоуровневые заголовки и их пропуски.

        :param sheet: Данные листа в виде DataFrame
        :return: Словарь с заголовками и данными
        """
        # 1. Пропускаем нулевую строку и работаем со строками 1-3 для верхних заголовков
        header_rows = sheet.iloc[1:4].fillna("")  # Выбираем строки 1-3, заменяя NaN на пустые строки

        # 2. Обрабатываем 3 строку сверху вниз, заполняя пропуски
        for row_idx in range(len(header_rows) - 1, 0, -1):  # Обходим строки с конца
            header_rows.iloc[row_idx] = header_rows.iloc[row_idx].combine_first(header_rows.iloc[row_idx - 1])

        # 3. Обрабатываем строки 1-2 слева направо, заполняя пропуски
        for row_idx in range(2):  # Проходим строки 1 и 2
            for col_idx in range(1, header_rows.shape[1]):  # Начинаем с 1 колонки
                if header_rows.iloc[row_idx, col_idx] == "":
                    header_rows.iloc[row_idx, col_idx] = header_rows.iloc[row_idx, col_idx - 1]

        # 4. Формируем горизонтальные заголовки (игнорируем первую колонку, где боковые заголовки)
        horizontal_headers = [
            ".".join(header_rows.iloc[:, col].tolist()).strip(".")  # Объединяем заголовки через "."
            for col in range(1, header_rows.shape[1])  # Начинаем с 1-го индекса
        ]

        # 5. Извлечение вертикальных заголовков из первой колонки (после 4 строки)
        vertical_headers = sheet.iloc[4:, 0].dropna().tolist()

        # 6. Формирование данных
        data = []
        for col_idx, column_header in enumerate(horizontal_headers, start=1):  # Начинаем с 1-го столбца
            column_values = []
            for row_idx, row_header in enumerate(vertical_headers):
                cell_value = sheet.iloc[row_idx + 4, col_idx]  # Данные начинаются с 5 строки (4-й индекс)
                column_values.append({"row_header": row_header, "value": cell_value})
            data.append({"column_header": column_header, "values": column_values})

        return {
            "headers": {
                "vertical": vertical_headers,
                "horizontal": horizontal_headers
            },
            "data": data
        }
