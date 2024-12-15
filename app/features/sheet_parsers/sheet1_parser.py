from app.features.sheet_parsers.base_sheet_parser import BaseSheetParser


class Sheet1Parser(BaseSheetParser):
    def parse(self, sheet):
        """
        Парсит данные с листа 1.

        :param sheet: Данные листа в виде DataFrame
        :return: Словарь с заголовками и данными в формате:
            {
                "headers": {
                    "vertical": ["RowHeader1", "RowHeader2", ...],
                    "horizontal": ["Header1.SubHeaderA", "Header2.SubHeaderB", ...]
                },
                "data": [
                    {
                        "column_header": "Header1.SubHeaderA",
                        "values": [
                            {"row_header": "RowHeader1", "value": 123.45},
                            {"row_header": "RowHeader2", "value": 67.89}
                        ]
                    },
                    ...
                ]
            }
        """
        # Извлечение вертикальных заголовков
        vertical_headers = sheet.iloc[1:, 0].dropna().tolist()  # Пропускаем первую строку, где нет данных

        # Извлечение горизонтальных заголовков с уровнем
        horizontal_headers = [
            ".".join(str(col).strip() for col in header_group if col)
            for header_group in sheet.iloc[0:1].to_dict("list").values()
        ]

        # Извлечение данных
        data = []
        for col_index, column_header in enumerate(horizontal_headers, start=1):
            column_values = []
            for row_index, row_header in enumerate(vertical_headers):
                cell_value = sheet.iloc[row_index + 1, col_index]
                column_values.append({"row_header": row_header, "value": cell_value})
            data.append({"column_header": column_header, "values": column_values})

        return {
            "headers": {
                "vertical": vertical_headers,
                "horizontal": horizontal_headers
            },
            "data": data
        }
