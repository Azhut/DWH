import pandas as pd

from app.parsers.base_sheet_parser import BaseSheetParser


class Sheet4Parser(BaseSheetParser):
    def __init__(self):
        super().__init__(
            header_row_range=(1, 5),  # Диапазон строк для горизонтальных заголовков
            vertical_header_col=0,  # Номер колонки для вертикальных заголовков
            start_data_row=6  # Строка начала данных
        )

    def parse(self, sheet: pd.DataFrame) -> dict:
        """
        Основной метод парсинга с дополнительной обработкой специфичной для Раздела1
        """

        parsed_data = super().parse(sheet)


        parsed_data["headers"]["horizontal"] = self._clean_specific_headers(
            parsed_data["headers"]["horizontal"]
        )

        return parsed_data
