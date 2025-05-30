import pandas as pd

from app.features.sheet_parsers.base_sheet_parser import BaseSheetParser


class Sheet3Parser(BaseSheetParser):
    def __init__(self):
        super().__init__(
            header_row_range=(1, 4),  # Диапазон строк для горизонтальных заголовков
            vertical_header_col=0,  # Номер колонки для вертикальных заголовков
            start_data_row=5  # Строка начала данных
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
