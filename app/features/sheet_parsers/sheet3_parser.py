import pandas as pd

from app.features.sheet_parsers.base_sheet_parser import BaseSheetParser


class Sheet3Parser(BaseSheetParser):
    def __init__(self):
        # Указываем конкретные параметры для этого парсера
        super().__init__(header_row_range=(1, 4), vertical_header_col=0, start_data_row=5)

    def parse_data(self, sheet: pd.DataFrame, horizontal_headers: list, vertical_headers: list):
        """
        Парсит данные с 3 листа.
        """

        return self.create_data(sheet, horizontal_headers, vertical_headers)
