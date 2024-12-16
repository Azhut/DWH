import pandas as pd

from app.features.sheet_parsers.base_sheet_parser import BaseSheetParser


class Sheet2Parser(BaseSheetParser):
    def __init__(self):
        super().__init__(header_row_range=(1, 5), vertical_header_col=0, start_data_row=6)

    def parse_data(self, sheet: pd.DataFrame, horizontal_headers: list, vertical_headers: list):
        return self.create_data(sheet, horizontal_headers, vertical_headers)
