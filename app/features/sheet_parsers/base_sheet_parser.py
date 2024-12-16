from typing import Tuple

import pandas as pd

class BaseSheetParser:
    def __init__(self, header_row_range: Tuple[int, int], vertical_header_col: int, start_data_row: int):
        self.header_row_range = header_row_range
        self.vertical_header_col = vertical_header_col
        self.start_data_row = start_data_row

    def parse(self, sheet: pd.DataFrame) -> dict:
        horizontal_headers, vertical_headers = self._parse_headers(sheet)
        data = self._create_data(sheet, horizontal_headers, vertical_headers)
        return {
            "headers": {
                "vertical": vertical_headers,
                "horizontal": horizontal_headers
            },
            "data": data
        }

    def _parse_headers(self, sheet: pd.DataFrame) -> Tuple[list, list]:
        header_rows = self._prepare_header_rows(sheet)
        horizontal_headers = self._generate_horizontal_headers(header_rows)
        vertical_headers = self._extract_vertical_headers(sheet)
        return horizontal_headers, vertical_headers

    def _prepare_header_rows(self, sheet: pd.DataFrame) -> pd.DataFrame:
        header_rows = sheet.iloc[self.header_row_range[0]:self.header_row_range[1]].fillna("")
        self._fill_empty_cells(header_rows)
        return header_rows

    @staticmethod
    def _fill_empty_cells(header_rows: pd.DataFrame):
        for row_idx in range(len(header_rows) - 1, 0, -1):
            for col_idx in range(header_rows.shape[1]):
                if header_rows.iloc[row_idx, col_idx] == "":
                    for search_row in range(row_idx - 1, -1, -1):
                        if header_rows.iloc[search_row, col_idx] != "":
                            header_rows.iloc[row_idx, col_idx] = header_rows.iloc[search_row, col_idx]
                            break
        for row_idx in range(len(header_rows)):
            for col_idx in range(1, header_rows.shape[1]):
                if header_rows.iloc[row_idx, col_idx] == "":
                    header_rows.iloc[row_idx, col_idx] = header_rows.iloc[row_idx, col_idx - 1]

    def _generate_horizontal_headers(self, header_rows: pd.DataFrame) -> list:
        horizontal_headers = []
        for col_idx in range(1, header_rows.shape[1]):
            path = []
            for row_idx in range(len(header_rows) - 1, -1, -1):
                value = header_rows.iloc[row_idx, col_idx]
                if value:
                    path.insert(0, value)
            horizontal_headers.append("$".join(path))
        return horizontal_headers

    def _extract_vertical_headers(self, sheet: pd.DataFrame) -> list:
        return sheet.iloc[self.start_data_row:, self.vertical_header_col].dropna().tolist()

    def _create_data(self, sheet: pd.DataFrame, horizontal_headers: list, vertical_headers: list) -> list:
        data = []
        for col_idx, column_header in enumerate(horizontal_headers, start=1):
            column_values = []
            for row_idx, row_header in enumerate(vertical_headers):
                cell_value = sheet.iloc[row_idx + self.start_data_row, col_idx]
                column_values.append({"row_header": row_header, "value": cell_value})
            data.append({"column_header": column_header, "values": column_values})
        return data
