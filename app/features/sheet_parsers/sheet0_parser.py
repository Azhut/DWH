import pandas as pd

from .base_sheet_parser import BaseSheetParser


class Sheet0Parser(BaseSheetParser):
    def __init__(self):
        super().__init__(
            header_row_range=(1, 4),
            vertical_header_col=0,
            start_data_row=4
        )

    def parse(self, sheet: pd.DataFrame) -> dict:
        """
        Переопределяем метод parse для добавления специфичной логики
        """

        parsed_data = super().parse(sheet)


        parsed_data["headers"]["horizontal"] = self._clean_headers(
            parsed_data["headers"]["horizontal"]
        )

        return parsed_data

    def _clean_headers(self, headers: list) -> list:
        """Дополнительная очистка заголовков для Sheet0"""
        return [h.replace("_x000D_", "") for h in headers]