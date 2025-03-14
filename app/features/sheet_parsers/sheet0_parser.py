from .base_sheet_parser import BaseSheetParser
import pandas as pd


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
        # Вызываем базовый парсинг
        parsed_data = super().parse(sheet)

        # Специфичные преобразования для Sheet0 (если нужны)
        parsed_data["headers"]["horizontal"] = self._clean_headers(
            parsed_data["headers"]["horizontal"]
        )

        return parsed_data

    def _clean_headers(self, headers: list) -> list:
        """Дополнительная очистка заголовков для Sheet0"""
        return [h.replace("_x000D_", "") for h in headers]