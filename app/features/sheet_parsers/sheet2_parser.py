from app.features.sheet_parsers.base_sheet_parser import BaseSheetParser

class Sheet2Parser(BaseSheetParser):
    def parse(self, sheet):
        """
        Парсит данные с листа 1.
        """
        # Логика для парсинга листа 1
        return {'Раздел2':'data'}