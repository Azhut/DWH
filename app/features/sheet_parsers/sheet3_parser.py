from app.features.sheet_parsers.base_sheet_parser import BaseSheetParser

class Sheet3Parser(BaseSheetParser):
    def parse(self, sheet):
        """
        Парсит данные с листа 1.
        """
        # Логика для парсинга листа 1
        return {'Раздел3':'data'}
