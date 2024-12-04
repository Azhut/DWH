from app.features.files.sheet_parsers.base_sheet_parser import BaseSheetParser

class Sheet5Parser(BaseSheetParser):
    def parse(self, sheet):
        """
        Парсит данные с листа 1.
        """
        # Логика для парсинга листа 1
        return {'Раздел5':'data'}
