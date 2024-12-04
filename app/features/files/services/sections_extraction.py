import pandas as pd
from io import BytesIO

class SheetExtractionService:
    """
    Парсер Excel-файлов для разделения данных по листам.
    """
    @staticmethod
    def parse_stream(file_stream: bytes):
        """
        Разделяет файл на несколько секций по листам из потока данных.
        :param file_stream: Поток данных Excel-файла (в байтах).
        :return: Словарь с данными по листам.
        """
        excel_data = pd.ExcelFile(BytesIO(file_stream))
        return SheetExtractionService._extract_sheets(excel_data)

    @staticmethod
    def _extract_sheets(excel_data: pd.ExcelFile):
        """
        Вспомогательный метод для извлечения секций из объекта ExcelFile.
        :param excel_data: Объект ExcelFile.
        :return: Словарь с данными по листам.
        """
        sections = {}
        for sheet_name in excel_data.sheet_names:
            df = pd.read_excel(excel_data, sheet_name=sheet_name, header=None)
            sections[sheet_name] = df
        return sections
