import pandas as pd
from fastapi import UploadFile
from typing import List
from io import BytesIO  # Необходимо для оборачивания байтов в BytesIO

class SheetExtractionService:
    @staticmethod
    async def extract_sheets(file: UploadFile) -> List[dict]:
        """
        Извлекает листы и данные из Excel-файла.

        :param file: Excel-файл
        :return: Список листов с их названиями и содержимым
        """
        try:
            file_content = await file.read()  # Получаем байты файла
            file_stream = BytesIO(file_content)  # Оборачиваем байты в BytesIO для использования с pandas

            # Теперь можно передавать в pd.ExcelFile объект BytesIO
            excel_data = pd.ExcelFile(file_stream)

            sheets = []
            for sheet_name in excel_data.sheet_names:
                sheet_data = excel_data.parse(sheet_name).to_dict(orient="records")
                sheets.append({"sheet_name": sheet_name, "data": sheet_data})

            return sheets
        except Exception as e:
            raise Exception(f"Ошибка извлечения данных из файла: {str(e)}")
