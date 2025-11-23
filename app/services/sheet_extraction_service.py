from io import BytesIO
import pandas as pd
from fastapi import UploadFile
from app.core.exceptions import log_and_raise_http
from app.core.logger import logger
from app.services.rounding_service import RoundingService

class SheetExtractionService:
    async def extract(self, file: UploadFile) -> list:
        try:
            content = await file.read()
            excel_data = BytesIO(content)
            df_dict = pd.read_excel(excel_data, sheet_name=None)
            sheets = []
            for name, df in df_dict.items():
                rounded = RoundingService.round_dataframe(name, df)
                sheets.append({"sheet_name": name, "data": rounded})
            logger.info(f"Извлечено {len(sheets)} листов из файла {file.filename}")
            return sheets
        except Exception as e:
            log_and_raise_http(400, "Не удалось извлечь листы из файла", e)
