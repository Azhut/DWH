from io import BytesIO

import pandas as pd
from fastapi import UploadFile

from app.core.exception_handler import log_and_raise_http, logger


class SheetExtractionService:
    async def extract(self, file: UploadFile) -> list:
        try:
            file_content = await file.read()
            file_stream = BytesIO(file_content)
            df_dict = pd.read_excel(file_stream, sheet_name=None, dtype=str)
            logger.info(f"Извлечено {len(df_dict)} листов из файла {file.filename}")
            sheets = [{"sheet_name": sheet_name, "data": df} for sheet_name, df in df_dict.items()]
            return sheets
        except Exception as e:
            log_and_raise_http(400, "Не удалось извлечь листы из файла", e)