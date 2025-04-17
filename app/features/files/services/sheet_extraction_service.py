from io import BytesIO

import pandas as pd
from fastapi import UploadFile, HTTPException


class SheetExtractionService:
    async def extract(self, file: UploadFile) -> list:
        try:
            file_content = await file.read()
            file_stream = BytesIO(file_content)
            df_dict = pd.read_excel(file_stream, sheet_name=None)
            sheets = [{"sheet_name": sheet_name, "data": df} for sheet_name, df in df_dict.items()]
            return sheets
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Не удалось извлечь листы из файла: {str(e)}")
