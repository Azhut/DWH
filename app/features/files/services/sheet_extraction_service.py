from fastapi import UploadFile, HTTPException
import pandas as pd

class SheetExtractionService:
    async def extract(self, file: UploadFile) -> list:
        try:
            df_dict = pd.read_excel(file.file, sheet_name=None)
            sheets = [{"sheet_name": sheet_name, "data": df} for sheet_name, df in df_dict.items()]
            return sheets
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Failed to extract sheets: {str(e)}")
