from typing import List

from fastapi import UploadFile, HTTPException

from app.core.logger import logger
from app.features.files.services.sheet_extraction_service import SheetExtractionService
from app.features.sheet_parsers.parsers import get_sheet_parser
from app.models.sheet_model import SheetModel


class SheetProcessor:
    def __init__(self):
        self.sheet_extractor = SheetExtractionService()

    async def extract_and_process_sheets(self, file: UploadFile, city: str, year: int) -> List[SheetModel]:
        sheets = await self.sheet_extractor.extract(file)
        return await self._process_sheets(file.filename, sheets, city, year)

    async def _process_sheets(self, file_id: str, sheets: List[dict], city: str, year: int) -> List[SheetModel]:
        sheet_models = []

        for sheet in sheets:
            if sheet["sheet_name"] == 'Раздел0':
                continue

            try:
                parser = get_sheet_parser(sheet["sheet_name"])
                parsed_data = parser.parse(sheet["data"])

                sheet_model = self._build_sheet_model(file_id, sheet, parsed_data, city, year)
                sheet_models.append(sheet_model)

            except Exception as e:
                logger.error(f"Error parsing sheet {sheet['sheet_name']}: {str(e)}")
                raise HTTPException(status_code=400, detail=f"Error processing sheet {sheet['sheet_name']}")

        return sheet_models

    def _build_sheet_model(self, file_id: str, sheet: dict, parsed_data: dict, city: str, year: int) -> SheetModel:
        headers = parsed_data.get("headers", {})
        data = parsed_data.get("data", [])

        return SheetModel(
            file_id=file_id,
            sheet_name=sheet["sheet_name"],
            sheet_fullname=sheet.get("sheet_fullname", sheet["sheet_name"]),
            year=year,
            city=city,
            headers={
                "vertical": headers.get("vertical", []),
                "horizontal": headers.get("horizontal", [])
            },
            data=[
                {
                    "column_header": col_data["column_header"],
                    "values": [
                        {"row_header": row["row_header"], "value": row["value"]}
                        for row in col_data["values"]
                    ]
                }
                for col_data in data
            ]
        )


