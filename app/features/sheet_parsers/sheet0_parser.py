# from typing import Dict
#
# from app.features.sheet_parsers.base_sheet_parser import BaseSheetParser
#
# class Sheet0Parser(BaseSheetParser):
#     async def parse(self, sheet_data: Dict) -> Dict:
#         headers = sheet_data["values"]
#         rows = sheet_data["rows"]
#
#         return {
#             "headers": headers,
#             "rows": rows
#         }