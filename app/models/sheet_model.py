from typing import List, Dict
from pydantic import BaseModel

class SheetModel(BaseModel):
    sheet_name: str                   # Имя листа
    sheet_fullname: str               # Полное имя листа (возможно, с детализацией)
    year: str
    city: str
    file_id: str                      # Ссылка на идентификатор файла, которому принадлежит лист
    headers: Dict[str, List[str]]     # Заголовки: "vertical" и "horizontal"
    data: List[Dict[str, List[Dict]]] # Данные, организованные по верхним заголовкам

    # Пример структуры:
    # headers = {
    #   "vertical": ["RowHeader1", "RowHeader2", "..."],
    #   "horizontal": ["Header1.SubHeaderA", "Header2.SubHeaderB", "..."]
    # }
    # data = [
    #   {
    #     "column_header": "Header1.SubHeaderA",
    #     "values": [
    #       {"row_header": "RowHeader1", "value": 123.45},
    #       {"row_header": "RowHeader2", "value": 67.89}
    #     ]
    #   },
    #   {
    #     "column_header": "Header2.SubHeaderB",
    #     "values": [
    #       {"row_header": "RowHeader1", "value": 45.67},
    #       {"row_header": "RowHeader2", "value": 0}
    #     ]
    #   }
    # ]
