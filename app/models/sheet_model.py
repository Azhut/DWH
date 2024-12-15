from pydantic import BaseModel, root_validator
from typing import List, Dict, Optional, Union


class SheetModel(BaseModel):
    file_id: str
    sheet_name: str
    sheet_fullname: str
    year: int
    city: str
    headers: Dict[str, List[str]]
    data: List[Dict[str, Union[str, List[Dict[str, Optional[Union[str, float]]]]]]]

    @root_validator(pre=True)
    def convert_column_header_to_list(cls, values):
        # Преобразуем column_header в строку, если это список
        if 'data' in values:
            for column in values['data']:
                # Преобразуем column_header в строку, если это список
                if isinstance(column.get('column_header'), list):
                    column['column_header'] = " ".join(column['column_header'])

                # Убедимся, что values остаются списком словарей
                if isinstance(column.get('values'), list):
                    for row in column['values']:
                        # Преобразуем row_header и value в строку, если они не None
                        row['row_header'] = str(row.get('row_header', ''))
                        row['value'] = str(row.get('value', '')) if row['value'] is not None else None

        # Преобразуем вертикальные и горизонтальные заголовки в строку
        if 'headers' in values:
            headers = values['headers']
            if 'vertical' in headers:
                headers['vertical'] = [str(item) for item in headers['vertical']]
            if 'horizontal' in headers:
                headers['horizontal'] = [str(item) for item in headers['horizontal']]

        return values
