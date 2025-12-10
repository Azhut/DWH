from typing import List, Dict, Optional, Union
from pydantic import BaseModel, ConfigDict


class SheetModel(BaseModel):
    file_id: str
    sheet_name: str
    sheet_fullname: str
    year: int
    city: str
    headers: Dict[str, List[str]]
    data: List[Dict[str, Union[str, List[Dict[str, Optional[Union[str, float]]]]]]]

    model_config = ConfigDict(arbitrary_types_allowed=True)

    @classmethod
    def convert_column_header_to_list(cls, values):

        if 'data' in values:
            for column in values['data']:

                if isinstance(column.get('column_header'), list):
                    column['column_header'] = " ".join(column['column_header'])

                if isinstance(column.get('values'), list):
                    for row in column['values']:
                        row['row_header'] = str(row.get('row_header', ''))
                        row['value'] = str(row.get('value', '')) if row['value'] is not None else None

        if 'headers' in values:
            headers = values['headers']
            if 'vertical' in headers:
                headers['vertical'] = [str(item) for item in headers['vertical']]
            if 'horizontal' in headers:
                headers['horizontal'] = [str(item) for item in headers['horizontal']]

        return values
