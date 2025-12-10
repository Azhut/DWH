import pandas as pd
from app.parsers.base_sheet_parser import BaseSheetParser
from app.parsers.sheet3_parser import Sheet3Parser
from app.parsers.sheet4_parser import Sheet4Parser

def make_test_df():
    # формируем минимально необходимые строки/колонки, но в виде строк
    data = {
        "VHeader": ["h1", "h2", "row1", "row2", "row3"],
        "C1": ["", "", "1", "2", "3"],
        "C2": ["", "", "4", "5", "6"]
    }
    df = pd.DataFrame(data)
    return df

def test_base_parser_headers_extracted():
    df = make_test_df()
    parser = BaseSheetParser(header_row_range=(0,2), vertical_header_col=0, start_data_row=2)
    parsed = parser.parse(df)
    assert "headers" in parsed and "data" in parsed
    assert isinstance(parsed["headers"]["horizontal"], list)
    assert isinstance(parsed["headers"]["vertical"], list)

def test_sheet3_parser_clean_headers():
    df = make_test_df()
    parser = Sheet3Parser()
    parsed = parser.parse(df)
    assert isinstance(parsed["headers"]["horizontal"], list)

def test_sheet4_parser_clean_headers():
    df = make_test_df()
    parser = Sheet4Parser()
    parsed = parser.parse(df)
    assert isinstance(parsed["headers"]["horizontal"], list)
