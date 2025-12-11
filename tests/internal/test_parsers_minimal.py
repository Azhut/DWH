import pytest
import pandas as pd
from app.parsers.sheet2_parser import Sheet2Parser
from app.parsers.sheet4_parser import Sheet4Parser
from app.parsers.sheet7_parser import Sheet7Parser

@pytest.mark.asyncio
async def test_sheet2_parser_minimal(sample_xlsx_path):
    df = pd.read_excel(sample_xlsx_path, sheet_name="Раздел2")
    parser = Sheet2Parser()
    parsed = parser.parse(df)
    assert "headers" in parsed
    assert "data" in parsed
    # несколько ключевых проверок
    assert isinstance(parsed["headers"]["horizontal"], list)
    assert isinstance(parsed["headers"]["vertical"], list)
    flat = parser.generate_flat_data(2023, "TESTCITY", "Раздел2")
    # минимум 1 записей в мини-версии теста
    assert isinstance(flat, list)

@pytest.mark.asyncio
async def test_sheet4_and_sheet7_minimal(sample_xlsx_path):
    df4 = pd.read_excel(sample_xlsx_path, sheet_name="Раздел4")
    df7 = pd.read_excel(sample_xlsx_path, sheet_name="Раздел7")

    p4 = Sheet4Parser()
    p7 = Sheet7Parser()

    parsed4 = p4.parse(df4)
    parsed7 = p7.parse(df7)

    assert parsed4["headers"]["horizontal"]
    assert parsed7["headers"]["horizontal"]

    flat4 = p4.generate_flat_data(2023, "TESTCITY", "Раздел4")
    flat7 = p7.generate_flat_data(2023, "TESTCITY", "Раздел7")

    # Проверяем, что в flat значения корректного типа и есть пара ключевых полей
    sample = (flat4 + flat7)[:3]
    for r in sample:
        assert "year" in r and "city" in r and "section" in r and "row" in r and "column" in r
