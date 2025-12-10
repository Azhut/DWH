from app.models.file_model import FileModel, FileStatus
from app.models.sheet_model import SheetModel

def test_file_model_create_new():
    fm = FileModel.create_new(filename="test.xlsx", year=2022, city="City")
    assert isinstance(fm.file_id, str)
    assert fm.filename == "test.xlsx"
    assert fm.year == 2022
    assert fm.city == "City"
    assert fm.status == FileStatus.PROCESSING
    assert fm.error is None
    assert isinstance(fm.upload_timestamp, type(fm.updated_at))
    assert fm.sheets == []
    assert fm.size == 0

def test_file_model_create_stub():
    stub = FileModel.create_stub(file_id="123", filename="f.xls", error_message="err", year=2021, city="City")
    assert stub.file_id == "123"
    assert stub.filename == "f.xls"
    assert stub.status == FileStatus.FAILED
    assert stub.error == "err"
    assert stub.year == 2021
    assert stub.city == "City"

def test_sheet_model_root_validator():
    # Проверим, что строки-числа конвертируются в строки
    sheet_data = [
        {"column_header": ["Col1", "Col2"], "values": [{"row_header": "R", "value": 10.5}]}
    ]
    headers = {"vertical": [1, 2], "horizontal": [3, 4]}
    sm = SheetModel(
        file_id="fid",
        sheet_name="Sheet",
        sheet_fullname="FullName",
        year=2020,
        city="C",
        headers=headers,
        data=sheet_data
    )
    # Проверяем, что числовые данные станут строками
    for col in sm.data:
        for row in col["values"]:
            assert isinstance(row["row_header"], str)
            assert isinstance(row["value"], str)
    # В headers все элементы должны быть строками
    assert all(isinstance(x, str) for x in sm.headers["vertical"])
    assert all(isinstance(x, str) for x in sm.headers["horizontal"])
