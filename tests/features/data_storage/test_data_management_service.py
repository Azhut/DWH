import pytest

from mongomock import MongoClient as MockMongoClient
from app.features.data_storage.data_management_service import DataManagementService
from app.models.sheet_model import SheetModel

@pytest.fixture
def mock_db():
    return MockMongoClient().db

@pytest.fixture
def data_management_service(mock_db):
    return DataManagementService(mock_db)

def test_process_and_save_all(data_management_service):
    # Входные данные для теста
    file_id = "test_file_id"
    sheet_models = [
        SheetModel(
            file_id="test_file_id",
            sheet_name="Sheet1",
            data={
                2021: {
                    "CityA": {
                        "headers": ["Header1", "Header2"],
                        "rows": [[1, 2], [3, 4]]
                    },
                    "CityB": {
                        "headers": ["Header1", "Header2"],
                        "rows": [[5, 6], [7, 8]]
                    }
                }
            }

        )
    ]


    # Выполнение тестируемого метода
    data_management_service.process_and_save_all(sheet_models, file_id)

    # Проверка коллекции Sheets
    sheets_docs = list(data_management_service.sheets_collection.find({}))
    assert sheets_docs[0]["file_id"] == file_id

    # Проверка коллекции DataTables
    data_table_docs = list(data_management_service.data_tables_collection.find({}))


    # Проверка коллекции CitiesAndYears
    cities_and_years_docs = list(data_management_service.cities_and_years_collection.find({}))


    # Проверка логов
    log_docs = list(data_management_service.logs_collection.find({}))

    assert "Successfully processed" in log_docs[0]["message"]
