import requests
import pytest
from pymongo import MongoClient
from app.core.config import settings

TEST_URL = "http://localhost:2700/api/v2/files/upload"
TEST_FILE_PATH = '../tests/data/АЛАПАЕВСК 2020.xlsx'


def test_db_connection(mongo_client):
    db = mongo_client[settings.DATABASE_NAME]
    assert db.command("ping")['ok'] == 1.0, "Нет подключения к MongoDB"

@pytest.fixture(scope="module")
def mongo_client():
    client = MongoClient(settings.DATABASE_URI)
    yield client
    client.close()


@pytest.fixture(scope="module")
def setup_database(mongo_client):


    # Загрузка тестовых данных
    with open(TEST_FILE_PATH, 'rb') as f:
        files = [('files', f)]
        response = requests.post(TEST_URL, files=files)
        assert response.status_code == 200

    yield




def test_file_upload(mongo_client, setup_database):
    db = mongo_client[settings.DATABASE_NAME]

    # Проверка загрузки файла
    with open(TEST_FILE_PATH, 'rb') as f:
        response = requests.post(TEST_URL, files={'files': f})
        print("Response:", response.json())  # Добавляем вывод ответа

    # Проверка коллекции Sheets
    sheets = list(db.Sheets.find())
    print("Sheets documents:", sheets)

    # Проверка коллекции FlatData
    flat_data = list(db.FlatData.find())
    print("FlatData documents:", flat_data)

    # Проверки
    assert len(sheets) > 0, "Данные не сохранены в Sheets"
    assert len(flat_data) > 0, "Данные не сохранены в FlatData"
    # Проверка сохранения в MongoDB
    db = mongo_client[settings.DATABASE_NAME]

    # Проверка основной коллекции
    sheets_count = db.Sheets.count_documents({})
    assert sheets_count > 0, "Нет данных в коллекции Sheets"

    # Проверка новой коллекции плоских данных
    flat_data_count = db.FlatData.count_documents({})
    assert flat_data_count > 0, "Нет данных в коллекции FlatData"

    # Проверка структуры данных
    first_sheet = db.Sheets.find_one()
    assert first_sheet is not None
    assert 'year' in first_sheet
    assert 'city' in first_sheet

    first_flat = db.FlatData.find_one()
    assert first_flat is not None
    assert all(key in first_flat for key in ['year', 'city', 'section', 'row', 'column', 'value'])