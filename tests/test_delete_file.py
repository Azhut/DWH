import requests
from pymongo import MongoClient

BASE_URL = "http://localhost:2700/api/v2/files"
MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "sport_data"
COLLECTION_NAME = "Files"
file_id = "АЛАПАЕВСК 2020.xls"


def test_delete_existing_file():

    # setup_file(file_id)
    resp = requests.delete(f"{BASE_URL}/{file_id}")
    print("Status code:", resp.status_code)
    print("Response body:", resp.json())
    assert resp.status_code == 200
    assert resp.json()["detail"] == f"Запись файла '{file_id}' успешно удалена"

    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    assert db[COLLECTION_NAME].find_one({"file_id": file_id}) is None
    client.close()


def test_delete_nonexistent_file():
    file_id = "NON_EXISTENT_FILE"
    resp = requests.delete(f"{BASE_URL}/{file_id}")
    print("Status code:", resp.status_code)
    print("Response body:", resp.json())
    assert resp.status_code == 404
    assert "не найден" in resp.json()["detail"]


def test_invalid_method():
    file_id = "TEST_INVALID_METHOD"
    resp = requests.post(f"{BASE_URL}/{file_id}")
    print("Status code:", resp.status_code)
    print("Response body:", resp.text)
    assert resp.status_code == 405


if __name__ == "__main__":
    test_delete_existing_file()
    test_delete_nonexistent_file()
    test_invalid_method()
    print("✅ Все тесты прошли успешно")
