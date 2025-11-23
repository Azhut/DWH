import requests
from pymongo import MongoClient

# URL твоего API
BASE_URL = "http://localhost:2700/api/v2/files"

# Настройки MongoDB (как у тебя в config)
MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "sport_data"


def test_delete_specific_file():

    file_id = "6690d3b2-0b27-4fc1-995a-db1731e46345"   # конкретный существующий ID

    # --- 1. Сначала убедимся, что данные существуют ----------------------------

    mongo = MongoClient(MONGO_URI)
    db = mongo[DB_NAME]

    file_doc = db.Files.find_one({"file_id": file_id})
    flat_doc = db.FlatData.find_one({"file_id": file_id})

    assert file_doc is not None, "Файл с таким file_id отсутствует в Files — тест невозможен"
    assert flat_doc is not None, "Связанные flat_data отсутствуют — тест невозможен"

    # --- 2. Вызываем DELETE -----------------------------------------------------

    response = requests.delete(f"{BASE_URL}/{file_id}")

    print("Код ответа:", response.status_code)
    print("Тело ответа:", response.text)

    assert response.status_code == 200, f"Ожидался 200, получили {response.status_code}"

    json_data = response.json()
    assert "detail" in json_data, "Нет поля detail в ответе API"

    # --- 3. Проверяем, что файл удалён -----------------------------------------

    file_after = db.Files.find_one({"file_id": file_id})
    assert file_after is None, "Файл НЕ удалён из коллекции Files!"

    # --- 4. Проверяем, что связанные flat_data удалены -------------------------

    flat_after = db.FlatData.find_one({"file_id": file_id})
    assert flat_after is None, "FlatData НЕ удалены!"

    print("Удаление прошло успешно.")


if __name__ == "__main__":
    test_delete_specific_file()
