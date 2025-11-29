import requests
import pytest
from datetime import datetime

BASE_URL = "http://localhost:2700/api/v2/files" # Измененный URL!
# BASE_URL = "http://5.165.236.240:2700/api/v2/files" # Измененный URL!


def print_response(response):
    print(f"\nURL: {response.url}")
    print(f"Status code: {response.status_code}")
    print("Response body:")
    print(response.text)
    try:
        return response.json()
    except:
        return None


def test_list_files_basic():
    response = requests.get(BASE_URL)
    data = print_response(response)

    assert response.status_code == 200
    assert isinstance(data, list)
    assert len(data) <= 100


def test_pagination_parameters():
    response = requests.get(f"{BASE_URL}?limit=5&offset=10")
    data = print_response(response)

    assert response.status_code == 200
    assert isinstance(data, list)
    assert len(data) <= 5


def test_response_structure():
    response = requests.get(f"{BASE_URL}?limit=1")
    data = print_response(response)

    if response.status_code == 200 and data:
        item = data[0]
        assert all(key in item for key in [
            "filename", "status", "error",
            "upload_timestamp", "updated_at"
        ])


def test_error_handling():
    response = requests.get(f"{BASE_URL}?limit=-5&offset=-1")
    data = print_response(response)

    assert response.status_code == 422
    if data:
        assert any(e["loc"][1] == "limit" for e in data.get("detail", []))


def test_file_filtering():
    response = requests.get(f"{BASE_URL}?year=2019")
    data = print_response(response)

    # Если фильтрация не реализована, тест должен быть пропущен
    if response.status_code == 422:
        pytest.skip("Фильтрация по году не реализована")
    elif response.status_code == 200:
        for item in data:
            assert item["year"] == 2019


def test_unique_files():
    response1 = requests.get(f"{BASE_URL}?limit=5&offset=0")
    response2 = requests.get(f"{BASE_URL}?limit=5&offset=5")

    data1 = print_response(response1)
    data2 = print_response(response2)

    if response1.status_code == 200 and response2.status_code == 200:
        ids1 = {item["filename"] for item in data1}
        ids2 = {item["filename"] for item in data2}
        assert ids1.isdisjoint(ids2)