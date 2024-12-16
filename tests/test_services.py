# test_endpoints.py
import requests

BASE_URL = "http://mmdwh.duckdns.org:8080/api/v1/documents"


def test_get_sections():
    url = f"{BASE_URL}/sections"

    response = requests.get(url)

    if response.status_code == 200:
        print(response.json())
    else:
        print(f"Ошибка: {response.status_code} - {response.text}")

    assert response.status_code == 200
    assert 'sections' in response.json()
    assert isinstance(response.json()['sections'], list)


def test_get_documents_info():
    url = f"{BASE_URL}/documents-info"
    payload = {
        "cities": ["АЛАПАЕВСК", "ИРБИТ"],
        "years": [2020, 2023]
    }

    response = requests.post(url, json=payload)

    if response.status_code == 200:
        print(response.json())
    else:
        print(f"Ошибка: {response.status_code} - {response.text}")

    assert response.status_code == 200
    assert 'cities' in response.json()
    assert 'years' in response.json()
    assert 'sections' in response.json()
    assert isinstance(response.json()['cities'], list)
    assert isinstance(response.json()['years'], list)
    assert isinstance(response.json()['sections'], list)


def test_get_documents_fields():
    url = f"{BASE_URL}/documents-fields"
    payload = {
        "section": "Раздел1",
        "cities": ["АЛАПАЕВСК"],
        "years": [2020]
    }

    response = requests.post(url, json=payload)

    if response.status_code == 200:
        print(response.json())
    else:
        print(f"Ошибка: {response.status_code} - {response.text}")

    assert response.status_code == 200
    assert 'rows' in response.json()
    assert 'columns' in response.json()
    assert isinstance(response.json()['rows'], list)
    assert isinstance(response.json()['columns'], list)


def test_get_documents():
    url = f"{BASE_URL}/documents"
    payload = {
        "section": "Раздел1",
        "cities": ["АЛАПАЕВСК"],
        "years": [2020],
        "rows": ["Всего штатных работников физической культуры и спорта  (сумма строк 02-09, 12-14)"],
        "columns": ["Всего"]
    }

    response = requests.post(url, json=payload)

    if response.status_code == 200:
        print(response.json())
    else:
        print(f"Ошибка: {response.status_code} - {response.text}")

    assert response.status_code == 200
    assert 'documents' in response.json()
    assert isinstance(response.json()['documents'], list)
    for document in response.json()['documents']:
        assert 'year' in document
        assert 'city' in document
        assert 'data' in document
