import requests

# BASE_URL = "http://localhost:2700/api/v2"
BASE_URL = "http://5.165.236.240:2700/api/v2"

def test_get_filters_names():
    url = f"{BASE_URL}/filters-names"
    response = requests.get(url)
    assert response.status_code == 200
    data = response.json()
    print(data)
    assert "filters" in data
    assert isinstance(data["filters"], list)
    assert "год" in data["filters"]

def test_post_filter_values():
    url = f"{BASE_URL}/filter-values"
    payload = {
        "filter-name": "строка",
        "filters": [
            {
                "filter-name": "год",
                "values": [2020]
            },
            {
                "filter-name": "город",
                "values": ["АЛАПАЕВСК"]
            }
        ],
    }
    response = requests.post(url, json=payload)
    assert response.status_code == 200
    data = response.json()
    print(data)
    assert data.get("filter-name") == "строка"
    assert "values" in data
    assert isinstance(data["values"], list)

def test_post_filtered_data():
    url = f"{BASE_URL}/filtered-data"
    payload = {
        "filters": [
            {
                "filter-name": "год",
                "values": [2020]
            },
            {
                "filter-name": "город",
                "values": ["АЛАПАЕВСК"]
            }
        ],
        "limit": 4,
        "offset": 0
    }
    response = requests.post(url, json=payload)
    assert response.status_code == 200
    data = response.json()
    print(data)
    assert "headers" in data
    assert "data" in data
    assert "size" in data
    assert "max_size" in data
    assert isinstance(data["headers"], list)
    assert isinstance(data["data"], list)


def test_get_all_years():
    """Тест для получения списка всех возможных годов без применения дополнительных фильтров"""
    url = f"{BASE_URL}/filter-values"
    payload = {
        "filter-name": "год",
        "filters": [],
        "pattern": ""
    }

    response = requests.post(url, json=payload)
    assert response.status_code == 200
    data = response.json()
    print("Все года:", data)

    assert data.get("filter-name") == "год"
    assert "values" in data
    assert isinstance(data["values"], list)
    assert len(data["values"]) > 0
    assert all(isinstance(year, int) for year in data["values"])


def test_get_all_cities():
    """Тест для получения списка всех возможных городов без применения дополнительных фильтров"""
    url = f"{BASE_URL}/filter-values"
    payload = {
        "filter-name": "город",
        "filters": [],
        "pattern": ""
    }

    response = requests.post(url, json=payload)
    assert response.status_code == 200
    data = response.json()
    print("Все города:", data)

    assert data.get("filter-name") == "город"
    assert "values" in data
    assert isinstance(data["values"], list)
    assert len(data["values"]) > 0
    assert all(isinstance(city, str) for city in data["values"])

# def test_download_filtered_data_formats():
#     url = f"{BASE_URL}/download-filtered-data"
#     response = requests.get(url)
#     # Если endpoint не реализован, может возвращаться 404
#     assert response.status_code in (200, 404)
#
# def test_download_filtered_data_csv():
#     url = f"{BASE_URL}/download-filtered-data/csv"
#     response = requests.get(url)
#     # Если endpoint не реализован, может возвращаться 404
#     assert response.status_code in (200, 404)
