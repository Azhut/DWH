import pytest
import requests
from pydantic import ValidationError
from app.api.v2.Models.filters import (
    FiltersNamesResponse,
    FilterValuesRequest,
    FilterValuesResponse,
    FilteredDataRequest,
    FilteredDataResponse
)

BASE_URL = "http://localhost:2700/api/v2"


@pytest.fixture(scope="module")
def setup_data():
    """Фикстура для загрузки тестовых данных в базу"""
    test_file = '../tests/data/АЛАПАЕВСК 2020.xlsx'

    with open(test_file, 'rb') as f:
        response = requests.post(
            f"{BASE_URL}/files/upload",
            files={'files': f}
        )
        assert response.status_code == 200


def test_get_filters_names(setup_data):
    response = requests.get(f"{BASE_URL}/filters-names")

    assert response.status_code == 200
    assert response.json() == {
        "filters": ["год", "город", "раздел", "строка", "колонка"]
    }


def test_filter_values_complex(setup_data):
    payload = {
        "filter-name": "раздел",
        "filters": [
            {"filter-name": "город", "values": ["Алапаевск"]}
        ],
        "pattern": ""
    }

    response = requests.post(f"{BASE_URL}/filter-values", json=payload)
    assert response.status_code == 200

    data = response.json()

    # Проверка структуры ответа
    assert "values" in data
    assert isinstance(data["values"], list)

    # Проверка существующих данных в базе
    assert len(data["values"]) > 0, f"Нет данных в ответе. Получено: {data}"

    # Проверка ожидаемого значения
    expected_sections = ["Раздел1", "Раздел2"]  # Добавьте актуальные значения
    for section in expected_sections:
        assert section in data["values"], f"Раздел '{section}' не найден в {data['values']}"


def test_filtered_data_pagination(setup_data):
    payload = {
        "filters": [
            {"filter-name": "год", "values": [2020]}
        ],
        "limit": 1,
        "offset": 0
    }

    response = requests.post(f"{BASE_URL}/filtered-data", json=payload)
    assert response.status_code == 200

    data = response.json()
    assert len(data["data"]) == 1
    assert data["headers"] == ["год", "город", "раздел", "строка", "колонка", "значение"]