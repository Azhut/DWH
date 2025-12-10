# tests/integration/test_filters.py
import pytest

def test_get_filters_names(client):
    response = client.get("/api/v2/filters-names")
    assert response.status_code == 200
    data = response.json()
    assert "filters" in data
    assert {"год", "город", "раздел", "строка", "колонка"} == set(data["filters"])

def test_get_filter_values_empty(client, monkeypatch):
    from app.data.services.data_retrieval import DataRetrievalService
    class DummyRetrieval:
        async def get_filter_values(self, filter_name, applied_filters, pattern=""):
            return []
    monkeypatch.setattr(DataRetrievalService, "get_filter_values", DummyRetrieval().get_filter_values)
    payload = {
        "filter-name": "раздел",
        "filters": [],
        "pattern": ""
    }
    response = client.post("/api/v2/filter-values", json=payload)
    assert response.status_code == 200
    data = response.json()
    assert data["filter-name"] == "раздел"
    assert data["values"] == []

def test_get_filtered_data_empty(client, monkeypatch):
    # Аналогично эмулируем пустые данные
    from app.data.services.data_retrieval import DataRetrievalService
    class DummyRetrieval:
        async def get_filtered_data(self, filters, limit, offset):
            return [], 0
    monkeypatch.setattr(DataRetrievalService, "get_filtered_data", DummyRetrieval().get_filtered_data)
    payload = {
        "filters": [{"filter-name": "год", "values": [2020]}],
        "limit": 10,
        "offset": 0
    }
    response = client.post("/api/v2/filtered-data", json=payload)
    assert response.status_code == 200
    data = response.json()
    assert data["headers"] == ["год", "город", "раздел", "строка", "колонка", "значение"]
    assert data["data"] == []
    assert data["size"] == 0
    assert data["max_size"] == 0