# Тесты для API фильтров

import pytest
from fastapi.testclient import TestClient
from main import app

client = TestClient(app)

def test_get_filters_names():
    response = client.get("/api/v2/filters-names")
    assert response.status_code == 200
    assert "filters" in response.json()

def test_post_filter_values():
    payload = {
        "filter-name": "год",
        "filters": [],
        "pattern": ""
    }
    response = client.post("/api/v2/filter-values", json=payload)
    assert response.status_code == 200
    assert "values" in response.json()