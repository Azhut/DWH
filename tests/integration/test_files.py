# tests/integration/test_files.py
import pytest
from fastapi import HTTPException
from app.api.v2.endpoints.files import get_data_delete_service

class DummyDeleteService:
    async def delete_file(self, file_id: str):
        if file_id == "exists":
            return
        if file_id == "notfound":
            raise HTTPException(status_code=404, detail=f"Файл '{file_id}' не найден")
        raise Exception("DB error")

@pytest.fixture(autouse=True)
def override_delete_service(client):
    # Переопределяем фабрику, которая возвращает DataDeleteService
    client.app.dependency_overrides[get_data_delete_service] = lambda: DummyDeleteService()

def test_delete_file_success(client):
    response = client.delete("/api/v2/files/exists")
    assert response.status_code == 200
    data = response.json()
    assert "detail" in data and "успешно удалена" in data["detail"]

def test_delete_file_not_found(client):
    response = client.delete("/api/v2/files/notfound")
    assert response.status_code == 404
    data = response.json()
    assert "Файл 'notfound' не найден" in data["detail"]

def test_delete_file_server_error(client):
    response = client.delete("/api/v2/files/errorcase")
    assert response.status_code == 500
    assert "Обратитесь к разработчикам" in response.json()["detail"]