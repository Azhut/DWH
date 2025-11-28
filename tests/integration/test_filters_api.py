import pytest
from fastapi.testclient import TestClient


class TestFilesAPI:
    """Интеграционные тесты для API файлов"""

    def test_list_files_empty(self, test_client, test_database):
        """Тест получения списка файлов из пустой БД"""
        response = test_client.get("/api/v2/files")

        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) == 0

    def test_list_files_with_pagination(self, test_client, test_database):
        """Тест пагинации списка файлов"""
        response = test_client.get("/api/v2/files?limit=5&offset=0")

        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)

    def test_list_files_invalid_pagination(self, test_client):
        """Тест неверных параметров пагинации"""
        response = test_client.get("/api/v2/files?limit=-1&offset=-1")

        assert response.status_code == 422  # Validation error

    def test_delete_nonexistent_file(self, test_client, test_database):
        """Тест удаления несуществующего файла"""
        response = test_client.delete("/api/v2/files/nonexistent-id")

        assert response.status_code == 404

    def test_files_response_structure(self, test_client, test_database):
        """Тест структуры ответа файлов"""
        response = test_client.get("/api/v2/files?limit=1")

        if response.status_code == 200:
            data = response.json()
            if data:  # если есть данные
                file_data = data[0]
                expected_fields = {
                    "file_id", "filename", "status", "error",
                    "upload_timestamp", "updated_at", "year"
                }
                assert expected_fields.issubset(set(file_data.keys()))