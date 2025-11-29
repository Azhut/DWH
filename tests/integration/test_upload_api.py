import pytest
import os



class TestUploadAPI:
    """Интеграционные тесты для API загрузки"""

    def test_upload_single_file(self, test_client, setup_test_database):
        """Тест загрузки одного файла"""
        test_file_path = "tests/test_data/valid_file.xlsx"

        if not os.path.exists(test_file_path):
            pytest.skip("Тестовый файл не найден")

        with open(test_file_path, "rb") as f:
            files = [("files", ("TEST_CITY 2023.xlsx", f, "application/vnd.ms-excel"))]

            response = test_client.post("/api/v2/upload", files=files)

        assert response.status_code == 200
        data = response.json()
        assert "message" in data
        assert "details" in data

    def test_upload_multiple_files(self, test_client, setup_test_database):
        """Тест загрузки нескольких файлов"""
        test_files = []

        for filename in ["TEST_CITY1 2023.xlsx", "TEST_CITY2 2022.xlsx"]:
            file_path = f"tests/test_data/{filename}"
            if os.path.exists(file_path):
                test_files.append(("files", (filename, open(file_path, "rb"), "application/vnd.ms-excel")))

        if not test_files:
            pytest.skip("Тестовые файлы не найдены")

        try:
            response = test_client.post("/api/v2/upload", files=test_files)
            assert response.status_code == 200
        finally:
            for _, (_, file, _) in test_files:
                file.close()