
def test_upload_invalid_extension(client, tmp_path):
    """Тест проверяет, что файлы с неправильным расширением обрабатываются с ошибкой"""
    invalid_file = tmp_path / "invalid.txt"
    invalid_file.write_text("not excel content")

    with open(invalid_file, "rb") as f:
        files = {"files": ("invalid.txt", f, "text/plain")}
        response = client.post("/api/v2/upload", files=files)

    # Ожидается 200 (API всегда возвращает 200, даже при ошибках)
    assert response.status_code == 200

    # Проверяем, что в ответе есть детали ошибки
    json_data = response.json()
    assert "details" in json_data
    assert len(json_data["details"]) == 1

    detail = json_data["details"][0]
    assert detail["status"] == "failed"
    assert "расширение" in detail["error"].lower()


def test_upload_invalid_name_format(client, tmp_path):
    """Тест проверяет, что файлы с неправильным именем обрабатываются с ошибкой"""
    bad_file = tmp_path / "BADNAME.xlsx"
    bad_file.write_text("")  # пустой Excel

    with open(bad_file, "rb") as f:
        files = {"files": ("BADNAME.xlsx", f, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")}
        response = client.post("/api/v2/upload", files=files)

    # Ожидается 200 (API всегда возвращает 200, даже при ошибках)
    assert response.status_code == 200

    # Проверяем, что в ответе есть детали ошибки
    json_data = response.json()
    assert "details" in json_data
    assert len(json_data["details"]) == 1

    detail = json_data["details"][0]
    assert detail["status"] == "failed"
    assert "формат" in detail["error"].lower() or "город" in detail["error"].lower()