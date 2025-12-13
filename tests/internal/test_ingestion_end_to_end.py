import io
import pytest
from fastapi import UploadFile
from app.models.file_model import FileModel
from app.models.file_status import FileStatus
from app.core.database import mongo_connection


@pytest.mark.asyncio
async def test_ingestion_happy_path(ingestion_service, sample_xlsx_path):
    # читаем файл в память и формируем UploadFile
    with open(sample_xlsx_path, "rb") as f:
        content = f.read()
    upload = UploadFile(file=io.BytesIO(content), filename="TESTCITY 2023.xlsx")

    # Передаём фиктивный form_id (тест на уровне сервиса — форма не обязательна в базе)
    resp = await ingestion_service.process_files([upload], form_id="test-form")
    assert "processed successfully" in resp.message.lower()

    db = mongo_connection.get_database()
    files_doc = await db.Files.find_one({"filename": "TESTCITY 2023.xlsx"})
    assert files_doc is not None
    assert files_doc.get("status") == FileStatus.SUCCESS.value

    flat_count = await db.FlatData.count_documents({"file_id": files_doc["file_id"]})
    assert flat_count > 0

    # Проверим, что фильтр по городу вернёт наш город (используем FilterService через dependency)
    from app.core.dependencies import get_data_retrieval_service
    dr = get_data_retrieval_service()
    vals = await dr.get_filter_values("город", [], "")
    assert any(isinstance(v, str) for v in vals)
