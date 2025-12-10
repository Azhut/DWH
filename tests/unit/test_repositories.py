
import pytest
from app.data.repositories.file import FileRepository
from app.data.repositories.flat_data import FlatDataRepository
from app.data.repositories.logs import LogsRepository

# Используем async_mongo_db из conftest
@pytest.mark.asyncio
async def test_find_by_file_id(async_mongo_db):
    # подготовим документ
    files = async_mongo_db.get_collection("Files")
    await files.insert_one({"file_id": "1", "filename": "a", "city": "C", "year": 2020})
    repo = FileRepository(files)
    res = await repo.find_by_file_id("1")
    assert res and res["file_id"] == "1"
    res2 = await repo.find_by_file_id("nonexistent")
    assert res2 is None

@pytest.mark.asyncio
async def test_find_by_filename(async_mongo_db):
    files = async_mongo_db.get_collection("Files")
    await files.insert_one({"file_id": "2", "filename": "a"})
    repo = FileRepository(files)
    res = await repo.find_by_filename("a")
    assert res and res["filename"] == "a"

@pytest.mark.asyncio
async def test_find_by_year_and_city(async_mongo_db):
    flat = async_mongo_db.get_collection("FlatData")
    await flat.insert_one({"year": 2020, "city": "C"})
    repo = FlatDataRepository(flat)
    res = await repo.find_by_year_and_city(2020, "C")
    assert isinstance(res, list)
    assert len(res) >= 1

@pytest.mark.asyncio
async def test_logs_repository_find_by_level(async_mongo_db):
    logs = async_mongo_db.get_collection("Logs")
    await logs.insert_one({"level": "info", "message": "ok"})
    repo = LogsRepository(logs)
    res = await repo.find_by_level("info")
    assert isinstance(res, list)
    assert len(res) >= 1
