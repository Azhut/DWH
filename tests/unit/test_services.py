import pytest
from app.data.services.data_delete import DataDeleteService
from fastapi import HTTPException


@pytest.mark.asyncio
async def test_delete_file_success(async_mongo_db, monkeypatch):
    # вставляем запись Files
    files = async_mongo_db.get_collection("Files")
    await files.insert_one({"file_id": "exists"})
    # monkeypatch mongo_connection.get_database -> наша async_mongo_db
    import app.core.database as dbmod
    monkeypatch.setattr(dbmod.mongo_connection, "get_database", lambda: async_mongo_db)
    service = DataDeleteService()
    await service.delete_file("exists")  # не должно бросать

@pytest.mark.asyncio
async def test_delete_file_not_found(async_mongo_db, monkeypatch):
    import app.core.database as dbmod
    monkeypatch.setattr(dbmod.mongo_connection, "get_database", lambda: async_mongo_db)
    service = DataDeleteService()
    with pytest.raises(HTTPException) as excinfo:
        await service.delete_file("notfound")
    assert excinfo.value.status_code == 404

@pytest.mark.asyncio
async def test_delete_flat_error(async_mongo_db, monkeypatch):
    # Подготовим file doc
    files = async_mongo_db.get_collection("Files")
    await files.insert_one({"file_id": "raise_error"})
    import app.core.database as dbmod
    monkeypatch.setattr(dbmod.mongo_connection, "get_database", lambda: async_mongo_db)
    # Подменим flat collection так, чтобы delete_many бросал
    flat = async_mongo_db.get_collection("FlatData")
    # простая замена метода delete_many на функцию, которая бросает
    async def raise_exc(query):
        raise Exception("flat delete error")
    flat.delete_many = raise_exc
    service = DataDeleteService()
    with pytest.raises(HTTPException) as excinfo:
        await service.delete_file("raise_error")
    assert excinfo.value.status_code == 500
