import pytest
import asyncio
import os
from unittest.mock import Mock, AsyncMock, patch
from fastapi.testclient import TestClient
from motor.motor_asyncio import AsyncIOMotorClient

from config import config
from main import app
from app.core.database import mongo_connection


# Устанавливаем тестовое окружение
os.environ["APP_ENV"] = "testing"


@pytest.fixture(scope="session")
def event_loop():
    """Создаем event loop для тестов"""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(autouse=True)
def setup_test_environment():
    """Автоматическая настройка тестового окружения"""
    original_env = os.getenv("APP_ENV")
    os.environ["APP_ENV"] = "testing"
    yield
    if original_env:
        os.environ["APP_ENV"] = original_env
    else:
        os.environ.pop("APP_ENV", None)


@pytest.fixture
def test_client():
    """Фикстура для тестового клиента FastAPI"""
    return TestClient(app)


@pytest.fixture
async def test_database():
    """Фикстура для тестовой базы данных"""
    if config.APP_ENV != "testing":
        pytest.skip("Только для тестового окружения")

    db = mongo_connection.get_database()

    # Очищаем коллекции перед тестом
    collections = await db.list_collection_names()
    for collection_name in collections:
        await db[collection_name].delete_many({})

    yield db

    # Очищаем после теста
    for collection_name in collections:
        await db[collection_name].delete_many({})


@pytest.fixture
def mock_file_metadata():
    """Мок метаданных файла"""
    return {
        "city": "TEST_CITY",
        "year": 2023,
        "filename": "TEST_CITY 2023.xlsx",
        "extension": "xlsx"
    }


@pytest.fixture
def sample_upload_file():
    """Мок UploadFile"""
    mock_file = Mock()
    mock_file.filename = "TEST_CITY 2023.xlsx"
    mock_file.read = AsyncMock(return_value=b"fake_excel_content")
    mock_file.seek = AsyncMock()
    return mock_file


@pytest.fixture
def mock_mongo_collection():
    """Мок MongoDB коллекции"""
    mock_collection = AsyncMock()
    mock_collection.find = AsyncMock()
    mock_collection.find_one = AsyncMock()
    mock_collection.insert_one = AsyncMock()
    mock_collection.update_one = AsyncMock()
    mock_collection.delete_one = AsyncMock()
    mock_collection.delete_many = AsyncMock()
    mock_collection.count_documents = AsyncMock(return_value=0)
    return mock_collection