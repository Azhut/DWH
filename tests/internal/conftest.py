import pytest
import asyncio


from app.core.database import mongo_connection
from app.core.dependencies import get_ingestion_service, get_file_service, get_filter_service, get_flat_data_service
from app.data.index_manager import create_indexes

pytest_plugins = ("pytest_asyncio",)

@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()

@pytest.fixture(autouse=True)
async def clean_db():
    """
    Автоматически очищаем коллекции перед каждым тестом:
    Files, FlatData, Logs
    """
    db = mongo_connection.get_database()
    # Drop indexes and collections to keep state clean
    await db.Files.delete_many({})
    await db.FlatData.delete_many({})
    await db.Logs.delete_many({})
    # Ensure indexes exist for tests that verify them
    try:
        await create_indexes()
    except Exception:
        # Если индексы уже есть — OK
        pass
    yield
    # очистка после теста (на всякий случай)
    await db.Files.delete_many({})
    await db.FlatData.delete_many({})
    await db.Logs.delete_many({})

@pytest.fixture
def sample_xlsx_path():
    # Путь к файлу, который вы прикрепили: /mnt/data/data_example.xlsx
    return "tests/internal/data/ИРБИТ 2023.xls"

@pytest.fixture
def ingestion_service():
    # Используем фабрику зависимостей проекта
    return get_ingestion_service()

@pytest.fixture
def file_service():
    return get_file_service()

@pytest.fixture
def filter_service():
    return get_filter_service()

@pytest.fixture
def flat_data_service():
    return get_flat_data_service()
