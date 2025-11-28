import pytest
import os
from app.core.database import mongo_connection


@pytest.mark.production
class TestProductionDB:
    """Тесты для работы с продакшн БД (запускаются только явно)"""

    @pytest.fixture(autouse=True)
    def check_production_environment(self):
        """Проверяем, что тесты запускаются в продакшн окружении"""
        if os.getenv("APP_ENV") != "production":
            pytest.skip("Только для продакшн окружения")

    @pytest.mark.production
    @pytest.mark.asyncio
    async def test_production_connection(self):
        """Тест подключения к продакшн БД"""
        db = mongo_connection.get_database()

        # Простая проверка что БД отвечает
        try:
            result = await db.command("ping")
            assert result["ok"] == 1.0
        except Exception as e:
            pytest.fail(f"Не удалось подключиться к продакшн БД: {e}")

    @pytest.mark.production
    @pytest.mark.asyncio
    async def test_production_collections(self):
        """Проверка наличия необходимых коллекций в продакшн"""
        db = mongo_connection.get_database()
        collections = await db.list_collection_names()

        expected_collections = ["Files", "FlatData", "Logs"]
        for collection in expected_collections:
            assert collection in collections, f"Коллекция {collection} отсутствует в продакшн БД"