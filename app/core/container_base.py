"""
Базовый контейнер без циклических импортов
"""
from typing import Any, Dict
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.core.database import mongo_connection


class ContainerBase:
    """Базовый контейнер зависимостей приложения"""

    def __init__(self):
        self._db = None
        self._services: Dict[str, Any] = {}

    def get_db(self) -> AsyncIOMotorDatabase:
        """Получить подключение к базе данных"""
        if self._db is None:
            self._db = mongo_connection.get_database()
        return self._db

    def register_service(self, name: str, service: Any):
        """Зарегистрировать сервис в контейнере"""
        self._services[name] = service

    def get_service(self, name: str) -> Any:
        """Получить сервис из контейнера"""
        return self._services.get(name)