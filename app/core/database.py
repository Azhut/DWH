"""Подключение к MongoDB через Motor: клиент, пул соединений, закрытие."""

from __future__ import annotations

import logging
from typing import Optional

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase

from config.config import config

logger = logging.getLogger(__name__)


class DatabaseConnection:
    """
    Держит один экземпляр AsyncIOMotorClient и объект базы.

    Параметры пула и таймаутов подобраны под API с параллельными запросами и пакетными вставками;
    при необходимости их можно вынести в конфигурацию.
    """

    def __init__(self, db_uri: str, db_name: str) -> None:
        self._uri = db_uri
        self._db_name = db_name
        self._client: Optional[AsyncIOMotorClient] = None
        self.db: Optional[AsyncIOMotorDatabase] = None
        self._last_init_error: Optional[BaseException] = None
        self._init_client()

    def _init_client(self) -> None:
        self._last_init_error = None
        try:
            self._client = AsyncIOMotorClient(
                self._uri,
                maxPoolSize=100,
                minPoolSize=10,
                socketTimeoutMS=120_000,
                connectTimeoutMS=10_000,
                serverSelectionTimeoutMS=10_000,
                waitQueueTimeoutMS=30_000,
            )
            self.db = self._client[self._db_name]
        except Exception as exc:
            self._last_init_error = exc
            logger.exception("Не удалось инициализировать клиент MongoDB")
            self._client = None
            self.db = None

    def get_client(self) -> AsyncIOMotorClient:
        """Возвращает клиент Motor; при необходимости повторяет инициализацию."""
        if self._client is None:
            self._init_client()
        if self._client is None:
            raise RuntimeError(
                "Не удалось установить соединение с MongoDB"
            ) from self._last_init_error
        return self._client

    def get_database(self) -> AsyncIOMotorDatabase:
        """Возвращает объект базы данных; поднимает ошибку, если клиент не создан."""
        if self.db is None:
            self._init_client()
        if self.db is None:
            raise RuntimeError(
                "Не удалось установить соединение с MongoDB"
            ) from self._last_init_error
        return self.db

    async def close(self) -> None:
        """Закрывает клиент и освобождает ресурсы пула."""
        if self._client:
            self._client.close()
            self._client = None
            self.db = None


mongo_connection = DatabaseConnection(config.MONGO_URI, config.DATABASE_NAME)
