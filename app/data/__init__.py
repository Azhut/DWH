"""Обёртка для совместимости: индексы перенесены в app.application.data.indexes."""
from app.application.data.indexes import MongoIndexManager, create_indexes

__all__ = ["MongoIndexManager", "create_indexes"]
