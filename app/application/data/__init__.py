"""Application-агрегатор операций над данными: сохранение, удаление, выборка, индексы."""

from app.application.data.delete import DataDeleteService
from app.application.data.indexes import MongoIndexManager, create_indexes
from app.application.data.save import DataSaveService

__all__ = [
    "DataSaveService",
    "DataDeleteService",
    "MongoIndexManager",
    "create_indexes",
]
