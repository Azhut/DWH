"""Агрегат application-уровня для операций над данными: сохранение, удаление, выборка, индексы."""

from app.application.data.save import DataSaveService
from app.application.data.delete import DataDeleteService
from app.application.data.retrieval import DataRetrievalService
from app.application.data.indexes import MongoIndexManager, create_indexes

__all__ = [
    "DataSaveService",
    "DataDeleteService",
    "DataRetrievalService",
    "MongoIndexManager",
    "create_indexes",
]

