"""Фабрика репозитория FlatData: MongoDB или DuckDB по конфигурации."""

from __future__ import annotations

from app.domain.flat_data.duckdb_repository import DuckDBFlatDataRepository
from app.domain.flat_data.repository import FlatDataRepository
from config.config import config


def create_flat_data_repository(database=None) -> FlatDataRepository | DuckDBFlatDataRepository:
    storage = (config.FLATDATA_STORAGE or "mongo").lower()
    if storage == "duckdb":
        return DuckDBFlatDataRepository()
    if database is None:
        from app.core.dependencies import get_database

        database = get_database()
    return FlatDataRepository(database.get_collection("FlatData"))
