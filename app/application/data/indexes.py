"""Создание индексов MongoDB для коллекций Files и FlatData."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any

from pymongo.errors import OperationFailure

from app.core.database import mongo_connection
from config.config import config

logger = logging.getLogger(__name__)

# Индексы, которые существовали раньше и должны быть удалены перед созданием новых.
# Добавляем сюда имена при каждой будущей миграции.
_OBSOLETE_FLAT_DATA_INDEXES = {
    "form_idx",
    "reporter_year_idx",
}


@dataclass(frozen=True)
class MongoIndexDefinition:
    collection_name: str
    keys: list[tuple[str, Any]]
    name: str
    unique: bool = False


class MongoIndexManager:
    def __init__(self, db) -> None:
        self.db = db

    async def _create_index_if_missing(self, definition: MongoIndexDefinition) -> None:
        collection = self.db[definition.collection_name]
        existing = await collection.index_information()
        if definition.name in existing:
            logger.info(
                "Индекс %s.%s уже существует, создание пропущено",
                definition.collection_name,
                definition.name,
            )
            return

        logger.info(
            "Создаётся индекс %s.%s: keys=%s unique=%s",
            definition.collection_name,
            definition.name,
            definition.keys,
            definition.unique,
        )
        await collection.create_index(
            definition.keys,
            name=definition.name,
            unique=definition.unique,
        )
        logger.info("Индекс %s.%s создан", definition.collection_name, definition.name)

    async def _drop_obsolete_indexes(self, collection_name: str, obsolete: set[str]) -> None:
        """Удаляет устаревшие индексы, если они ещё существуют.

        Ошибки игнорируются — индекса может уже не быть, это нормально.
        """
        collection = self.db[collection_name]
        existing = await collection.index_information()
        for name in obsolete:
            if name in existing:
                try:
                    await collection.drop_index(name)
                    logger.info("Удалён устаревший индекс %s.%s", collection_name, name)
                except OperationFailure as e:
                    logger.warning("Не удалось удалить индекс %s.%s: %s", collection_name, name, e)

    async def create_flat_data_index(self) -> None:
        await self._drop_obsolete_indexes("FlatData", _OBSOLETE_FLAT_DATA_INDEXES)

        await self._create_index_if_missing(
            MongoIndexDefinition(
                collection_name="FlatData",
                keys=[
                    ("form", 1),
                    ("reporter", 1),
                    ("year", 1),
                    ("section", 1),
                    ("row", 1),
                    ("column", 1),
                ],
                name="form_filters_idx",
            )
        )

        await self._create_index_if_missing(
            MongoIndexDefinition(
                collection_name="FlatData",
                keys=[
                    ("file_id", 1),
                    ("year", 1),
                    ("reporter", 1),
                    ("section", 1),
                    ("row", 1),
                    ("column", 1),
                ],
                unique=True,
                name="main_unique_idx",
            )
        )

        await self._create_index_if_missing(
            MongoIndexDefinition(
                collection_name="FlatData",
                keys=[("column", "text"), ("row", "text")],
                name="text_search_idx",
            )
        )

    async def create_file_indexes(self) -> None:
        await self._create_index_if_missing(
            MongoIndexDefinition(
                collection_name="Files",
                keys=[("file_id", 1)],
                unique=True,
                name="uniq_file_id",
            )
        )
        try:
            await self._create_index_if_missing(
                MongoIndexDefinition(
                    collection_name="Files",
                    keys=[("filename", 1), ("form_id", 1)],
                    unique=True,
                    name="uniq_filename_form_id",
                )
            )
        except OperationFailure as exc:
            raise RuntimeError(
                "Невозможно создать уникальный индекс uniq_filename_form_id: "
                "в коллекции Files уже есть дубликаты (filename, form_id)."
            ) from exc

    async def create_all_indexes(self) -> None:
        if (config.FLATDATA_STORAGE or "mongo").lower() != "duckdb":
            await self.create_flat_data_index()
        else:
            logger.info(
                "FLATDATA_STORAGE=duckdb: индексы коллекции FlatData в MongoDB не создаются"
            )
        await self.create_file_indexes()


async def create_indexes() -> None:
    db = mongo_connection.get_database()
    await MongoIndexManager(db).create_all_indexes()


async def create_indexes_background() -> None:
    logger.info("Фоновое создание индексов MongoDB запущено")
    try:
        await create_indexes()
    except asyncio.CancelledError:
        logger.info("Фоновое создание индексов MongoDB отменено")
        raise
    except Exception:
        logger.exception("Фоновое создание индексов MongoDB завершилось ошибкой")
    else:
        logger.info("Фоновое создание индексов MongoDB завершено")


def schedule_index_creation() -> asyncio.Task[None]:
    return asyncio.create_task(
        create_indexes_background(),
        name="mongo-index-creation",
    )
