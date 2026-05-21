"""Создание индексов MongoDB для коллекций Files и FlatData."""

from __future__ import annotations
import logging
from pymongo.errors import OperationFailure
from app.core.database import mongo_connection

logger = logging.getLogger(__name__)

# Индексы, которые существовали раньше и должны быть удалены перед созданием новых.
# Добавляем сюда имена при каждой будущей миграции.
_OBSOLETE_FLAT_DATA_INDEXES = {
    "form_idx",
    "reporter_year_idx",
}


class MongoIndexManager:
    def __init__(self, db) -> None:
        self.db = db

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

        await self.db.FlatData.create_index(
            [
                ("form",     1),
                ("reporter", 1),
                ("year",     1),
                ("section",  1),
                ("row",      1),
                ("column",   1),
            ],
            name="form_filters_idx",
        )

        await self.db.FlatData.create_index(
            [
                ("file_id",  1),
                ("year",     1),
                ("reporter", 1),
                ("section",  1),
                ("row",      1),
                ("column",   1),
            ],
            unique=True,
            name="main_unique_idx",
        )

        await self.db.FlatData.create_index(
            [("column", "text"), ("row", "text")],
            name="text_search_idx",
        )

    async def create_file_indexes(self) -> None:
        await self.db.Files.create_index(
            [("file_id", 1)],
            unique=True,
            name="uniq_file_id",
        )
        try:
            await self.db.Files.create_index(
                [("filename", 1), ("form_id", 1)],
                unique=True,
                name="uniq_filename_form_id",
            )
        except OperationFailure as exc:
            raise RuntimeError(
                "Невозможно создать уникальный индекс uniq_filename_form_id: "
                "в коллекции Files уже есть дубликаты (filename, form_id)."
            ) from exc

    async def create_all_indexes(self) -> None:
        await self.create_flat_data_index()
        await self.create_file_indexes()


async def create_indexes() -> None:
    db = mongo_connection.get_database()
    await MongoIndexManager(db).create_all_indexes()