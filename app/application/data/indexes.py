"""Создание индексов MongoDB для коллекций Files и FlatData."""

from __future__ import annotations

from pymongo.errors import OperationFailure

from app.core.database import mongo_connection


class MongoIndexManager:
    """Инициализирует индексы под сценарии загрузки, выборки и текстового поиска."""

    def __init__(self, db) -> None:
        self.db = db

    async def create_flat_data_index(self) -> None:
        """Создаёт уникальный составной индекс и вспомогательные индексы коллекции FlatData."""
        await self.db.FlatData.create_index(
            [
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

        await self.db.FlatData.create_index(
            [("form", 1)],
            name="form_idx",
        )

        await self.db.FlatData.create_index(
            [("reporter", 1), ("year", 1)],
            name="reporter_year_idx",
        )

        await self.db.FlatData.create_index(
            [("column", "text"), ("row", "text")],
            name="text_search_idx",
        )

    async def create_file_indexes(self) -> None:
        """Создаёт уникальные индексы коллекции Files по file_id и паре filename/form_id."""
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
                "в коллекции Files уже есть дубликаты (filename, form_id). "
                "Устраните дубликаты и перезапустите приложение."
            ) from exc

    async def create_all_indexes(self) -> None:
        """Создаёт все индексы FlatData и Files."""
        await self.create_flat_data_index()
        await self.create_file_indexes()


async def create_indexes() -> None:
    """Точка входа: создаёт индексы через MongoIndexManager."""
    db = mongo_connection.get_database()
    index_manager = MongoIndexManager(db)
    await index_manager.create_all_indexes()
