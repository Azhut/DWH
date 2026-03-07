"""MongoDB index management for data aggregates (Files, FlatData)."""

from pymongo.errors import OperationFailure

from app.core.database import mongo_connection


class MongoIndexManager:
    def __init__(self, db):
        self.db = db

    async def create_flat_data_index(self):
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
            background=True,
        )

        await self.db.FlatData.create_index(
            [("reporter", 1), ("year", 1)],
            name="reporter_year_idx",
            background=True,
        )

        await self.db.FlatData.create_index(
            [("column", "text"), ("row", "text")],
            name="text_search_idx",
            background=True,
        )

    async def create_file_indexes(self):
        await self.db.Files.create_index(
            [("file_id", 1)],
            unique=True,
            name="uniq_file_id",
            background=True,
        )

        # Upload uniqueness is scoped by form.
        try:
            await self.db.Files.create_index(
                [("filename", 1), ("form_id", 1)],
                unique=True,
                name="uniq_filename_form_id",
                background=True,
            )
        except OperationFailure as exc:
            raise RuntimeError(
                "Cannot create unique index uniq_filename_form_id because duplicate "
                "(filename, form_id) pairs already exist in Files. "
                "Clean duplicates first, then restart the app."
            ) from exc


    async def create_all_indexes(self):
        await self.create_flat_data_index()
        await self.create_file_indexes()


async def create_indexes():
    """Factory method to create all indexes via MongoIndexManager."""
    db = mongo_connection.get_database()
    index_manager = MongoIndexManager(db)
    await index_manager.create_all_indexes()
