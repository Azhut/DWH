from app.data_storage.repositories.flat_data_repository import FlatDataRepository
from pymongo import UpdateOne
from typing import List
from app.core.logger import logger

class FlatDataService:
    def __init__(self, flat_data_repo: FlatDataRepository):
        self.flat_data_repo = flat_data_repo

    async def save_flat_data(self, records: List[dict]):
        """
        Сохраняет уникальные записи в коллекцию FlatData

        :param records: Список записей
        """
        if not records:
            return
        try:
            operations = []
            for record in records:
                filter_criteria = {
                    "year": record.get("year"),
                    "city": record.get("city"),
                    "section": record.get("section"),
                    "row": record.get("row"),
                    "column": record.get("column"),
                    "value": record.get("value")
                }
                operations.append(
                    UpdateOne(filter_criteria, {"$setOnInsert": record}, upsert=True)
                )
            if operations:
                result = await self.flat_data_repo.collection.bulk_write(operations, ordered=False)
                inserted_count = result.upserted_count
                logger.info(f"Inserted {inserted_count} unique flat records out of {len(records)}")
        except Exception as e:
            logger.error(f"Failed to save flat data: {str(e)}")
            raise