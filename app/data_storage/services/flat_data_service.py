from app.core.exception_handler import log_and_raise_http
from app.data_storage.repositories.flat_data_repository import FlatDataRepository
from pymongo import UpdateOne
from typing import List
from app.core.logger import logger

from app.models.file_model import FileModel


class FlatDataService:
    def __init__(self, flat_data_repo: FlatDataRepository):
        self.flat_data_repo = flat_data_repo

    async def save_flat_data(self, records: List[dict]):
        """
        Сохраняет уникальные записи в коллекцию FlatData

        :param records: Список записей
        """
        if not records:
            logger.warning("Нет данных для сохранения в FlatData")
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
                logger.info(f"Вставлено {inserted_count} уникальных записей из  {len(records)}")
        except Exception as e:
            logger.error(f"Ошибка при сохранении плоских данных: {str(e)}", exc_info=True)
            log_and_raise_http(500, "Ошибка при сохранении плоских данных", e)

    async def delete_by_file_id(self, file_model: FileModel):
        """Удаляет записи FlatData по file_id (или другим критериям)."""

        city= FileModel.city
        year= FileModel.year
        await self.flat_data_repo.delete_many({"city": city, "year": year})

