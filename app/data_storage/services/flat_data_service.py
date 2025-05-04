import asyncio

from pymongo.errors import  ServerSelectionTimeoutError, NetworkTimeout

from app.core.exception_handler import log_and_raise_http
from app.data_storage.repositories.flat_data_repository import FlatDataRepository
from pymongo import UpdateOne, InsertOne
from typing import List
from app.core.logger import logger

from app.models.file_model import FileModel


class FlatDataService:
    def __init__(self, flat_data_repo: FlatDataRepository):
        self.flat_data_repo = flat_data_repo

    async def save_flat_data(self, records: List[dict]):
        if not records:
            return


        CHUNK_SIZE = 5000
        chunks = [records[i:i + CHUNK_SIZE] for i in range(0, len(records), CHUNK_SIZE)]

        for chunk in chunks:
            operations = [
                InsertOne(r) for r in chunk  # Замена UpdateOne на InsertOne
            ]

            try:
                await self.flat_data_repo.collection.bulk_write(
                    operations,
                    ordered=False,
                    bypass_document_validation=True
                )
            except Exception as e:
                logger.error(f"Final error: {str(e)}")
                raise

    async def _execute_with_retry(self, operations, max_retries=3):
        for attempt in range(max_retries):
            try:
                return await self.flat_data_repo.collection.bulk_write(
                    operations,
                    ordered=False,
                    bypass_document_validation=True
                )
            except (ServerSelectionTimeoutError, NetworkTimeout) as e:
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt
                    logger.warning(f"Retry {attempt + 1}/{max_retries}. Waiting {wait_time}s")
                    await asyncio.sleep(wait_time)
                else:
                    raise

    async def delete_by_file_id(self, file_model: FileModel):
        """Удаляет записи FlatData по file_id (или другим критериям)."""

        city= file_model.city
        year= file_model.year
        await self.flat_data_repo.delete_many({"city": city, "year": year})

