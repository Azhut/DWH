"""Репозиторий агрегата FlatData: работа с коллекцией FlatData."""
from typing import Any, Dict, List, Tuple

from app.domain.base import BaseRepository
from app.domain.flat_data.models import TABLE_FIELDS


class FlatDataRepository(BaseRepository):
    TABLE_FIELDS = TABLE_FIELDS

    async def distinct(self, field: str, query: Dict[str, Any]) -> List[Any]:
        return await self.collection.distinct(field, query)

    async def get_filtered_data(
        self,
        query: Dict[str, Any],
        limit: int,
        offset: int,
    ) -> Tuple[List[Dict[str, Any]], int]:
        projection = {f: 1 for f in self.TABLE_FIELDS}
        projection["_id"] = 0
        docs = await self.find(
            query=query,
            projection=projection,
            limit=limit,
            skip=offset,
        )
        total = await self.count_documents(query)
        return docs, total

    async def delete_by_form(self, form_id: str) -> Any:
        return await self.delete_many({"form": form_id})

    async def delete_by_file_id(self, file_id: str) -> Any:
        return await self.delete_many({"file_id": file_id})
