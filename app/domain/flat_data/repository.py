"""Репозиторий агрегата FlatData: коллекция FlatData."""

from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Tuple

from pymongo import InsertOne

from app.domain.base import BaseRepository
from app.domain.flat_data.models import TABLE_FIELDS


class FlatDataRepository(BaseRepository):
    """Доступ к документам FlatData: выборки, пакетная запись, удаление по form/file."""

    TABLE_FIELDS = TABLE_FIELDS

    async def distinct(self, field: str, query: Dict[str, Any], session: Any = None) -> List[Any]:
        return await self.collection.distinct(field, filter=query, session=session)

    async def bulk_write_ops(
        self,
        operations: List[InsertOne],
        *,
        ordered: bool = False,
        session: Any = None,
    ) -> Any:
        return await self.collection.bulk_write(operations, ordered=ordered, session=session)

    async def get_filtered_data(
        self,
        query: Dict[str, Any],
        limit: int,
        offset: int,
        session: Any = None,
    ) -> Tuple[List[Dict[str, Any]], int]:
        """Возвращает страницу данных и общий счётчик параллельными запросами.

        Прежде find() и count_documents() шли последовательно — каждый тяжёлый
        запрос блокировал следующий. asyncio.gather() запускает оба одновременно:
        итоговая latency равна max(find, count), а не их сумме.
        """
        projection = {f: 1 for f in self.TABLE_FIELDS}
        projection["_id"] = 0

        docs_coro = self.find(
            query=query,
            projection=projection,
            limit=limit,
            skip=offset,
            session=session,
        )
        count_coro = self.count_documents(query, session=session)

        docs, total = await asyncio.gather(docs_coro, count_coro)
        return docs, total

    async def delete_by_form(self, form_id: str, session: Any = None) -> Any:
        return await self.delete_many({"form": form_id}, session=session)

    async def delete_by_file_id(self, file_id: str, session: Any = None) -> Any:
        return await self.delete_many({"file_id": file_id}, session=session)