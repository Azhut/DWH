from app.data_storage.repositories.flat_data_repository import FlatDataRepository
from typing import List, Dict

class FilterService:
    def __init__(self, flat_data_repo: FlatDataRepository):
        self.flat_data_repo = flat_data_repo

    async def get_filter_values(self, filter_name: str, applied_filters: List[Dict], pattern: str = "") -> List:
        """
        Получить значения фильтра из коллекции FlatData

        :param filter_name: Имя фильтра
        :param applied_filters: Примененные фильтры
        :param pattern: Шаблон для поиска
        :return: Список значений фильтра
        """
        query = self._build_query(applied_filters) if applied_filters else {}

        if pattern:
            query[self._map_filter_name(filter_name)] = {
                "$regex": pattern,
                "$options": "i"
            }

        return await self._get_flat_collection_values(filter_name, query)

    def _map_filter_name(self, filter_name: str) -> str:
        filter_name = filter_name.lower()
        mapping = {
            "год": "year",
            "город": "city",
            "раздел": "section",
            "строка": "row",
            "колонка": "column"
        }
        return mapping[filter_name]

    async def _get_flat_collection_values(self, filter_name: str, query: dict) -> List:
        field = self._map_filter_name(filter_name)
        pipeline = [
            {"$match": query},
            {"$group": {"_id": f"${field}"}},
            {"$project": {"_id": 0, "value": "$_id"}}
        ]
        cursor = self.flat_data_repo.collection.aggregate(pipeline)
        return [doc["value"] async for doc in cursor]

    def _build_query(self, filters: List[Dict]) -> dict:
        query_conditions = []

        for f in filters:
            field = self._map_filter_name(f["filter-name"])
            values = f["values"]

            if not values:
                continue

            if field == "city":
                values = [v.upper() for v in values]

            query_conditions.append({field: {"$in": values}})

        return {"$and": query_conditions} if query_conditions else {}