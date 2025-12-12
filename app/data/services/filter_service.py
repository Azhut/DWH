import math
from app.data.repositories.flat_data import FlatDataRepository
from typing import List, Dict, Tuple, Union


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
        query = _build_query(applied_filters) if applied_filters else {}

        if pattern:
            query[_map_filter_name(filter_name)] = {
                "$regex": pattern,
                "$options": "i"
            }

        return await self._get_flat_collection_values(filter_name, query)

    async def get_filtered_data(self, filters: List[Dict], limit: int, offset: int) -> Tuple[
        List[List[Union[str, int, float]]], int]:
        """
        Получает отфильтрованные данные с пагинацией
        """
        query = _build_query(filters)
        sort_order = [
            ("year", 1),
            ("city", 1),
            ("section", 1),
            ("row", 1),
            ("column", 1),
            ("value", 1)
        ]

        cursor = (
            self.flat_data_repo.collection
            .find(query)
            .sort(sort_order)
            .skip(offset)
            .limit(limit)
        )

        total = await self.flat_data_repo.count_documents(query)
        data = await cursor.to_list(length=None)

        return _process_data(data), total

    async def _get_flat_collection_values(self, filter_name: str, query: dict) -> List:
        field = _map_filter_name(filter_name)
        pipeline = [
            {"$match": query},
            {"$group": {"_id": f"${field}"}},
            {"$project": {"_id": 0, "value": "$_id"}}
        ]
        cursor = self.flat_data_repo.collection.aggregate(pipeline)
        return [doc["value"] async for doc in cursor]


def _process_data(data: List[Dict]) -> List[List[Union[str, int, float, None]]]:
    processed_data = []
    required_fields = ["year", "city", "section", "row", "column"]

    for item in data:
        row = []
        for key in required_fields + ["value"]:
            value = item.get(key)

            if key in required_fields:
                if value is None or (isinstance(value, float) and math.isnan(value)):
                    raise ValueError(f"Обязательное поле '{key}' содержит NaN или None")

                if key == "year" and isinstance(value, float):
                    value = int(value)

                row.append(value)
                continue

            if key == "value":
                if isinstance(value, float):

                    if math.isnan(value):
                        value = 0.0

                    value = int(value) if value.is_integer() else round(value, 2)

                row.append(value)

        processed_data.append(row)

    return processed_data


def _map_filter_name(filter_name: str) -> str:
    filter_name = filter_name.lower()
    mapping = {
        "год": "year",
        "город": "city",
        "раздел": "section",
        "строка": "row",
        "колонка": "column"
    }
    return mapping[filter_name]


def _build_query(filters: List[Dict]) -> dict:
    query_conditions = []

    for f in filters:
        field = _map_filter_name(f["filter-name"])
        values = f["values"]

        if not values:
            continue

        if field == "city":
            values = [v.upper() for v in values]

        query_conditions.append({field: {"$in": values}})

    return {"$and": query_conditions} if query_conditions else {}
