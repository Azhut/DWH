from typing import List, Dict, Tuple

from motor.motor_asyncio import AsyncIOMotorClient


class DataRetrievalService:
    def __init__(self, db_uri: str, db_name: str):
        self.client = AsyncIOMotorClient(db_uri)
        self.db = self.client[db_name]
        self.sheets_collection = self.db.get_collection("Sheets")
        self.flat_data_collection = self.db.get_collection("FlatData")  # Новая коллекция

    async def get_filter_values(self, filter_name: str, applied_filters: List[Dict], pattern: str = "") -> List:
        # Создаем базовый запрос
        query = self._build_query(applied_filters)

        # Добавляем поиск по паттерну
        if pattern:
            query[self._map_filter_name(filter_name)] = {"$regex": pattern, "$options": "i"}

        # Для полей из основной коллекции
        if filter_name in ["год", "город", "раздел"]:
            return await self._get_main_collection_values(filter_name, query)

        # Для полей из плоской коллекции
        return await self._get_flat_collection_values(filter_name, query)

    def _map_filter_name(self, filter_name: str) -> str:
        mapping = {
            "год": "year",
            "город": "city",
            "раздел": "section",
            "строка": "row",
            "колонка": "column"
        }
        return mapping[filter_name]

    async def _get_main_collection_values(self, filter_name: str, query: dict) -> List:
        field = self._map_filter_name(filter_name)
        return await self.sheets_collection.distinct(field, query)

    async def _get_flat_collection_values(self, filter_name: str, query: dict) -> List:
        field = self._map_filter_name(filter_name)
        pipeline = [
            {"$match": query},
            {"$group": {"_id": f"${field}"}},
            {"$project": {"_id": 0, "value": "$_id"}}
        ]
        cursor = self.flat_data_collection.aggregate(pipeline)
        return [doc["value"] async for doc in cursor]

    async def get_filtered_data(self, filters: List[Dict], limit: int, offset: int) -> Tuple[List, int]:
        query = self._build_query(filters)
        total = await self.flat_data_collection.count_documents(query)
        cursor = self.flat_data_collection.find(query).skip(offset).limit(limit)
        data = await cursor.to_list(length=None)
        return [
            [item["year"], item["city"], item["section"],
             item["row"], item["column"], item["value"]
             ] for item in data], total

    def _build_query(self, filters: List[Dict]) -> dict:
        query = {"$and": []}
        for f in filters:
            field = self._map_filter_name(f["filter-name"])
            query["$and"].append({field: {"$in": f["values"]}})
        return query