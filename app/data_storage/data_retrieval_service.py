import math
from typing import List, Dict, Tuple

from motor.motor_asyncio import AsyncIOMotorClient

from app.core.logger import logger


class DataRetrievalService:
    def __init__(self, db_uri: str, db_name: str):
        self.client = AsyncIOMotorClient(db_uri)
        self.db = self.client[db_name]
        self.sheets_collection = self.db.get_collection("Sheets")
        self.flat_data_collection = self.db.get_collection("FlatData")

    async def get_filter_values(self, filter_name: str, applied_filters: List[Dict], pattern: str = "") -> List:
        logger.debug(f"Filter: {filter_name}")
        logger.debug(f"Applied filters: {applied_filters}")
        logger.debug(f"Pattern: {pattern}")
        try:
            query = self._build_query(applied_filters)
            if pattern:
                query[self._map_filter_name(filter_name)] = {"$regex": pattern, "$options": "i"}
            if filter_name in ["год", "город"]:
                return await self._get_main_collection_values(filter_name, query)
            return await self._get_flat_collection_values(filter_name, query)
        except Exception as e:
            logger.error(f"Filter values error: {str(e)}")
            raise

    def _map_filter_name(self, filter_name: str) -> str:
        filter_name.lower()
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
        processed_data = []
        for item in data:
            row = []
            for key in ["year", "city", "section", "row", "column", "value"]:
                value = item.get(key)
                if isinstance(value, float) and math.isnan(value):
                    value = None
                row.append(value)
            processed_data.append(row)
        return processed_data, total

    def _build_query(self, filters: List[Dict]) -> dict:
        query = {"$and": []}
        for f in filters:
            field = self._map_filter_name(f["filter-name"])
            values = f["values"]
            if field == "city":
                values = [v.upper() for v in values]
            query["$and"].append({field: {"$in": values}})
        return query
