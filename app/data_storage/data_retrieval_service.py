import math
from typing import List, Dict, Tuple

from app.core.config import mongo_connection
from app.data_storage.repositories.flat_data_repository import FlatDataRepository
from app.data_storage.services.filter_service import FilterService


def _map_filter_name(filter_name: str) -> str:
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


class DataRetrievalService:
    def __init__(self, filter_service: FilterService):
        db = mongo_connection.get_database()
        self.flat_data_repo = FlatDataRepository(db.get_collection("FlatData"))
        self.filter_service = filter_service

    async def get_filter_values(self, filter_name: str, applied_filters: List[Dict], pattern: str = "") -> List:
        """
        Получить значения фильтра через FilterService
        """
        return await self.filter_service.get_filter_values(filter_name, applied_filters, pattern)

    async def get_filtered_data(self, filters: List[Dict], limit: int, offset: int) -> Tuple[List, int]:
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

        total = await self.flat_data_repo.collection.count_documents(query)
        data = await cursor.to_list(length=None)
        processed_data = []
        for item in data:
            row = []
            for key in ["year", "city", "section", "row", "column", "value"]:
                value = item.get(key)

                if isinstance(value, float):
                    if key == "value":

                        if value.is_integer():
                            value = int(value)
                        else:
                            value = f"{value:.2f}"
                    else:

                        value = int(value) if value.is_integer() else value

                elif isinstance(value, float) and math.isnan(value):
                    value = None

                row.append(value)
            processed_data.append(row)

        return processed_data, total

def create_data_retrieval_service():
    """
    Фабричный метод для создания экземпляра DataRetrievalService с инициализированным FilterService
    """
    db = mongo_connection.get_database()
    flat_data_repo = FlatDataRepository(db.get_collection("FlatData"))
    filter_service = FilterService(flat_data_repo)


    return DataRetrievalService(filter_service)
