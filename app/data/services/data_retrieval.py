from typing import List, Dict, Tuple
from app.data.services.filter_service import FilterService


class DataRetrievalService:
    def __init__(self, filter_service: FilterService):
        self.filter_service = filter_service

    async def get_filter_values(self, filter_name: str, applied_filters: List[Dict], pattern: str = "") -> List:
        return await self.filter_service.get_filter_values(filter_name, applied_filters, pattern)

    async def get_filtered_data(self, filters: List[Dict], limit: int, offset: int) -> Tuple[List, int]:
        return await self.filter_service.get_filtered_data(filters, limit, offset)