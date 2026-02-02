"""Сценарий получения отфильтрованных данных: делегирует агрегату flat_data."""
from typing import Any, Dict, List, Tuple

from app.domain.flat_data.service import FlatDataService


class DataRetrievalService:
    """Получение значений фильтров и отфильтрованных данных. Делегирует FlatDataService."""

    def __init__(self, flat_data_service: FlatDataService):
        self._flat_data_service = flat_data_service

    async def get_filter_values(
        self,
        filter_name: str,
        applied_filters: List[Dict[str, Any]],
        pattern: str = "",
        form_id: str | None = None,
    ) -> List:
        return await self._flat_data_service.get_filter_values(
            filter_name,
            applied_filters,
            pattern,
            form_id,
        )

    async def get_filtered_data(
        self,
        filters: List[Dict[str, Any]],
        limit: int,
        offset: int,
        form_id: str | None = None,
    ) -> Tuple[List, int]:
        return await self._flat_data_service.get_filtered_data(
            filters,
            limit,
            offset,
            form_id,
        )

